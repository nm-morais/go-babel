package pkg

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"

	internalMsg "github.com/nm-morais/go-babel/internal/message"
	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	log "github.com/sirupsen/logrus"
)

type streamManager struct {
	dialingTransports      *sync.Map
	dialingTransportsMutex *sync.Mutex

	inboundTransports  *sync.Map
	outboundTransports *sync.Map

	hbTimers *sync.Map
	logger   *log.Logger
}

var appMsgDeserializer = internalMsg.AppMessageWrapperSerializer{}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

type inboundStreamValueType = io.ReadCloser
type outboundStreamValueType = io.WriteCloser

type StreamManager interface {
	AcceptConnectionsAndNotify(listener stream.Stream)
	DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream stream.Stream)
	SendMessageSideStream(toSend message.Message, toDial peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, stream stream.Stream) errors.Error
	Disconnect(peer peer.Peer)
	SendMessage(message []byte, peer peer.Peer) errors.Error
	Logger() *log.Logger
}

const streamManagerCaller = "StreamManager"

func NewStreamManager() StreamManager {
	sm := streamManager{
		hbTimers:               &sync.Map{},
		dialingTransports:      &sync.Map{},
		dialingTransportsMutex: &sync.Mutex{},
		inboundTransports:      &sync.Map{},
		outboundTransports:     &sync.Map{},
		logger:                 logs.NewLogger(streamManagerCaller),
	}
	return sm
}

func (sm streamManager) Logger() *log.Logger {
	return sm.logger
}

func (sm streamManager) SendMessageSideStream(toSend message.Message, toDial peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, s stream.Stream) errors.Error {
	switch s.(type) {
	case *stream.TCPStream:
		if err := s.Dial(&net.TCPAddr{IP: toDial.IP(), Port: int(toDial.ProtosPort())}); err != nil {
			return err
		}
		defer s.Close()
		mw := messageIO.NewMessageWriter(s)
		err := sm.sendHandshakeMessage(mw, destProtos, TemporaryTunnel)
		if err != nil {
			return err
		}
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
		sm.logger.Info("Sending message sideChannel TCP")
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		_, wErr := mw.Write(wrappedBytes)
		if wErr != nil {
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		return nil

	case *stream.UDPStream:
		if err := s.Dial(&net.UDPAddr{IP: toDial.IP(), Port: int(toDial.ProtosPort())}); err != nil {
			return err
		}
		defer s.Close()

		msgSizeBytes := make([]byte, 4)
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
		sm.logger.Info("Sending message sideChannel UDP")
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		peerBytes := p.config.Peer.Marshal()
		binary.BigEndian.PutUint32(msgSizeBytes, uint32(len(wrappedBytes)))
		totalMsgBytes := append(msgSizeBytes, wrappedBytes...)
		_, wErr := s.Write(append(totalMsgBytes, peerBytes...))
		if wErr != nil {
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		return nil
	default:
		log.Panicf("Unknown stream type %s", reflect.TypeOf(s))
	}
	return nil
}

func (sm streamManager) AcceptConnectionsAndNotify(s stream.Stream) {
	deserializer := internalMsg.AppMessageWrapperSerializer{}
	sm.logger.Infof("Starting listener of type %s", reflect.TypeOf(s))
	listener, err := s.Listen()
	if err != nil {
		panic(err.Reason())
	}
	sm.logger.Infof("Done starting listener %s", reflect.TypeOf(s))
	switch s.(type) {
	case *stream.TCPStream:
		for {
			newStream, err := listener.Accept()
			go func() {
				if err != nil {
					err.Log(sm.logger)
					newStream.Close()
					return
				}
				mr := messageIO.NewMessageReader(newStream)
				mw := messageIO.NewMessageWriter(newStream)

				handshakeMsg, err := sm.waitForHandshakeMessage(mr)
				if err != nil {
					err.Log(sm.logger)
					mr.Close()
					mw.Close()
					newStream = nil
					sm.logStreamManagerState()
					return
				}
				remotePeer := handshakeMsg.Peer
				//sm.logger.Infof("Got handshake message %+v from peer %s", handshakeMsg, remotePeer.ToString())
				if handshakeMsg.TunnelType == TemporaryTunnel {
					go sm.handleTmpStream(remotePeer, mr)
					return
				}

				err = sm.sendHandshakeMessage(mw, RegisteredProtos(), PermanentTunnel)
				if err != nil {
					sm.logger.Errorf("An error occurred during handshake with %s: %s", remotePeer.String(), err.Reason())
					sm.inboundTransports.Delete(remotePeer.String())
					mr.Close()
					mw.Close()
					newStream = nil
					sm.logStreamManagerState()
					return
				}

				sm.logger.Warnf("New connection from %s", remotePeer.String())

				sm.inboundTransports.Store(remotePeer.String(), newStream)
				if !inConnRequested(handshakeMsg.Protos, remotePeer) {
					mr.Close()
					mw.Close()
					newStream = nil
					sm.inboundTransports.Delete(remotePeer.String())
					sm.logger.Infof("Peer %s conn was not accepted, closing stream", remotePeer.String())
					sm.logStreamManagerState()
					return
				}
				sm.logger.Warnf("Accepted connection from %s successfully", remotePeer.String())
				sm.logStreamManagerState()
				go sm.handleInStream(mr, remotePeer)
			}()
		}
	case *stream.UDPStream:
		for {
			msgBuf := make([]byte, 2048)
			n, rErr := s.Read(msgBuf)
			if rErr != nil {
				log.Warnf("An error ocurred reading message using UDP:%s ", rErr.Error())
				continue
			}

			msgSize := int(binary.BigEndian.Uint32(msgBuf[:4]))
			deserialized := deserializer.Deserialize(msgBuf[4 : 4+msgSize])
			sender := &peer.IPeer{}
			sender.Unmarshal(msgBuf[4+msgSize : n])
			protoMsg := deserialized.(*internalMsg.AppMessageWrapper)
			for _, toNotifyID := range protoMsg.DestProtos {
				if toNotify, ok := p.protocols.Load(toNotifyID); ok {
					appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
					toNotify.(protocolValueType).DeliverMessage(sender, appMsg)
				} else {
					sm.logger.Panicf("Ignored message: %+v", protoMsg)
				}
			}
		}
	}
}

func (sm streamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream stream.Stream) {

	if toDial.String() == SelfPeer().String() {
		panic("Dialing self")
	}

	sm.logger.Warnf("Dialing: %s", toDial.String())

	doneDialing, ok := sm.dialingTransports.Load(toDial.String())
	if ok {
		waitChan := doneDialing.(chan interface{})
		sm.waitDial(dialingProto, toDial, waitChan)
		return
	}

	sm.dialingTransportsMutex.Lock()

	_, connUp := sm.outboundTransports.Load(toDial.String())
	if connUp {
		p.channelSubscribersMutex.Lock()
		subs := p.channelSubscribers[toDial.String()]
		sm.logger.Warnf("stream to %s was already active?", toDial.String())
		subs[dialingProto] = true
		p.channelSubscribers[toDial.String()] = subs
		p.channelSubscribersMutex.Unlock()
		callerProto, _ := p.protocols.Load(dialingProto)
		callerProto.(protocolValueType).DialSuccess(dialingProto, toDial)
		sm.dialingTransportsMutex.Unlock()
		return
	}

	doneDialing, ok = sm.dialingTransports.Load(toDial.String())
	if ok {
		waitChan := doneDialing.(chan interface{})
		sm.dialingTransportsMutex.Unlock()
		sm.waitDial(dialingProto, toDial, waitChan)
		return
	}

	done := make(chan interface{})
	sm.dialingTransports.Store(toDial.String(), done)
	defer func() {
		sm.dialingTransports.Delete(toDial.String())
		close(done)
	}()

	sm.dialingTransportsMutex.Unlock()
	remotePeer := &net.TCPAddr{IP: toDial.IP(), Port: int(toDial.ProtosPort())}
	sm.logger.Infof("Dialing %+v", remotePeer)
	err := stream.DialWithTimeout(remotePeer, p.config.DialTimeout)
	if err != nil {
		err.Log(sm.logger)
		dialError(dialingProto, toDial)
		return
	}

	// sm.logger.Infof("Done dialing node %s", toDial.Addr())
	// sm.logger.Infof("Exchanging protos")
	//sm.logger.Info("Remote protos: %d", handshakeMsg.Protos)
	//sm.logger.Infof("Starting handshake...")

	mw := messageIO.NewMessageWriter(stream)
	mr := messageIO.NewMessageReader(stream)

	err = sm.sendHandshakeMessage(mw, RegisteredProtos(), PermanentTunnel)
	if err != nil {
		sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), err.Reason())
		mr.Close()
		dialError(dialingProto, toDial)
		stream.Close()
		stream = nil
		return
	}

	handshakeMsg, err := sm.waitForHandshakeMessage(mr)
	if err != nil {
		mr.Close()
		mw.Close()
		dialError(dialingProto, toDial)
		sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), err.Reason())
		stream = nil
		return
	}

	newOutboundConn := messageIO.NewMessageWriter(stream)
	sm.outboundTransports.Store(toDial.String(), newOutboundConn)
	if !dialSuccess(dialingProto, handshakeMsg.Protos, toDial) {
		sm.logger.Error("No protocol accepted conn")
		mr.Close()
		mw.Close()
		sm.outboundTransports.Delete(toDial.String())
		sm.logStreamManagerState()
		stream = nil
		return
	}
	sm.logger.Warnf("Dialed %s successfully", toDial.String())
	sm.logStreamManagerState()
}

func (sm streamManager) SendMessage(message []byte, p peer.Peer) errors.Error {
	if p == nil {
		sm.logger.Panic("Peer is nil")
	}
	outboundStream, ok := sm.outboundTransports.Load(p.String())
	if !ok {
		sm.dialingTransportsMutex.Lock()
		doneDialing, ok := sm.dialingTransports.Load(p.String())
		if !ok {
			sm.dialingTransportsMutex.Unlock()
			outboundStream, ok = sm.outboundTransports.Load(p.String())
			if !ok {
				return errors.NonFatalError(404, "stream not found", streamManagerCaller)
			}
		} else {
			sm.dialingTransportsMutex.Unlock()
			waitChan := doneDialing.(chan interface{})
			<-waitChan
			outboundStream, ok = sm.outboundTransports.Load(p.String())
			if !ok {
				return errors.NonFatalError(404, "stream not found", streamManagerCaller)
			}
		}
	}

	_, err := outboundStream.(outboundStreamValueType).Write(message)
	if err != nil {
		sm.logger.Error(err)
		sm.handleOutboundTransportFailure(p)
	}
	return nil
}

func (sm streamManager) Disconnect(p peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Disconnecting from %s", p.String())
	sm.dialingTransportsMutex.Lock()
	if conn, ok := sm.outboundTransports.Load(p.String()); ok {
		conn.(outboundStreamValueType).Close()
		sm.outboundTransports.Delete(p.String())
	}
	sm.dialingTransportsMutex.Unlock()
	sm.logStreamManagerState()
}

func (sm streamManager) handleInStream(mr inboundStreamValueType, newPeer peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Handling peer stream %s", newPeer.String())
	defer sm.logger.Warnf("[ConnectionEvent] : Done handling peer stream %s", newPeer.String())

	deserializer := internalMsg.AppMessageWrapperSerializer{}
	for {
		msgBuf := make([]byte, 2048)
		n, err := mr.Read(msgBuf)
		if err != nil {
			if err == io.EOF {
				sm.logger.Warnf("Read routine from %s got %s, exiting cleanly...", newPeer.String(), err)
			} else {
				sm.logger.Error(err)
			}
			mr.Close()
			mr = nil
			sm.inboundTransports.Delete(newPeer.String())
			return
		}

		//sm.logger.Infof("Read %d bytes from %s", n, newPeer.ToString())
		deserialized := deserializer.Deserialize(msgBuf[:n])
		protoMsg := deserialized.(*internalMsg.AppMessageWrapper)
		for _, toNotifyID := range protoMsg.DestProtos {
			if toNotify, ok := p.protocols.Load(toNotifyID); ok {
				appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
				//sm.logger.Infof("Proto %d Got message: %s from %s", toNotifyID, reflect.TypeOf(appMsg), newPeer.ToString())
				toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
			} else {
				sm.logger.Panicf("Ignored message: %+v", protoMsg)
			}
		}
	}
}

func (sm streamManager) waitDial(dialerProto protocol.ID, toDial peer.Peer, waitChan chan interface{}) {
	<-waitChan
	proto, _ := p.protocols.Load(dialerProto)
	_, connUp := sm.outboundTransports.Load(toDial.String())
	if !connUp {
		proto.(protocolValueType).DialFailed(toDial)
	} else {
		p.channelSubscribersMutex.Lock()
		subs := p.channelSubscribers[toDial.String()]
		if proto.(protocolValueType).DialSuccess(dialerProto, toDial) {
			subs[dialerProto] = true
			p.channelSubscribers[toDial.String()] = subs
		}
		p.channelSubscribersMutex.Unlock()
	}
}

func (sm streamManager) handleOutboundTransportFailure(remotePeer peer.Peer) errors.Error {
	sm.dialingTransportsMutex.Lock()
	outConn, ok := sm.outboundTransports.Load(remotePeer.String())
	if ok {
		outConn.(outboundStreamValueType).Close()
		sm.outboundTransports.Delete(remotePeer.String())
	}
	sm.dialingTransportsMutex.Unlock()
	outTransportFailure(remotePeer)
	return nil
}

func (sm streamManager) handleTmpStream(newPeer peer.Peer, mr messageIO.MessageReader) {

	if newPeer.String() == SelfPeer().String() {
		panic("Dialing self")
	}

	//sm.logger.Info("Reading from tmp stream")
	msg, err := sm.readAppMessage(mr)
	if err != nil {
		err.Log(sm.logger)
		return
	}
	//sm.logger.Info("Done reading from tmp stream")

	appMsg := p.serializationManager.Deserialize(msg.MessageID, msg.WrappedMsgBytes)
	for _, toNotifyID := range msg.DestProtos {
		if toNotify, ok := p.protocols.Load(toNotifyID); ok {
			//sm.logger.Infof("Proto %d Got message: %s from %s", toNotifyID, reflect.TypeOf(appMsg), newPeer.ToString())
			toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
		} else {
			sm.logger.Errorf("Ignored message: %+v", appMsg)
		}
	}
	mr.Close()
}

func (sm streamManager) waitForHandshakeMessage(transport io.Reader) (*internalMsg.ProtoHandshakeMessage, errors.Error) {
	msgBytes := make([]byte, 2048)
	read, err := transport.Read(msgBytes)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes[:read]).(internalMsg.ProtoHandshakeMessage)
	//sm.logger.Infof("Received proto exchange message %+v", msg)
	return &msg, nil
}

func (sm streamManager) readAppMessage(stream io.Reader) (*internalMsg.AppMessageWrapper, errors.Error) {
	msgBuf := make([]byte, 2048)
	n, err := stream.Read(msgBuf)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	deserialized := appMsgDeserializer.Deserialize(msgBuf[:n]).(*internalMsg.AppMessageWrapper)
	return deserialized, nil
}

func (sm streamManager) sendHandshakeMessage(transport io.Writer, destProtos []protocol.ID, chanType uint8) errors.Error {
	var toSend = internalMsg.NewProtoHandshakeMessage(destProtos, SelfPeer(), chanType)
	msgBytes := protoMsgSerializer.Serialize(toSend)
	_, err := transport.Write(msgBytes)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	return nil
}

func (sm streamManager) logStreamManagerState() {
	inboundNr := 0
	outboundNr := 0
	toLog := "inbound connections : "
	sm.logger.Info("------------- Protocol Manager state -------------")
	sm.inboundTransports.Range(func(peer, conn interface{}) bool {
		toLog += fmt.Sprintf("%s, ", peer.(string))
		inboundNr++
		return true
	})
	sm.logger.Info(toLog)
	toLog = ""
	toLog = "outbound connections : "
	sm.outboundTransports.Range(func(peer, conn interface{}) bool {
		toLog += fmt.Sprintf("%s, ", peer.(string))
		outboundNr++
		return true
	})
	sm.logger.Info(toLog)
	if outboundNr != inboundNr {
		sm.logger.Warn("Inbound connections and outboundConnections are mismatched")
	}
	sm.logger.Info("--------------------------------------------------")
}
