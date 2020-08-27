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

	hbChannels *sync.Map
	hbTimers   *sync.Map

	logger *log.Logger
}

var appMsgDeserializer = internalMsg.AppMessageWrapperSerializer{}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

type inboundStreamValueType = stream.Stream
type outboundStreamValueType = struct {
	mw                *messageIO.MessageWriter
	outboundTransport stream.Stream
}

type hbTimerRoutineChannels = struct {
	finish chan interface{}
	renew  chan struct{}
}

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
		hbChannels:             &sync.Map{},
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
		peerBytes := p.config.Peer.SerializeToBinary()
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
			if err != nil {
				err.Log(sm.logger)
				newStream.Close()
				continue
			}
			mr := messageIO.NewMessageReader(newStream)
			handshakeMsg, err := sm.waitForHandshakeMessage(mr)
			if err != nil {
				err.Log(sm.logger)
				newStream.Close()
				sm.logStreamManagerState()
				continue
			}
			remotePeer := handshakeMsg.Peer
			//sm.logger.Infof("Got handshake message %+v from peer %s", handshakeMsg, remotePeer.ToString())
			if handshakeMsg.TunnelType == TemporaryTunnel {
				go sm.handleTmpStream(remotePeer, mr)
				continue
			}
			sm.logger.Warnf("New connection from %s", remotePeer.ToString())
			sm.inboundTransports.Store(remotePeer.ToString(), newStream)

			if !inConnRequested(handshakeMsg.Protos, remotePeer) {
				newStream.Close()
				sm.inboundTransports.Delete(remotePeer.ToString())
				sm.logger.Infof("Peer %s conn was not accepted, closing stream", remotePeer.ToString())
				sm.logStreamManagerState()
				continue
			}

			err = sm.sendHandshakeMessage(messageIO.NewMessageWriter(newStream), RegisteredProtos(), PermanentTunnel)
			if err != nil {
				sm.logger.Errorf("An error occurred during handshake with %s: %s", remotePeer.ToString(), err.Reason())
				sm.inboundTransports.Delete(remotePeer.ToString())
				newStream.Close()
				sm.logStreamManagerState()
				continue
			}

			go sm.handleInStream(mr, newStream, remotePeer)
			sm.logger.Warnf("Accepted connection from %s successfully", remotePeer.ToString())
			sm.logStreamManagerState()
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
			_, sender := peer.DeserializePeer(msgBuf[4+msgSize : n])

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

	if toDial.ToString() == SelfPeer().ToString() {
		panic("Dialing self")
	}

	sm.logger.Warnf("Dialing: %s", toDial.ToString())

	doneDialing, ok := sm.dialingTransports.Load(toDial.ToString())
	if ok {
		waitChan := doneDialing.(chan interface{})
		sm.waitDial(dialingProto, toDial, waitChan)
		return
	}

	sm.dialingTransportsMutex.Lock()

	_, connUp := sm.outboundTransports.Load(toDial.ToString())
	if connUp {
		p.channelSubscribersMutex.Lock()
		subs := p.channelSubscribers[toDial.ToString()]
		sm.logger.Warnf("stream to %s was already active?", toDial.ToString())
		subs[dialingProto] = true
		p.channelSubscribers[toDial.ToString()] = subs
		p.channelSubscribersMutex.Unlock()
		callerProto, _ := p.protocols.Load(dialingProto)
		callerProto.(protocolValueType).DialSuccess(dialingProto, toDial)
		sm.dialingTransportsMutex.Unlock()
		return
	}

	doneDialing, ok = sm.dialingTransports.Load(toDial.ToString())
	if ok {
		waitChan := doneDialing.(chan interface{})
		sm.dialingTransportsMutex.Unlock()
		sm.waitDial(dialingProto, toDial, waitChan)
		return
	}
	done := make(chan interface{})
	sm.dialingTransports.Store(toDial.ToString(), done)
	defer func() {
		sm.dialingTransports.Delete(toDial.ToString())
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

	err = sm.sendHandshakeMessage(messageIO.NewMessageWriter(stream), RegisteredProtos(), PermanentTunnel)
	if err != nil {
		sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.ToString(), err.Reason())
		dialError(dialingProto, toDial)
		stream.Close()
		return
	}

	mr := messageIO.NewMessageReader(stream)
	handshakeMsg, err := sm.waitForHandshakeMessage(mr)
	if err != nil {
		stream.Close()
		sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.ToString(), err.Reason())
		return
	}

	newOutboundConn := outboundStreamValueType{
		mw:                messageIO.NewMessageWriter(stream),
		outboundTransport: stream,
	}

	hbChannel := hbTimerRoutineChannels{
		finish: make(chan interface{}),
		renew:  make(chan struct{}),
	}

	sm.hbChannels.Store(toDial.ToString(), hbChannel)
	sm.outboundTransports.Store(toDial.ToString(), newOutboundConn)

	if !dialSuccess(dialingProto, handshakeMsg.Protos, toDial) {
		sm.logger.Panicln("No protocol accepted conn")
		stream.Close()
		sm.hbChannels.Delete(toDial.ToString())
		sm.outboundTransports.Delete(toDial.ToString())
		sm.logStreamManagerState()
		return
	}
	sm.logger.Warnf("Dialed %s successfully", toDial.ToString())
	sm.logStreamManagerState()
}

func (sm streamManager) SendMessage(message []byte, peer peer.Peer) errors.Error {
	if peer == nil {
		sm.logger.Panic("Peer is nil")
	}

	outboundStream, ok := sm.outboundTransports.Load(peer.ToString())
	if !ok {
		doneDialing, ok := sm.dialingTransports.Load(peer.ToString())
		if !ok {
			return errors.NonFatalError(404, "stream not found", streamManagerCaller)
		}
		waitChan := doneDialing.(chan interface{})
		<-waitChan
		_, ok = sm.outboundTransports.Load(peer.ToString())
		if !ok {
			return errors.NonFatalError(404, "stream not found", streamManagerCaller)
		}
	}

	_, err := outboundStream.(outboundStreamValueType).mw.Write(message)
	if err != nil {
		sm.logger.Error(err)
		sm.handleOutboundTransportFailure(peer)
	}
	return nil
}

func (sm streamManager) Disconnect(peer peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Disconnecting from %s", peer.ToString())
	sm.dialingTransportsMutex.Lock()
	if conn, ok := sm.outboundTransports.Load(peer.ToString()); ok {
		conn.(outboundStreamValueType).outboundTransport.Close()
		sm.outboundTransports.Delete(peer.ToString())
	}
	if conn, ok := sm.inboundTransports.Load(peer.ToString()); ok {
		conn.(inboundStreamValueType).Close()
		sm.inboundTransports.Delete(peer.ToString())
	}
	sm.dialingTransportsMutex.Unlock()
	sm.logStreamManagerState()
}

func (sm streamManager) handleInStream(mr *messageIO.MessageReader, t stream.Stream, newPeer peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Handling peer stream %s", newPeer.ToString())
	defer sm.logger.Warnf("[ConnectionEvent] : Done handling peer stream %s", newPeer.ToString())
	deserializer := internalMsg.AppMessageWrapperSerializer{}
	for {
		msgBuf := make([]byte, 2048)
		n, err := mr.Read(msgBuf)
		if err != nil {
			if err == io.EOF {
				sm.logger.Warnf("Read routine from %s got %s, exiting cleanly...", newPeer.ToString(), err)
			} else {
				sm.logger.Error(err)
			}
			t.Close()
			sm.inboundTransports.Delete(newPeer.ToString())
			return
		}

		//sm.logger.Infof("Read %d bytes from %s", n, newPeer.ToString())
		deserialized := deserializer.Deserialize(msgBuf[:n])
		protoMsg := deserialized.(*internalMsg.AppMessageWrapper)
		if protoMsg.MessageID == internalMsg.HeartbeatMessageType {
			continue
		}

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
	_, connUp := sm.inboundTransports.Load(toDial.ToString())
	if !connUp {
		proto.(protocolValueType).DialFailed(toDial)
	} else {
		p.channelSubscribersMutex.Lock()
		subs := p.channelSubscribers[toDial.ToString()]
		if proto.(protocolValueType).DialSuccess(dialerProto, toDial) {
			subs[dialerProto] = true
			p.channelSubscribers[toDial.ToString()] = subs
		}
		p.channelSubscribersMutex.Unlock()
	}
}

func (sm streamManager) handleOutboundTransportFailure(remotePeer peer.Peer) errors.Error {
	sm.dialingTransportsMutex.Lock() //TODO think about this, should we close inbound conn on outbound close
	outConn, ok := sm.outboundTransports.Load(remotePeer.ToString())
	if ok {
		outConn.(outboundStreamValueType).outboundTransport.Close()
		sm.outboundTransports.Delete(remotePeer.ToString())
	}
	_, ok = sm.inboundTransports.Load(remotePeer.ToString())
	if ok {
		outConn.(inboundStreamValueType).Close()
		sm.inboundTransports.Delete(remotePeer.ToString())
	}
	sm.dialingTransportsMutex.Unlock()
	outTransportFailure(remotePeer)
	return nil
}

func (sm streamManager) logConnections() {
	sm.logger.Info("------------- Active Connections -------------")
	var toLog string
	toLog = "Inbound connections : "
	sm.inboundTransports.Range(func(peer, conn interface{}) bool {
		toLog += fmt.Sprintf("%s, ", peer.(string))
		return true
	})
	sm.logger.Info(toLog)
	toLog = ""
	toLog = "outbound connections : "
	sm.inboundTransports.Range(func(peer, conn interface{}) bool {
		toLog += fmt.Sprintf("%s, ", peer.(string))
		return true
	})
	sm.logger.Info(toLog)
	sm.logger.Info("----------------------------------------------")
}

func (sm streamManager) handleTmpStream(newPeer peer.Peer, transport io.Reader) {

	if newPeer.ToString() == SelfPeer().ToString() {
		panic("Dialing self")
	}

	//sm.logger.Info("Reading from tmp stream")
	msg, err := sm.readAppMessage(transport)
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
