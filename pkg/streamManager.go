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
	log "github.com/sirupsen/logrus"
)

var (
	encoderConfig = messageIO.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}

	decoderConfig = messageIO.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
)

type streamManager struct {
	udpConn *net.UDPConn

	outboundTransports     *sync.Map
	dialingTransportsMutex *sync.RWMutex

	inboundTransports *sync.Map

	logger *log.Logger
}

type outboundTransport struct {
	Addr net.Addr

	Dialed   chan interface{}
	DialErr  chan interface{}
	Finished chan interface{}

	MsgChan chan []byte
}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

type StreamManager interface {
	AcceptConnectionsAndNotify(listenAddr net.Addr) (done chan interface{})
	DialAndNotify(dialingProto protocol.ID, peer peer.Peer, toDial net.Addr)
	SendMessageSideStream(toSend message.Message, peer peer.Peer, addr net.Addr, sourceProtoID protocol.ID, destProtos []protocol.ID) errors.Error
	Disconnect(peer peer.Peer)
	SendMessage(message []byte, peer peer.Peer) errors.Error
	Logger() *log.Logger
}

const streamManagerCaller = "StreamManager"

func NewStreamManager() StreamManager {
	sm := &streamManager{
		udpConn:                &net.UDPConn{},
		dialingTransportsMutex: &sync.RWMutex{},
		inboundTransports:      &sync.Map{},
		outboundTransports:     &sync.Map{},
		logger:                 logs.NewLogger(streamManagerCaller),
	}
	return sm
}

func (sm *streamManager) Logger() *log.Logger {
	return sm.logger
}

func (sm *streamManager) SendMessageSideStream(toSend message.Message, peer peer.Peer, rAddrInt net.Addr, sourceProtoID protocol.ID, destProtos []protocol.ID) errors.Error {
	switch rAddr := rAddrInt.(type) {
	case *net.UDPAddr:
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		peerBytes := p.config.Peer.Marshal()
		_, wErr := sm.udpConn.WriteToUDP(append(peerBytes, wrappedBytes...), rAddr)
		if wErr != nil {
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
	case *net.TCPAddr:
		tcpStream, err := net.Dial(rAddr.Network(), rAddr.String())
		if err != nil {
			return errors.NonFatalError(500, err.Error(), streamManagerCaller)
		}
		frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, tcpStream)
		hErr := sm.sendHandshakeMessage(frameBasedConn, sourceProtoID, destProtos, TemporaryTunnel)
		if hErr != nil {
			return hErr
		}
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		wErr := frameBasedConn.WriteFrame(wrappedBytes)
		if wErr != nil {
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		tcpStream.Close()
	default:
		log.Panicf("Unknown addr type %s", reflect.TypeOf(rAddr))
	}
	return nil
}

func (sm *streamManager) AcceptConnectionsAndNotify(lAddrInt net.Addr) chan interface{} {
	done := make(chan interface{})
	go func() {
		sm.logger.Infof("Starting listener of type %s", lAddrInt.Network())
		switch lAddr := lAddrInt.(type) {
		case *net.TCPAddr:
			listener, err := net.ListenTCP(lAddr.Network(), lAddr)
			if err != nil {
				panic(err)
			}
			close(done)
			for {
				newStream, err := listener.Accept()
				if err != nil {
					panic(err)
				}
				go func() {
					frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, newStream)
					handshakeMsg, err := sm.waitForHandshakeMessage(frameBasedConn)
					if err != nil {
						err.Log(sm.logger)
						frameBasedConn.Close()
						sm.logStreamManagerState()
						return
					}
					remotePeer := handshakeMsg.Peer
					//sm.logger.Infof("Got handshake message %+v from peer %s", handshakeMsg, remotePeer.ToString())
					if handshakeMsg.TunnelType == TemporaryTunnel {
						go sm.handleTmpStream(remotePeer, frameBasedConn)
						return
					}

					sm.logger.Warnf("New connection from %s", remotePeer.String())
					acceptedProtos := inConnRequested(handshakeMsg.DialerProto, handshakeMsg.Protos, remotePeer)
					if len(acceptedProtos) == 0 {
						frameBasedConn.Close()
						sm.inboundTransports.Delete(remotePeer.String())
						sm.logger.Infof("Peer %s conn was not accepted, closing stream", remotePeer.String())
						sm.logStreamManagerState()
						return
					}

					err = sm.sendHandshakeMessage(frameBasedConn, 0, acceptedProtos, PermanentTunnel)
					if err != nil {
						sm.logger.Errorf("An error occurred during handshake with %s: %s", remotePeer.String(), err.Reason())
						sm.inboundTransports.Delete(remotePeer.String())
						frameBasedConn.Close()
						sm.logStreamManagerState()
						return
					}
					sm.logger.Warnf("Accepted connection from %s successfully", remotePeer.String())
					sm.logStreamManagerState()
					sm.inboundTransports.Store(remotePeer.String(), newStream)
					go sm.handleInStream(frameBasedConn, remotePeer)
				}()
			}
		case *net.UDPAddr:
			deserializer := internalMsg.AppMessageWrapperSerializer{}
			packetConn, err := net.ListenUDP(lAddr.Network(), lAddr)
			if err != nil {
				panic(err)
			}
			sm.udpConn = packetConn
			close(done)
			for {
				msgBytes := make([]byte, 2048)
				n, _, err := packetConn.ReadFrom(msgBytes)
				if err != nil {
					panic(err)
				}
				sender := &peer.IPeer{}
				msgBuf := msgBytes[:n]
				peerSize := sender.Unmarshal(msgBuf)
				sm.logger.Info(sender.String())
				deserialized := deserializer.Deserialize(msgBuf[peerSize:])
				protoMsg := deserialized.(*internalMsg.AppMessageWrapper)
				// sm.logger.Infof("Got message via UDP: %+v", protoMsg)
				for _, toNotifyID := range protoMsg.DestProtos {
					if toNotify, ok := p.protocols.Load(toNotifyID); ok {
						appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
						toNotify.(protocolValueType).DeliverMessage(sender, appMsg)
					} else {
						sm.logger.Panicf("Ignored message: %+v", protoMsg)
					}
				}
			}
		default:
			panic("cannot listen in such addr")
		}
	}()
	return done
}

func (sm *streamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, addr net.Addr) {
	sm.logger.Warnf("Dialing: %s", toDial.String())

	// check-lock-check

	outboundTransportInt, connUp := sm.outboundTransports.Load(toDial.String())
	if connUp {
		outboundTransport := outboundTransportInt.(outboundTransport)
		select {
		case <-outboundTransport.DialErr:
			dialError(dialingProto, toDial)
		case <-outboundTransport.Dialed:
			p.channelSubscribersMutex.Lock()
			subs := p.channelSubscribers[toDial.String()]
			subs[dialingProto] = true
			p.channelSubscribers[toDial.String()] = subs
			p.channelSubscribersMutex.Unlock()
			callerProto, _ := p.protocols.Load(dialingProto)
			callerProto.(protocolValueType).DialSuccess(dialingProto, toDial)
		}
		return
	}
	sm.dialingTransportsMutex.Lock()
	outboundTransportInt, connUp = sm.outboundTransports.Load(toDial.String())
	if connUp {
		outboundTransport := outboundTransportInt.(outboundTransport)
		select {
		case <-outboundTransport.DialErr:
			dialError(dialingProto, toDial)
		case <-outboundTransport.Dialed:
			p.channelSubscribersMutex.Lock()
			subs := p.channelSubscribers[toDial.String()]
			subs[dialingProto] = true
			p.channelSubscribers[toDial.String()] = subs
			p.channelSubscribersMutex.Unlock()
			callerProto, _ := p.protocols.Load(dialingProto)
			callerProto.(protocolValueType).DialSuccess(dialingProto, toDial)
		}
		return
	}
	newOutboundTransport := outboundTransport{
		Addr:     addr,
		DialErr:  make(chan interface{}),
		Dialed:   make(chan interface{}),
		Finished: make(chan interface{}),
		MsgChan:  make(chan []byte),
	}
	sm.outboundTransports.Store(toDial.String(), newOutboundTransport)
	sm.dialingTransportsMutex.Unlock()

	conn, err := net.DialTimeout(addr.Network(), addr.String(), p.config.DialTimeout)
	if err != nil {
		sm.logger.Error(err)
		close(newOutboundTransport.DialErr)
		dialError(dialingProto, toDial)
		sm.outboundTransports.Delete(toDial.String())
		return
	}

	// sm.logger.Infof("Done dialing node %s", toDial.Addr())
	// sm.logger.Infof("Exchanging protos")
	//sm.logger.Info("Remote protos: %d", handshakeMsg.Protos)
	//sm.logger.Infof("Starting handshake...")

	switch newStreamTyped := conn.(type) {
	case net.Conn:
		frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, newStreamTyped)
		herr := sm.sendHandshakeMessage(frameBasedConn, dialingProto, RegisteredProtos(), PermanentTunnel)
		if herr != nil {
			sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), err.Error())
			frameBasedConn.Close()
			close(newOutboundTransport.DialErr)
			dialError(dialingProto, toDial)
			sm.outboundTransports.Delete(toDial.String())
			return
		}

		handshakeMsg, err := sm.waitForHandshakeMessage(frameBasedConn)
		if err != nil {
			sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), err.Reason())
			frameBasedConn.Close()
			close(newOutboundTransport.DialErr)
			dialError(dialingProto, toDial)
			sm.outboundTransports.Delete(toDial.String())
			return
		}

		if !dialSuccess(dialingProto, handshakeMsg.Protos, toDial) {
			sm.logger.Error("No protocol accepted conn")
			frameBasedConn.Close()
			close(newOutboundTransport.DialErr)
			dialError(dialingProto, toDial)
			sm.outboundTransports.Delete(toDial.String())
			return
		}

		sm.logger.Warnf("Dialed %s successfully", toDial.String())
		sm.logStreamManagerState()
		sm.handleOutTransportFrameConn(newOutboundTransport, frameBasedConn, toDial)
	default:
		sm.logger.Panic("Unsupported conn type")
	}
}

func (sm *streamManager) handleOutTransportFrameConn(t outboundTransport, conn messageIO.FrameConn, peer peer.Peer) {
	close(t.Dialed)
	for msg := range t.MsgChan {
		err := conn.WriteFrame(msg)
		if err != nil {
			conn.Close()
			close(t.Finished)
			sm.handleOutboundTransportFailure(t, peer)
		}
	}
}

func (sm *streamManager) handleOutboundTransportFailure(t outboundTransport, remotePeer peer.Peer) errors.Error {
	sm.dialingTransportsMutex.Lock()
	sm.outboundTransports.Delete(remotePeer.String())
	sm.dialingTransportsMutex.Unlock()
	outTransportFailure(remotePeer)
	return nil
}

func (sm *streamManager) SendMessage(message []byte, targetPeer peer.Peer) errors.Error {
	sm.dialingTransportsMutex.RLock()
	defer sm.dialingTransportsMutex.RUnlock()
	outboundStreamInt, ok := sm.outboundTransports.Load(targetPeer.String())
	if !ok {
		return errors.NonFatalError(404, "stream not found", streamManagerCaller)
	}
	outboundStream := outboundStreamInt.(outboundTransport)

	select {
	case <-outboundStream.DialErr:
		return errors.NonFatalError(500, "dial failed", streamManagerCaller)
	case <-outboundStream.Finished:
		return errors.NonFatalError(500, "connection error", streamManagerCaller)
	case <-outboundStream.Dialed:
		select {
		case <-outboundStream.Finished:
			return errors.NonFatalError(500, "connection error", streamManagerCaller)
		case outboundStream.MsgChan <- message:
		}
	}
	return nil
}

func (sm *streamManager) Disconnect(p peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Disconnecting from %s", p.String())
	sm.dialingTransportsMutex.Lock()
	if conn, ok := sm.outboundTransports.Load(p.String()); ok {
		sm.outboundTransports.Delete(p.String())
		close(conn.(outboundTransport).MsgChan)
	}
	sm.dialingTransportsMutex.Unlock()
	sm.logStreamManagerState()
}

func (sm *streamManager) handleInStream(mr messageIO.FrameConn, newPeer peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Handling peer stream %s", newPeer.String())
	defer sm.logger.Warnf("[ConnectionEvent] : Done handling peer stream %s", newPeer.String())

	deserializer := internalMsg.AppMessageWrapperSerializer{}
	for {
		msgBuf, err := mr.ReadFrame()
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

		if len(msgBuf) > 0 {
			//sm.logger.Infof("Read %d bytes from %s", n, newPeer.ToString())
			deserialized := deserializer.Deserialize(msgBuf)
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
}

func (sm *streamManager) handleTmpStream(newPeer peer.Peer, c messageIO.FrameConn) {
	deserializer := internalMsg.AppMessageWrapperSerializer{}

	if newPeer.String() == SelfPeer().String() {
		panic("Dialing self")
	}

	//sm.logger.Info("Reading from tmp stream")
	msgBytes, err := c.ReadFrame()
	if err != nil {
		return
	}

	msgGeneric := deserializer.Deserialize(msgBytes)
	msg := msgGeneric.(*internalMsg.AppMessageWrapper)
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
	c.Close()
}

func (sm *streamManager) waitForHandshakeMessage(frameBasedConn messageIO.FrameConn) (*internalMsg.ProtoHandshakeMessage, errors.Error) {
	msgBytes, err := frameBasedConn.ReadFrame()
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes).(internalMsg.ProtoHandshakeMessage)
	//sm.logger.Infof("Received proto exchange message %+v", msg)
	return &msg, nil
}

func (sm *streamManager) sendHandshakeMessage(transport messageIO.FrameConn, dialerProto protocol.ID, destProtos []protocol.ID, chanType uint8) errors.Error {
	var toSend = internalMsg.NewProtoHandshakeMessage(dialerProto, destProtos, SelfPeer(), chanType)
	msgBytes := protoMsgSerializer.Serialize(toSend)
	err := transport.WriteFrame(msgBytes)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	return nil
}

func (sm *streamManager) logStreamManagerState() {
	inboundNr := 0
	outboundNr := 0
	toLog := "inbound connections : "
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
}
