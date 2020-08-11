package pkg

import (
	"fmt"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/transport"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
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

var appMsgDeserializer = message.AppMessageWrapperSerializer{}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

type hbTimerRoutineChannels = struct {
	finish chan interface{}
	renew  chan struct{}
}

type StreamManager interface {
	AcceptConnectionsAndNotify()
	DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream transport.Stream)
	SendMessageSideStream(toSend message.Message, toDial peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, stream transport.Stream)
	Disconnect(peer peer.Peer)
	SendMessage(message []byte, peer peer.Peer) errors.Error
	Logger() *log.Logger
}

const streamManagerCaller = "StreamManager"

func NewStreamManager() StreamManager {
	return streamManager{
		hbTimers:               &sync.Map{},
		dialingTransports:      &sync.Map{},
		dialingTransportsMutex: &sync.Mutex{},
		hbChannels:             &sync.Map{},
		inboundTransports:      &sync.Map{},
		outboundTransports:     &sync.Map{},
		logger:                 logs.NewLogger(streamManagerCaller),
	}
}

func (sm streamManager) Logger() *log.Logger {
	return sm.logger
}

func (sm streamManager) SendMessageSideStream(toSend message.Message, toDial peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, stream transport.Stream) {
	if err := stream.Dial(toDial); err != nil {
		sm.logger.Errorf("Failed to establish temporary stream due to: %s", err.Reason())
		return
	}
	sm.sendHandshakeMessage(stream, destProtos, TemporaryTunnel)
	msgBytes := toSend.Serializer().Serialize(toSend)
	msgWrapper := message.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
	// sm.logger.Info("Sending message sideChannel")
	wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
	_, _ = stream.Write(wrappedBytes)
	stream.Close()
}

func (sm streamManager) AcceptConnectionsAndNotify() {
	listener, err := p.listener.Listen()
	if err != nil {
		panic(err.Reason())
	}
	for {
		newStream, err := listener.Accept()
		if err != nil {
			err.Log(sm.logger)
			newStream.Close()
			continue
		}

		handshakeMsg, err := sm.waitForHandshakeMessage(newStream)
		if err != nil {
			err.Log(sm.logger)
			newStream.Close()
			continue
		}
		remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)
		//sm.logger.Infof("Got handshake message %+v from peer %s", handshakeMsg, remotePeer.ToString())
		if handshakeMsg.TunnelType == TemporaryTunnel {
			go sm.handleTmpStream(remotePeer, newStream)
			continue
		}
		sm.logger.Warnf("New connection from %s", remotePeer.ToString())

		sm.inboundTransports.Store(remotePeer.ToString(), newStream)

		if !inConnRequested(handshakeMsg.Protos, remotePeer) {
			newStream.Close()
			sm.inboundTransports.Delete(remotePeer.ToString())
			sm.logger.Infof("Peer %s conn was not accepted, closing stream", remotePeer.ToString())
			continue
		}

		err = sm.sendHandshakeMessage(newStream, RegisteredProtos(), PermanentTunnel)
		if err != nil {
			sm.logger.Errorf("An error occurred during handshake with %s: %s", remotePeer.ToString(), err.Reason())
			sm.inboundTransports.Delete(remotePeer.ToString())
			newStream.Close()
			continue
		}

		go sm.handleInStream(newStream, remotePeer)
		sm.logger.Warnf("Accepted connection from %s successfully", remotePeer.ToString())
	}
}

func (sm streamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream transport.Stream) {

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
		sm.waitDial(dialingProto, toDial, waitChan)
		sm.dialingTransportsMutex.Unlock()
		return
	}
	done := make(chan interface{})
	sm.dialingTransports.Store(toDial.ToString(), done)
	defer func() {
		sm.dialingTransports.Delete(toDial.ToString())
		close(done)
	}()

	sm.dialingTransportsMutex.Unlock()
	err := stream.Dial(toDial)
	if err != nil {
		err.Log(sm.logger)
		dialError(dialingProto, toDial)
		return
	}

	// sm.logger.Infof("Done dialing node %s", toDial.Addr())
	// sm.logger.Infof("Exchanging protos")
	//sm.logger.Info("Remote protos: %d", handshakeMsg.Protos)
	//sm.logger.Infof("Starting handshake...")

	err = sm.sendHandshakeMessage(stream, RegisteredProtos(), PermanentTunnel)
	if err != nil {
		sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.ToString(), err.Reason())
		dialError(dialingProto, toDial)
		stream.Close()
		return
	}

	handshakeMsg, err := sm.waitForHandshakeMessage(stream)
	if err != nil {
		stream.Close()
		sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.ToString(), err.Reason())
		return
	}

	hbChannel := hbTimerRoutineChannels{
		finish: make(chan interface{}),
		renew:  make(chan struct{}),
	}
	sm.hbChannels.Store(toDial.ToString(), hbChannel)
	sm.outboundTransports.Store(toDial.ToString(), stream)

	if !dialSuccess(dialingProto, handshakeMsg.Protos, toDial) {
		sm.logger.Warn("No protocol accepted conn")
		sm.hbChannels.Delete(toDial.ToString())
		sm.outboundTransports.Delete(toDial.ToString())
		stream.Close()
		return
	}
	go sm.startConnHeartbeat(stream, toDial)
	sm.logger.Warnf("Dialed %s successfully", toDial.ToString())
}

func (sm streamManager) SendMessage(message []byte, peer peer.Peer) errors.Error {
	stream, ok := sm.outboundTransports.Load(peer.ToString())
	if !ok {
		return errors.NonFatalError(404, "stream not found", streamManagerCaller)
	}
	sm.renewHBTimer(peer)
	_, err := stream.(transport.Stream).Write(message)
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
		conn.(transport.Stream).Close()
		sm.outboundTransports.Delete(peer.ToString())
	}
	if conn, ok := sm.inboundTransports.Load(peer.ToString()); ok {
		conn.(transport.Stream).Close()
		sm.inboundTransports.Delete(peer.ToString())
	}
	sm.dialingTransportsMutex.Unlock()
}

func (sm streamManager) handleInStream(t transport.Stream, newPeer peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Handling peer stream %s", newPeer.ToString())
	defer sm.logger.Warnf("[ConnectionEvent] : Done handling peer stream %s", newPeer.ToString())
	deserializer := message.AppMessageWrapperSerializer{}
	for {
		t.SetReadTimeout(p.config.ConnectionReadTimeout)
		msgBuf := make([]byte, 2048)
		n, err := t.Read(msgBuf)
		if err != nil {
			if err == io.EOF {
				sm.logger.Warnf("Read routine from %s got %s, exiting cleanly...", newPeer.ToString(), err)
			} else {
				sm.logger.Error(err)
			}
			sm.inboundTransports.Delete(newPeer.ToString())
			t.Close()
			return
		}

		//sm.logger.Infof("Read %d bytes from %s", n, newPeer.ToString())
		deserialized := deserializer.Deserialize(msgBuf[:n])
		protoMsg := deserialized.(*message.AppMessageWrapper)
		if protoMsg.MessageID == message.HeartbeatMessageType {
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

func (sm streamManager) renewHBTimer(peer peer.Peer) {
	defer func() {
		if r := recover(); r != nil {
			sm.logger.Warnf("Error occurred renewing hb timer: %s", r)
		}
	}()
	hbChan, ok := sm.hbChannels.Load(peer.ToString())
	if ok {
		select {
		case <-hbChan.(hbTimerRoutineChannels).finish:
		case hbChan.(hbTimerRoutineChannels).renew <- struct{}{}:
		}
	}
}

func (sm streamManager) startConnHeartbeat(transport transport.Stream, peer peer.Peer) {
	hbChannel := hbTimerRoutineChannels{
		finish: make(chan interface{}),
		renew:  make(chan struct{}),
	}
	sm.hbChannels.Store(peer.ToString(), hbChannel)

	finishChan := hbChannel.finish
	renewChan := hbChannel.renew

	hbMessage := message.HeartbeatMessage{}
	wrapperMessage := &message.AppMessageWrapper{
		MessageID:   hbMessage.Type(),
		SourceProto: hbProtoInternalID,
		DestProtos:  []protocol.ID{hbProtoInternalID},
	}

	defer sm.hbChannels.Delete(peer.ToString())
	for {
		select {
		case <-finishChan:
			sm.logger.Warn("Heartbeat routine exiting...")
			return
		case <-renewChan:
		case <-time.After(p.config.HeartbeatTickDuration):
			//sm.logger.Info("Sending heartbeat")
			_, err := transport.Write(appMsgSerializer.Serialize(wrapperMessage))
			if err != nil {
				sm.handleOutboundTransportFailure(peer)
				return
			}
		}
	}
}

func (sm streamManager) waitDial(dialerProto protocol.ID, toDial peer.Peer, waitChan chan interface{}) {
	select {
	case <-waitChan:
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
	case <-time.After(p.config.DialTimeout):
		sm.logger.Error("Protocol waiting to dial has timed out")
		proto, _ := p.protocols.Load(dialerProto)
		proto.(protocolValueType).DialFailed(toDial)
	}
}

func (sm streamManager) handleOutboundTransportFailure(remotePeer peer.Peer) errors.Error {
	sm.dialingTransportsMutex.Lock()
	_, ok := sm.outboundTransports.Load(remotePeer.ToString())
	if ok {
		sm.outboundTransports.Delete(remotePeer.ToString())
	}
	_, ok = sm.inboundTransports.Load(remotePeer.ToString())
	if ok {
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

func (sm streamManager) handleTmpStream(newPeer peer.Peer, transport transport.Stream) {

	if newPeer.ToString() == SelfPeer().ToString() {
		panic("Dialing self")
	}

	//sm.logger.Info("Reading from tmp stream")
	msg, err := sm.readAppMessage(transport)
	if err != nil {
		err.Log(sm.logger)
		transport.Close()
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
	transport.Close()
}

func (sm streamManager) waitForHandshakeMessage(transport transport.Stream) (*message.ProtoHandshakeMessage, errors.Error) {
	msgBytes := make([]byte, 2048)
	read, err := transport.Read(msgBytes)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes[:read]).(message.ProtoHandshakeMessage)
	//sm.logger.Infof("Received proto exchange message %+v", msg)
	return &msg, nil
}

func (sm streamManager) readAppMessage(stream transport.Stream) (*message.AppMessageWrapper, errors.Error) {
	msgBuf := make([]byte, 2048)
	n, err := stream.Read(msgBuf)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	deserialized := appMsgDeserializer.Deserialize(msgBuf[:n]).(*message.AppMessageWrapper)
	return deserialized, nil
}

func (sm streamManager) sendHandshakeMessage(transport transport.Stream, destProtos []protocol.ID, chanType uint8) errors.Error {
	var toSend = message.NewProtoHandshakeMessage(destProtos, SelfPeer().Addr(), chanType)
	msgBytes := protoMsgSerializer.Serialize(toSend)
	_, err := transport.Write(msgBytes)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	return nil
}
