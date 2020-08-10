package pkg

import (
	"github.com/nm-morais/go-babel/pkg/errors"
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

	hbChannels       *sync.Map
	activeTransports *sync.Map
	hbTimers         *sync.Map
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
	Disconnect(peer peer.Peer)
	SendMessage(message []byte, peer peer.Peer) errors.Error
}

const streamManagerCaller = "streamManagerCaller"

func NewStreamManager() StreamManager {
	return streamManager{
		hbTimers:               &sync.Map{},
		dialingTransports:      &sync.Map{},
		dialingTransportsMutex: &sync.Mutex{},
		hbChannels:             &sync.Map{},
		activeTransports:       &sync.Map{},
	}
}

func (sm streamManager) AcceptConnectionsAndNotify() {
	listener, err := p.listener.Listen()
	if err != nil {
		panic(err.Reason())
	}
	for {
		newStream, err := listener.Accept()
		if err != nil {
			err.Log()
			newStream.Close()
			continue
		}

		handshakeMsg, err := waitForHandshakeMessage(newStream)
		if err != nil {
			err.Log()
			newStream.Close()
			continue
		}
		remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)
		//log.Infof("Got handshake message %+v from peer %s", handshakeMsg, remotePeer.ToString())
		if handshakeMsg.TunnelType == TemporaryTunnel {
			go handleTmpStream(remotePeer, newStream)
			continue
		}

		hbChannel := hbTimerRoutineChannels{
			finish: make(chan interface{}),
			renew:  make(chan struct{}),
		}
		sm.hbChannels.Store(remotePeer.ToString(), hbChannel)
		sm.activeTransports.Store(remotePeer.ToString(), newStream)

		if !inConnRequested(handshakeMsg.Protos, remotePeer) {
			newStream.Close()
			sm.hbChannels.Delete(remotePeer.ToString())
			sm.activeTransports.Delete(remotePeer.ToString())
			log.Infof("Peer %s conn was not accepted, closing stream", remotePeer.ToString())
			continue
		}

		err = sendHandshakeMessage(newStream, RegisteredProtos(), handshakeMsg.TunnelType)
		if err != nil {
			err.Log()
			sm.hbChannels.Delete(remotePeer.ToString())
			sm.activeTransports.Delete(remotePeer.ToString())
			newStream.Close()
			continue
		}

		go sm.startConnHeartbeatSendRoutine(newStream, remotePeer)
		go sm.handleStream(newStream, remotePeer)
		log.Warnf("Accepted connection from %s successfully", remotePeer.ToString())
	}
}

func (sm streamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream transport.Stream) {

	if toDial.ToString() == SelfPeer().ToString() {
		panic("Dialing self")
	}

	log.Warnf("Dialing: %s", toDial.ToString())

	/*
		_, connUp := sm.activeTransports.Load(toDial.ToString())
		if connUp {
			callerProto, _ := p.protocols.Load(dialingProto)
			callerProto.(protocolValueType).DialSuccess(dialingProto, toDial)
			p.channelSubscribersMutex.Lock()
			subs := p.channelSubscribers[toDial.ToString()]
			subs[dialingProto] = true
			p.channelSubscribers[toDial.ToString()] = subs
			p.channelSubscribersMutex.Unlock()
		}
	*/

	doneDialing, ok := sm.dialingTransports.Load(toDial.ToString())
	if ok {
		waitChan := doneDialing.(chan interface{})
		sm.waitDial(dialingProto, toDial, waitChan)
		return
	}

	sm.dialingTransportsMutex.Lock()
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
		close(done)
	}()

	sm.dialingTransportsMutex.Unlock()
	err := stream.Dial(toDial)
	if err != nil {
		err.Log()
		dialError(dialingProto, toDial)
		return
	}
	sm.dialingTransports.Delete(toDial.ToString())

	// log.Infof("Done dialing node %s", toDial.Addr())
	// log.Infof("Exchanging protos")
	//log.Info("Remote protos: %d", handshakeMsg.Protos)
	//log.Infof("Starting handshake...")
	err = sendHandshakeMessage(stream, RegisteredProtos(), PermanentTunnel)
	if err != nil {
		log.Errorf("An error occurred during handshake with %s: %s", toDial.ToString(), err.Reason())
		dialError(dialingProto, toDial)
		stream.Close()
		return
	}
	handshakeMsg, err := waitForHandshakeMessage(stream)
	if err != nil {
		log.Errorf("An error occurred during handshake with %s: %s", toDial.ToString(), err.Reason())
		dialError(dialingProto, toDial)
		stream.Close()
		return
	}

	//log.Infof("handshake done...")
	remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)
	//log.Infof("Got handshake message %+v from peer %s, (dialed peer: %s)", handshakeMsg, remotePeer.ToString(), toDial.ToString())

	hbChannel := hbTimerRoutineChannels{
		finish: make(chan interface{}),
		renew:  make(chan struct{}),
	}
	sm.hbChannels.Store(remotePeer.ToString(), hbChannel)
	sm.hbChannels.Store(remotePeer.ToString(), hbChannel)
	sm.activeTransports.Store(remotePeer.ToString(), stream)

	if !dialSuccess(dialingProto, handshakeMsg.Protos, remotePeer) {
		log.Warn("No protocol accepted conn")
		sm.hbChannels.Delete(remotePeer.ToString())
		sm.activeTransports.Delete(remotePeer.ToString())
		stream.Close()
		return
	}
	go sm.startConnHeartbeatSendRoutine(stream, remotePeer)
	go sm.handleStream(stream, remotePeer)
	log.Warnf("Dialed %s successfully", toDial.ToString())
}

func (sm streamManager) SendMessage(message []byte, peer peer.Peer) errors.Error {
	stream, ok := sm.activeTransports.Load(peer.ToString())
	if !ok {
		return errors.NonFatalError(404, "stream not found", streamManagerCaller)
	}
	sm.renewHBTimer(peer)
	_, err := stream.(transport.Stream).Write(message)
	if err != nil {
		if err == io.EOF {
			return errors.NonFatalError(500, err.Error(), streamManagerCaller)
		} else {
			return errors.NonFatalError(500, err.Error(), streamManagerCaller)
		}
	}
	return nil
}

func (sm streamManager) Disconnect(peer peer.Peer) {
	log.Warnf("[ConnectionEvent] : Disconnecting from %s", peer.ToString())
	stream, ok := sm.activeTransports.Load(peer.ToString())
	if !ok {
		return
	}
	stream.(transport.Stream).Close()
}

func handleTmpStream(newPeer peer.Peer, transport transport.Stream) {

	if newPeer.ToString() == SelfPeer().ToString() {
		panic("Dialing self")
	}

	//log.Info("Reading from tmp stream")
	msg, err := readAppMessage(transport)
	if err != nil {
		err.Log()
		transport.Close()
		return
	}
	//log.Info("Done reading from tmp stream")

	appMsg := p.serializationManager.Deserialize(msg.MessageID, msg.WrappedMsgBytes)
	for _, toNotifyID := range msg.DestProtos {
		if toNotify, ok := p.protocols.Load(toNotifyID); ok {
			//log.Infof("Proto %d Got message: %s from %s", toNotifyID, reflect.TypeOf(appMsg), newPeer.ToString())
			toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
		} else {
			log.Errorf("Ignored message: %+v", appMsg)
		}
	}
	transport.Close()
}

func (sm streamManager) handleStream(t transport.Stream, newPeer peer.Peer) {
	log.Warnf("[ConnectionEvent] : Handling peer stream %s", newPeer.ToString())
	defer log.Warnf("[ConnectionEvent] : Done handling peer stream %s", newPeer.ToString())
	deserializer := message.AppMessageWrapperSerializer{}
	for {
		t.SetReadTimeout(p.config.ConnectionReadTimeout)
		msgBuf := make([]byte, 2048)
		n, err := t.Read(msgBuf)
		if err != nil {
			if err == io.EOF {
				log.Warnf("Read routine from %s got %s, exiting cleanly...", newPeer.ToString(), err)
			} else {
				log.Error(err)
			}
			sm.activeTransports.Delete(newPeer.ToString())
			hbChan, ok := sm.hbChannels.Load(newPeer.ToString())
			if ok {
				sm.hbChannels.Delete(newPeer.ToString())
				close(hbChan.(hbTimerRoutineChannels).finish)
			}
			t.Close()
			warnTransportFailure(newPeer)
			return
		}

		//log.Infof("Read %d bytes from %s", n, newPeer.ToString())
		deserialized := deserializer.Deserialize(msgBuf[:n])
		protoMsg := deserialized.(*message.AppMessageWrapper)
		if protoMsg.MessageID == message.HeartbeatMessageType {
			continue
		}

		for _, toNotifyID := range protoMsg.DestProtos {
			if toNotify, ok := p.protocols.Load(toNotifyID); ok {
				appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
				//log.Infof("Proto %d Got message: %s from %s", toNotifyID, reflect.TypeOf(appMsg), newPeer.ToString())
				toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
			} else {
				log.Panicf("Ignored message: %+v", protoMsg)
			}
		}
	}
}

func (sm streamManager) renewHBTimer(peer peer.Peer) {
	defer func() {
		if r := recover(); r != nil {
			log.Warnf("Error occurred renewing hb timer: %s", r)
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

func (sm streamManager) startConnHeartbeatSendRoutine(transport transport.Stream, peer peer.Peer) {
	hbChan, ok := sm.hbChannels.Load(peer.ToString())
	if !ok {
		return
	}

	finishChan := hbChan.(hbTimerRoutineChannels).finish
	renewChan := hbChan.(hbTimerRoutineChannels).renew

	hbMessage := message.HeartbeatMessage{}
	wrapperMessage := &message.AppMessageWrapper{
		MessageID:   hbMessage.Type(),
		SourceProto: hbProtoInternalID,
		DestProtos:  []protocol.ID{hbProtoInternalID},
	}
	for {
		select {
		case <-finishChan:
			log.Warn("Heartbeat routine exiting...")
			return
		case <-renewChan:
		case <-time.After(p.config.HeartbeatTickDuration):
			//log.Info("Sending heartbeat")
			if _, err := transport.Write(appMsgSerializer.Serialize(wrapperMessage)); err != nil {
				log.Error(err)
				return
			}
		}
	}
}

func (sm streamManager) waitDial(dialerProto protocol.ID, toDial peer.Peer, waitChan chan interface{}) {
	select {
	case <-waitChan:
		proto, _ := p.protocols.Load(dialerProto)
		_, connUp := sm.activeTransports.Load(toDial.ToString())
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
		log.Error("Protocol waiting to dial has timed out")
		proto, _ := p.protocols.Load(dialerProto)
		proto.(protocolValueType).DialFailed(toDial)
	}
}

func waitForHandshakeMessage(transport transport.Stream) (*message.ProtoHandshakeMessage, errors.Error) {
	msgBytes := make([]byte, 2048)
	read, err := transport.Read(msgBytes)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes[:read]).(message.ProtoHandshakeMessage)
	//log.Infof("Received proto exchange message %+v", msg)
	return &msg, nil
}

func readAppMessage(stream transport.Stream) (*message.AppMessageWrapper, errors.Error) {
	msgBuf := make([]byte, 2048)
	n, err := stream.Read(msgBuf)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	deserialized := appMsgDeserializer.Deserialize(msgBuf[:n]).(*message.AppMessageWrapper)
	return deserialized, nil
}

func sendHandshakeMessage(transport transport.Stream, destProtos []protocol.ID, chanType uint8) errors.Error {
	var toSend = message.NewProtoHandshakeMessage(destProtos, SelfPeer().Addr(), chanType)
	msgBytes := protoMsgSerializer.Serialize(toSend)
	_, err := transport.Write(msgBytes)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	return nil
}
