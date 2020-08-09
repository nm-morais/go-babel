package pkg

import (
	"fmt"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/transport"
	log "github.com/sirupsen/logrus"
	"io"
	"reflect"
	"sync"
	"time"
)

type streamManager struct {
	listeningTransports    []transport.Stream
	dialingTransports      *sync.Map
	dialingTransportsMutex *sync.Mutex

	hbChannels       *sync.Map
	activeTransports *sync.Map
}

var appMsgDeserializer = message.AppMessageWrapperSerializer{}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

type StreamManager interface {
	AcceptConnectionsAndNotify()
	DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream transport.Stream)
	Disconnect(peer peer.Peer)
	SendMessage(message []byte, peer peer.Peer) errors.Error
}

const streamManagerCaller = "streamManagerCaller"

func NewStreamManager() StreamManager {
	return streamManager{
		listeningTransports:    make([]transport.Stream, 0),
		dialingTransports:      &sync.Map{},
		dialingTransportsMutex: &sync.Mutex{},
		hbChannels:             &sync.Map{},
		activeTransports:       &sync.Map{},
	}
}

func (sm streamManager) AcceptConnectionsAndNotify() {
	listener, err := p.listener.Listen()
	if err != nil {
		panic(err)
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
		if handshakeMsg.TunnelType == TemporaryTunnel {
			go handleTmpStream(remotePeer, newStream)
			continue
		}

		err = sendHandshakeMessage(newStream, RegisteredProtos(), handshakeMsg.TunnelType)
		if err != nil {
			err.Log()
			newStream.Close()
			continue
		}

		if !inConnRequested(handshakeMsg.Protos, remotePeer) {
			newStream.Close()
			log.Info(fmt.Sprintf("Peer %s did not have matching protocols (%+v)", remotePeer, handshakeMsg.Protos))
			continue
		}

		sm.activeTransports.Store(remotePeer.ToString(), newStream)
		go sm.handleStream(newStream, remotePeer)
	}
}

func (sm streamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream transport.Stream) {
	log.Infof("Dialing: %s", toDial.ToString())

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

	doneDialing, ok := sm.dialingTransports.Load(toDial.ToString())
	log.Infof("Done dialing: %s", toDial.ToString())
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
		sm.dialingTransports.Delete(toDial.ToString())
	}()

	sm.dialingTransportsMutex.Unlock()
	err := stream.Dial(toDial)
	if err != nil {
		err.Log()
		dialError(dialingProto, toDial)
		return
	}

	// log.Infof("Done dialing node %s", toDial.Addr())
	// log.Infof("Exchanging protos")
	//log.Info("Remote protos: %d", handshakeMsg.Protos)
	log.Infof("doing handshake...")
	err = sendHandshakeMessage(stream, RegisteredProtos(), PermanentTunnel)
	if err != nil {
		err.Log()
		stream.Close()
		return
	}
	handshakeMsg, err := waitForHandshakeMessage(stream)
	if err != nil {
		err.Log()
		stream.Close()
		return
	}

	log.Infof("handshake done...")
	remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)

	sm.activeTransports.Store(remotePeer.ToString(), stream)
	go sm.handleStream(stream, remotePeer)

	if !dialSuccess(dialingProto, handshakeMsg.Protos, remotePeer) {
		log.Warn("No protocol accepted conn")
		stream.Close()
		return
	}
}

func (sm streamManager) SendMessage(message []byte, peer peer.Peer) errors.Error {
	stream, ok := sm.activeTransports.Load(peer.ToString())
	if !ok {
		return errors.NonFatalError(404, "stream not found", streamManagerCaller)
	}
	_, err := stream.(transport.Stream).Write(message)
	if err != nil {
		if err == io.EOF {
			return nil
		} else {
			return errors.NonFatalError(500, err.Error(), streamManagerCaller)
		}
	}
	return nil
}

func (sm streamManager) Disconnect(peer peer.Peer) {
	stream, ok := sm.activeTransports.Load(peer.ToString())
	if !ok {
		return
	}
	stream.(transport.Stream).Close()
}

func handleTmpStream(newPeer peer.Peer, transport transport.Stream) {
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
			toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
		} else {
			log.Errorf("Ignored message: %+v", appMsg)
		}
	}
	transport.Close()
}

func (sm streamManager) handleStream(t transport.Stream, newPeer peer.Peer) {
	log.Warnf("Handling peer stream %s", newPeer.ToString())
	defer log.Warnf("Done handling peer stream %s", newPeer.ToString())

	hbChannel := make(chan *struct{})
	go startConnHeartbeat(t, newPeer, hbChannel)
	deserializer := message.AppMessageWrapperSerializer{}
	for {
		msgBuf := make([]byte, 2048)
		n, err := t.Read(msgBuf)
		if err != nil {
			if err == io.EOF {
				log.Error(err)
				log.Info("Read routine exiting cleanly...")
			} else {
				log.Error(err)
			}
			sm.activeTransports.Delete(newPeer)
			t.Close()
			handleTransportFailure(newPeer)
			close(hbChannel)
			return
		}
		log.Infof("Read %d bytes", n)
		deserialized := deserializer.Deserialize(msgBuf[:n])
		protoMsg := deserialized.(*message.AppMessageWrapper)
		hbChannel <- nil // cancel heartbeat timer

		if protoMsg.MessageID == message.HeartbeatMessageType {
			continue
		}

		for _, toNotifyID := range protoMsg.DestProtos {
			if toNotify, ok := p.protocols.Load(toNotifyID); ok {
				appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
				log.Warnf("Got message: %s", reflect.TypeOf(appMsg))
				toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
			} else {
				log.Panicf("Ignored message: %+v", protoMsg)
			}
		}
	}
}

func startConnHeartbeat(t transport.Stream, peer peer.Peer, hbChan chan *struct{}) {
	hbMessage := message.HeartbeatMessage{}
	for {
		select {
		case _, ok := <-hbChan:
			//log.Info("Setting read timeout")
			t.SetReadTimeout(p.config.ConnectionReadTimeout)
			if !ok {
				log.Warn("Heartbeat routine exiting...")
				return
			}
		case <-time.After(p.config.HeartbeatTickDuration):
			//log.Info("Sending heartbeat")
			if err := SendMessage(hbMessage, peer, hbProtoInternalID, []protocol.ID{hbProtoInternalID}); err != nil {
				err.Log()
				return
			}
		}
	}
}

func waitForHandshakeMessage(transport transport.Stream) (*message.ProtoHandshakeMessage, errors.Error) {
	msgBytes := make([]byte, 2048)
	read, err := transport.Read(msgBytes)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes[:read]).(message.ProtoHandshakeMessage)
	log.Infof("Received proto exchange message %+v", msg)
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
			subs[dialerProto] = true
			p.channelSubscribers[toDial.ToString()] = subs
			p.channelSubscribersMutex.Unlock()
			proto.(protocolValueType).DialSuccess(dialerProto, toDial)
		}
	case <-time.After(p.config.DialTimeout):
		proto, _ := p.protocols.Load(dialerProto)
		proto.(protocolValueType).DialFailed(toDial)
	}
}

func sendHandshakeMessage(transport transport.Stream, selfProtos []protocol.ID, chanType uint8) errors.Error {
	var toSend = message.NewProtoHandshakeMessage(selfProtos, SelfPeer().Addr(), chanType)

	msgBytes := protoMsgSerializer.Serialize(toSend)
	_, err := transport.Write(msgBytes)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	return nil
}
