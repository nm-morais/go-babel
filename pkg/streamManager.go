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

		handshakeMsg, err := exchangeHandshakeMessage(newStream, RegisteredProtos(), false)
		if err != nil {
			err.Log()
			newStream.Close()
			continue
		}

		remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)
		if handshakeMsg.TemporaryConn == 1 {
			handleTmpStream(remotePeer, newStream)
			newStream.Close()
			continue
		}

		if !inConnRequested(handshakeMsg.Protos, remotePeer) {
			newStream.Close()
			log.Info(fmt.Sprintf("Peer %s did not have matching protocols (%+v)", remotePeer, handshakeMsg.Protos))
			continue
		}

		sm.activeTransports.Store(remotePeer.ToString(), newStream)
		go handleStream(newStream, remotePeer)
	}
}

func (sm streamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, stream transport.Stream) {
	log.Infof("Dialing: %s", toDial.ToString())
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
	sm.dialingTransportsMutex.Unlock()

	err := stream.Dial(toDial)
	if err != nil {
		dialError(dialingProto, toDial)
		return
	}

	defer func() {
		close(done)
		sm.dialingTransports.Delete(toDial.ToString())
	}()

	// log.Infof("Done dialing node %s", toDial.Addr())
	// log.Infof("Exchanging protos")
	//log.Info("Remote protos: %d", handshakeMsg.Protos)
	log.Infof("doing handshake...")
	handshakeMsg, err := exchangeHandshakeMessage(stream, RegisteredProtos(), false)
	if err != nil {
		stream.Close()
		return
	}
	log.Infof("handshake done...")
	remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)

	if !dialSuccess(dialingProto, handshakeMsg.Protos, remotePeer) {
		log.Warn("No protocol accepted conn")
		stream.Close()
		return
	}

	sm.activeTransports.Store(remotePeer.ToString(), stream)
	go handleStream(stream, remotePeer)
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
	msg, err := readAppMessage(transport)
	if err != nil {
		err.Log()
		transport.Close()
		return
	}
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

func handleStream(t transport.Stream, newPeer peer.Peer) {
	log.Infof("Handling peer stream %s", newPeer.ToString())
	defer log.Infof("Done handling peer stream %s", newPeer.ToString())

	hbChannel := make(chan *struct{})
	go startConnHeartbeat(newPeer, hbChannel)
	deserializer := message.AppMessageWrapperSerializer{}
	for {
		t.SetReadTimeout(p.config.ConnectionReadTimeout)
		msgBuf := make([]byte, 2048)
		n, err := t.Read(msgBuf)
		if err != nil {
			if err == io.EOF {
				log.Info("Read routine exiting cleanly...")
			} else {
				handleTransportFailure(newPeer)
			}
			close(hbChannel)
			return
		}
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

func startConnHeartbeat(peer peer.Peer, hbChan chan *struct{}) {
	hbMessage := message.HeartbeatMessage{}
	for {
		select {
		case _, ok := <-hbChan:
			if !ok {
				log.Info("Heartbeat routine exiting...")
				return
			}
		case <-time.After(p.config.HeartbeatTickDuration):
			if err := SendMessage(hbMessage, peer, hbProtoInternalID, []protocol.ID{hbProtoInternalID}); err != nil {
				err.Log()
				return
			}
		}
	}
}

func exchangeHandshakeMessage(transport transport.Stream, selfProtos []protocol.ID, tempChan bool) (*message.ProtoHandshakeMessage, errors.Error) {
	var tempChanUint8 uint8 = 0
	if tempChan == true {
		tempChanUint8 = 1
	}
	go func() {
		var toSend = message.NewProtoHandshakeMessage(selfProtos, SelfPeer().Addr(), tempChanUint8)
		log.Infof("Sending proto exchange message %+v", toSend)
		if _, err := transport.Write(protoMsgSerializer.Serialize(toSend)); err != nil {
			log.Error(err)
			//		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
		}
	}()

	msgBytes := make([]byte, 2048)
	log.Infof("Reading proto exchange message...")
	read, err := transport.Read(msgBytes)
	log.Infof("Done")
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes[:read]).(message.ProtoHandshakeMessage)
	log.Infof("Msg: %+v", msg)
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

func sendHandshakeMessage(transport transport.Stream, selfProtos []protocol.ID, tempChan bool) {
	var tempChanUint8 uint8 = 0
	if tempChan == true {
		tempChanUint8 = 1
	}
	var toSend = message.NewProtoHandshakeMessage(selfProtos, SelfPeer().Addr(), tempChanUint8)

	msgBytes := protoMsgSerializer.Serialize(toSend)
	_, _ = transport.Write(msgBytes)
}
