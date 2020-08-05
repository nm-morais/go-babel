package pkg

import (
	"github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/internal/notificationHub"
	internalProto "github.com/nm-morais/go-babel/internal/protocol"
	"github.com/nm-morais/go-babel/internal/serialization"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/handlers"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/nm-morais/go-babel/pkg/transport"
	log "github.com/sirupsen/logrus"
	"net"
	"reflect"
	"sync"
)

const configFilePath = "./configs/config.json"

const ProtoManagerCaller = "ProtoManager"

/*
type IProtocolManager interface {
	RegisteredProtos() []protocol.ID
	RegisterProtocol(protocol protocol.Protocol) Error
	RegisterRequestHandler(protocol protocol.ID, request request.ID, handler handlers.RequestHandler) Error
	RegisterMessageHandler(protocol protocol.ID, request message.ID, handler handlers.MessageHandler) Error
	ReceiveMessage(message message.Message, peer peer.Peer)
	SendMessage(message message.Message, peer peer.Peer, origin protocol.ID, destinations []protocol.ID) Error
	SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) Error
	Dial(peer peer.Peer, sourceProto protocol.ID, transport transport.Transport) Error
	dialFailed(peer peer.Peer, sourceProto protocol.ID)
	dialSuccess(peer peer.Peer, sourceProto protocol.ID)
	inConnRequested(peerProtos []protocol.ID, peer peer.Peer, transport transport.Transport) bool
	transportFailure(peerProtos []protocol.ID, transport transport.Transport)
}
*/

type protocolValueType = *internalProto.WrapperProtocol
type activeTransportValueType = transport.Transport

type protoManager struct {
	config                  configs.ProtocolManagerConfig
	notificationHub         notificationHub.NotificationHub
	serializationManager    *serialization.Manager
	protocols               *sync.Map
	protoIds                []protocol.ID
	dialingTransports       *sync.Map
	dialingTransportsMutex  *sync.Mutex
	activeTransports        *sync.Map
	listener                transport.Transport
	channelSubscribers      map[peer.Peer][]protocol.ID
	channelSubscribersMutex *sync.Mutex
}

var p *protoManager
var protoMsgSerializer = message.ProtoHandshakeMessageSerializer{}

func GetProtocolManager() *protoManager {
	return p
}

func InitProtoManager(configs configs.ProtocolManagerConfig) *protoManager {
	p = &protoManager{
		config:                  configs,
		notificationHub:         notificationHub.NewNotificationHub(),
		serializationManager:    serialization.NewSerializationManager(),
		protocols:               &sync.Map{},
		protoIds:                []protocol.ID{},
		dialingTransports:       &sync.Map{},
		dialingTransportsMutex:  &sync.Mutex{},
		activeTransports:        &sync.Map{},
		channelSubscribers:      make(map[peer.Peer][]protocol.ID),
		channelSubscribersMutex: &sync.Mutex{},
	}
	return p
}

func handleTransportListener() {
	transports := p.listener.Listen()
	for newPeerTransport := range transports {
		go newPeerTransport.PipeBytesToChan()

		handshakeMsg := exchangeHandshakeMessage(newPeerTransport, p.protoIds, true)
		remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)

		if handshakeMsg.TemporaryConn == 1 {
			go handleIncTmpTransport(remotePeer, newPeerTransport)
			continue
		}

		p.activeTransports.Store(remotePeer, newPeerTransport)
		for _, remoteProtoID := range handshakeMsg.Protos {
			if proto, ok := p.protocols.Load(remoteProtoID); ok {
				p.channelSubscribersMutex.Lock()
				if proto.(protocolValueType).InConnRequested(remotePeer) {
					p.channelSubscribers[remotePeer] = append(p.channelSubscribers[remotePeer], remoteProtoID)
				}
				if len(p.channelSubscribers[remotePeer]) == 0 {
					p.channelSubscribersMutex.Unlock()
					newPeerTransport.Close()
					return
				}
				p.channelSubscribersMutex.Unlock()
			}
		}
		go handlePeerConn(remotePeer, newPeerTransport.MessageChan())
	}
}

func RegisterTransportListener(listener transport.Transport) {
	p.listener = listener
}

func RegisterProtocol(protocol protocol.Protocol) errors.Error {
	_, ok := p.protocols.Load(protocol.ID())
	if ok {
		return errors.FatalError(409, "Protocol already registered", ProtoManagerCaller)
	}
	log.Infof("Protocol %s registered", reflect.TypeOf(protocol))
	protocolWrapper := internalProto.NewWrapperProtocol(protocol)
	p.protocols.Store(protocol.ID(), protocolWrapper)
	p.protoIds = append(p.protoIds, protocol.ID())
	return nil
}

func RegisterNotificationHandler(protoID protocol.ID, notificationID notification.ID, handler handlers.NotificationHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if ok {
		return errors.FatalError(409, "Protocol already registered", ProtoManagerCaller)
	}
	p.notificationHub.AddListener(notificationID, proto.(protocolValueType))
	proto.(protocolValueType).RegisterNotificationHandler(notificationID, handler)
	return nil
}

func RegisterTimerHandler(protoID protocol.ID, timer timer.ID, handler handlers.TimerHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).RegisterTimerHandler(timer, handler)
	return nil
}

func RegisterRequestHandler(protoID protocol.ID, request request.ID, handler handlers.RequestHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).RegisterRequestHandler(request, handler)
	return nil
}

func RegisterMessageHandler(protoID protocol.ID, message message.Message, handler handlers.MessageHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	log.Infof("Protocol %d registered handler for msg %+v", protoID, reflect.TypeOf(message))
	p.serializationManager.RegisterSerializer(message.Type(), message.Serializer())
	p.serializationManager.RegisterDeserializer(message.Type(), message.Deserializer())

	proto.(protocolValueType).RegisterMessageHandler(message.Type(), handler)
	return nil
}

func SendMessage(toSend message.Message, destPeer peer.Peer, origin protocol.ID, destinations []protocol.ID) errors.Error {
	conn, ok := p.activeTransports.Load(destPeer)
	if !ok {
		return errors.NonFatalError(404, "No active connection to peer", ProtoManagerCaller)
	}

	go func() {
		msgBytes := p.serializationManager.Serialize(toSend)
		wrapper := message.NewAppMessageWrapper(toSend.Type(), origin, destinations, msgBytes)
		// log.Infof("Sending: %+v", wrapper)
		toSendBytes := wrapper.Serializer().Serialize(wrapper)
		// log.Infof("Sending (bytes): %+v", toSendBytes)
		err := conn.(transport.Transport).SendMessage(toSendBytes)
		if err != nil {
			err.Log()
		}
	}()

	return nil
}

func SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) errors.Error {
	_, ok := p.protocols.Load(origin)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto, ok := p.protocols.Load(destination)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}

	go func() {
		respChan := proto.(protocolValueType).DeliverRequest(request)
		reply := <-respChan
		SendRequestReply(reply, destination, origin)
	}()
	return nil
}

func SendRequestReply(reply request.Reply, origin protocol.ID, destination protocol.ID) errors.Error {
	_, ok := p.protocols.Load(origin)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto, ok := p.protocols.Load(destination)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).DeliverRequestReply(reply)
	return nil
}

func SendNotification(notification notification.Notification) errors.Error {
	p.notificationHub.AddNotification(notification)
	return nil
}

func RegisterTimer(origin protocol.ID, timer timer.Timer) errors.Error {
	callerProto, ok := p.protocols.Load(origin)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	go func() { // TODO can be improved to use single routine instead of routine per channel
		timer.Wait()
		callerProto.(protocolValueType).DeliverTimer(timer)
	}()
	return nil
}

func RegisteredProtos() []protocol.ID {
	return p.protoIds
}

func SendMessageTempTransport(toSend message.Message, targetPeer peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, t transport.Transport) {
	errChan := t.Dial(targetPeer)
	go func() {
		err := <-errChan
		if err != nil {
			log.Error("Side channel dial failed")
			log.Error(err)
			return
		}
		go t.PipeBytesToChan()
		_ = exchangeHandshakeMessage(t, []protocol.ID{}, true)
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := message.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
		// log.Info("Sending message sideChannel")
		wrappedBytes := msgWrapper.Serializer().Serialize(msgWrapper)
		t.SendMessage(wrappedBytes)
		t.Close()
	}()
}

func handleIncTmpTransport(newPeer peer.Peer, transport transport.Transport) {
	deserializer := message.AppMessageWrapperSerializer{}
	msgChan := transport.MessageChan()
	// log.Info("Waiting for message on temp channel...")
	msg, ok := <-msgChan
	if !ok {
		log.Error("temporary conn exited before receiving applicational message")
		return
	}
	// log.Info("received message via temp channel")
	deserialized := deserializer.Deserialize(msg)
	protoMsg := deserialized.(*message.AppMessageWrapper)
	appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
	for _, toNotifyID := range protoMsg.DestProtos {
		if toNotify, ok := p.protocols.Load(toNotifyID); ok {
			toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
		} else {
			log.Errorf("Ignored message: %+v", protoMsg)
		}
	}
	transport.Close()
}

func Dial(toDial peer.Peer, sourceProtoID protocol.ID, t transport.Transport) errors.Error {
	sourceProto, ok := p.protocols.Load(sourceProtoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	decodedSourceProto := sourceProto.(protocolValueType)

	_, ok = p.activeTransports.Load(toDial)
	if ok {
		p.channelSubscribersMutex.Lock()
		if decodedSourceProto.DialSuccess(sourceProtoID, toDial) {
			p.channelSubscribers[toDial] = append(p.channelSubscribers[toDial], sourceProtoID)
		}
		p.channelSubscribersMutex.Unlock()
		return nil
	}

	doneDialing, ok := p.dialingTransports.Load(toDial)
	if ok {
		waitChan := doneDialing.(chan interface{})
		go func() {
			<-waitChan
			_, ok = p.activeTransports.Load(toDial)
			if !ok {
				decodedSourceProto.DialFailed(toDial)
			}
		}()
		return nil
	}

	p.dialingTransportsMutex.Lock()
	doneDialing, ok = p.dialingTransports.Load(toDial)
	if ok {
		waitChan := doneDialing.(chan interface{})
		go func() {
			<-waitChan
			_, ok = p.activeTransports.Load(toDial)
			if !ok {
				decodedSourceProto.DialFailed(toDial)
			}
		}()
		return nil
	}
	done := make(chan interface{})
	p.dialingTransports.Store(toDial, done)
	p.dialingTransportsMutex.Unlock()
	go func() {
		log.Infof("Dialing new node %s", toDial.Addr())
		errChan := t.Dial(toDial)
		err := <-errChan
		if err != nil {
			err.Log()
			decodedSourceProto.DialFailed(toDial)
			close(done)
			return
		}
		go t.PipeBytesToChan()
		log.Infof("Done dialing node %s", toDial.Addr())
		log.Infof("Exchanging protos")
		handshakeMsg := exchangeHandshakeMessage(t, p.protoIds, false)
		remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)
		log.Infof("Done exchanging protos")
		p.activeTransports.Store(remotePeer, t)

		for _, destProtoID := range handshakeMsg.Protos {
			proto, ok := p.protocols.Load(destProtoID)
			if ok {
				convertedProto := proto.(protocolValueType)
				p.channelSubscribersMutex.Lock()
				if convertedProto.DialSuccess(sourceProtoID, remotePeer) {
					p.channelSubscribers[remotePeer] = append(p.channelSubscribers[remotePeer], convertedProto.ID())
				}
				if len(p.channelSubscribers[remotePeer]) == 0 {
					p.channelSubscribersMutex.Unlock()
					t.Close()
					return
				}
				p.channelSubscribersMutex.Unlock()
			}
		}
		close(done)
		handlePeerConn(remotePeer, t.MessageChan())
	}()
	return nil
}

func handlePeerConn(newPeer peer.Peer, msgChan <-chan []byte) {
	deserializer := message.AppMessageWrapperSerializer{}
	for {
		msg, ok := <-msgChan
		if !ok {
			p.channelSubscribersMutex.Lock()
			toNotify := p.channelSubscribers[newPeer]
			for _, protoID := range toNotify {
				proto, _ := p.protocols.Load(protoID)
				proto.(protocolValueType).TransportFailure(newPeer)
			}
			p.channelSubscribersMutex.Unlock()
			p.dialingTransportsMutex.Lock()
			p.activeTransports.Delete(newPeer)
			p.dialingTransports.Delete(newPeer)
			p.dialingTransportsMutex.Unlock()
			return
		}
		deserialized := deserializer.Deserialize(msg)
		protoMsg := deserialized.(*message.AppMessageWrapper)
		//log.Infof("Got protoMessage: %+v", protoMsg)
		for _, toNotifyID := range protoMsg.DestProtos {
			if toNotify, ok := p.protocols.Load(toNotifyID); ok {
				appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
				//log.Infof("Notifying: %s", reflect.TypeOf(toNotify.(protocolValueType)))
				toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
			} else {
				log.Infof("Ignored messages: %+v", protoMsg)
			}
		}
	}
}

func Disconnect(source protocol.ID, peer peer.Peer) {
	p.channelSubscribersMutex.Lock()
	subscribers := p.channelSubscribers[peer]
	for i, protoID := range subscribers {
		if protoID == source {
			subscribers[i] = subscribers[len(subscribers)-1]
			newArr := subscribers[:len(subscribers)-1]
			p.channelSubscribers[peer] = newArr
			if len(p.channelSubscribers[peer]) == 0 {
				if t, ok := p.activeTransports.Load(peer); ok {
					t.(activeTransportValueType).Close()
				}
				delete(p.channelSubscribers, peer)
			}
			break
		}
	}
	p.channelSubscribersMutex.Unlock()
}

func exchangeHandshakeMessage(transport transport.Transport, selfProtos []protocol.ID, tempChan bool) message.ProtoHandshakeMessage {
	var tempChanUint8 uint8 = 0
	if tempChan == true {
		tempChanUint8 = 1
	}
	var toSend = message.NewProtoHandshakeMessage(selfProtos, p.config.ListenAddr, tempChanUint8)
	// log.Infof("Sending proto exchange message %+v", toSend)
	transport.SendMessage(protoMsgSerializer.Serialize(toSend))
	msgChan := transport.MessageChan()
	// log.Infof("Waiting for proto exchange message")
	msgBytes := <-msgChan
	received := protoMsgSerializer.Deserialize(msgBytes).(message.ProtoHandshakeMessage)
	// log.Infof("Received proto exchange message: %+v", received)
	return received
}

func Addr() net.Addr {
	return p.config.ListenAddr
}

func Start() {

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	go handleTransportListener()

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})

	select {} // in order to hang forever
}
