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
	listeningTransports     []transport.Transport
	channelSubscribers      map[peer.Peer][]protocol.ID
	channelSubscribersMutex *sync.Mutex
}

var p *protoManager
var protoMsgSerializer = message.ProtoHandshakeMessageSerializer{}

func GetProtocolManager() *protoManager {
	return p
}

func InitProtoManager() *protoManager {
	p = &protoManager{
		//	config:                  configs.ReadConfigFile(configFilePath),
		notificationHub:         notificationHub.NewNotificationHub(),
		serializationManager:    serialization.NewSerializationManager(),
		protocols:               &sync.Map{},
		protoIds:                []protocol.ID{},
		dialingTransports:       &sync.Map{},
		dialingTransportsMutex:  &sync.Mutex{},
		activeTransports:        &sync.Map{},
		listeningTransports:     []transport.Transport{},
		channelSubscribers:      make(map[peer.Peer][]protocol.ID),
		channelSubscribersMutex: &sync.Mutex{},
	}
	return p
}

func StartTransportListener(listener transport.Transport) {
	transports := listener.Listen()
	for newPeerTransport := range transports {
		remotePeer := newPeerTransport.Peer()
		remoteProtos := exchangeProtos(newPeerTransport, p.protoIds)
		for _, remoteProtoID := range remoteProtos {
			if proto, ok := p.protocols.Load(remoteProtoID); ok {
				p.channelSubscribersMutex.Lock()
				if proto.(protocolValueType).InConnRequested(remotePeer) {
					p.channelSubscribers[remotePeer] = append(p.channelSubscribers[remotePeer], remoteProtoID)
				}
				p.channelSubscribersMutex.Unlock()
			}
		}
		go handlePeerConn(remotePeer, newPeerTransport.MessageChan())
	}
}

func RegisterTransportListener(listener transport.Transport) {
	p.listeningTransports = append(p.listeningTransports, listener)
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
		wrapper := message.NewAppMessageWrapper(toSend.Type(), origin, destinations, p.serializationManager.Serialize(toSend))
		conn.(transport.Transport).SendMessage(p.serializationManager.Serialize(wrapper))
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
	defer func() { // TODO can be improved to use single routine instead of routine per channel
		timer.Wait()
		callerProto.(protocolValueType).DeliverTimer(timer)
	}()
	return nil
}

func RegisteredProtos() []protocol.ID {
	return p.protoIds
}

func Dial(peer peer.Peer, sourceProtoID protocol.ID, t transport.Transport) errors.Error {
	sourceProto, ok := p.protocols.Load(sourceProtoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	decodedSourceProto := sourceProto.(protocolValueType)

	_, ok = p.activeTransports.Load(peer)
	if ok {
		p.channelSubscribersMutex.Lock()
		if decodedSourceProto.DialSuccess(sourceProtoID, peer) {
			p.channelSubscribers[peer] = append(p.channelSubscribers[peer], sourceProtoID)
		}
		p.channelSubscribersMutex.Unlock()
		return nil
	}

	doneDialing, ok := p.dialingTransports.Load(peer)
	if ok {
		waitChan := doneDialing.(chan interface{})
		defer func() {
			<-waitChan
			_, ok = p.activeTransports.Load(peer)
			if ok {
				p.channelSubscribersMutex.Lock()
				if decodedSourceProto.DialSuccess(sourceProtoID, peer) {
					p.channelSubscribers[peer] = append(p.channelSubscribers[peer], sourceProtoID)
				}
				p.channelSubscribersMutex.Unlock()
			} else {
				decodedSourceProto.DialFailed(peer)
			}
		}()
		return nil
	}

	p.dialingTransportsMutex.Lock()
	doneDialing, ok = p.dialingTransports.Load(peer)
	if ok {
		waitChan := doneDialing.(chan interface{})
		defer func() {
			<-waitChan
			_, ok = p.activeTransports.Load(peer)
			if ok {
				p.channelSubscribersMutex.Lock()
				if decodedSourceProto.DialSuccess(sourceProtoID, peer) {
					p.channelSubscribers[peer] = append(p.channelSubscribers[peer], sourceProtoID)
				}
				p.channelSubscribersMutex.Unlock()
			} else {
				decodedSourceProto.DialFailed(peer)
			}
		}()
		return nil
	}
	done := make(chan interface{})
	p.dialingTransports.Store(peer, done)
	p.dialingTransportsMutex.Unlock()
	go func() {
		log.Infof("Dialing new node %s", peer.Addr())
		errChan := t.Dial(peer)
		err := <-errChan
		if err != nil {
			log.Error(err)
			decodedSourceProto.DialFailed(peer)
			close(done)
			return
		}

		log.Infof("Done dialing node %s", peer.Addr())
		destProtos := exchangeProtos(t, p.protoIds)
		p.activeTransports.Store(peer, t)
		for _, destProtoID := range destProtos {
			proto, ok := p.protocols.Load(destProtoID)
			if ok {
				convertedProto := proto.(protocolValueType)
				p.channelSubscribersMutex.Lock()
				if convertedProto.DialSuccess(sourceProtoID, peer) {
					p.channelSubscribers[peer] = append(p.channelSubscribers[peer], convertedProto.ID())
				}
				p.channelSubscribersMutex.Unlock()
			}
		}
		close(done)
		handlePeerConn(peer, t.MessageChan())
	}()
	return nil
}

func handlePeerConn(peer peer.Peer, msgChan <-chan []byte) {
	deserializer := message.AppMessageWrapperSerializer{}
	for {
		msg, ok := <-msgChan

		deserialized := deserializer.Deserialize(msg)
		protoMsg := deserialized.(*message.AppMessageWrapper)

		if !ok {
			p.channelSubscribersMutex.Lock()
			toNotify := p.channelSubscribers[peer]
			for _, protoID := range toNotify {
				proto, _ := p.protocols.Load(protoID)
				proto.(protocolValueType).TransportFailure(peer)
			}
			p.channelSubscribersMutex.Unlock()
			return
		}
		for _, toNotifyID := range protoMsg.DestProtos {
			if toNotify, ok := p.protocols.Load(toNotifyID); ok {
				appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
				toNotify.(protocolValueType).DeliverMessage(peer, appMsg)
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

func exchangeProtos(transport transport.Transport, selfProtos []protocol.ID) []protocol.ID {
	var toSend = message.NewProtoHandshakeMessage(selfProtos)
	transport.SendMessage(protoMsgSerializer.Serialize(toSend))
	msgChan := transport.MessageChan()
	msgBytes := <-msgChan
	received := protoMsgSerializer.Deserialize(msgBytes)
	return received.(message.ProtoHandshakeMessage).Protos
}

func Start() {

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	for _, listener := range p.listeningTransports {
		go StartTransportListener(listener)
	}

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})

	select {} // in order to hang forever
}
