package pkg

import (
	"github.com/DeMMon/go-babel/configs"
	"github.com/DeMMon/go-babel/internal/notificationHub"
	internalProto "github.com/DeMMon/go-babel/internal/protocol"
	"github.com/DeMMon/go-babel/pkg/handlers"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/notification"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/protocol"
	"github.com/DeMMon/go-babel/pkg/request"
	"github.com/DeMMon/go-babel/pkg/timer"
	"github.com/DeMMon/go-babel/pkg/transport"
	"github.com/DeMMon/go-babel/pkg/utils"
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

type ProtocolManager struct {
	config                  configs.ProtocolManagerConfig
	notificationHub         notificationHub.NotificationHub
	protocols               *sync.Map
	activeTransports        *sync.Map
	dialingTransportsMutex  *sync.Mutex
	dialingTransports       *sync.Map
	listeningTransports     []transport.Transport
	channelSubscribersMutex *sync.Mutex
	channelSubscribers      map[peer.Peer][]protocol.ID
	protoIds                []protocol.ID
}

var pm *ProtocolManager

func GetProtocolManager() *ProtocolManager {
	return pm
}

func Init() *ProtocolManager {
	pm = &ProtocolManager{
		config:                  configs.ReadConfigFile(configFilePath),
		notificationHub:         notificationHub.NewNotificationHub(),
		protocols:               &sync.Map{},
		protoIds:                []protocol.ID{},
		listeningTransports:     []transport.Transport{},
		activeTransports:        &sync.Map{},
		dialingTransports:       &sync.Map{},
		channelSubscribers:      make(map[peer.Peer][]protocol.ID),
		channelSubscribersMutex: &sync.Mutex{},
	}
	return pm
}

func (p ProtocolManager) StartTransportListener(listener transport.Transport) {
	transports := listener.Listen()
	for newPeerTransport := range transports {
		remotePeer := newPeerTransport.Peer()
		remoteProtos := transport.ExchangeProtos(newPeerTransport, p.protoIds)
		for _, remoteProtoID := range remoteProtos {
			if proto, ok := p.protocols.Load(remoteProtoID); ok {
				p.channelSubscribersMutex.Lock()
				if proto.(protocolValueType).InConnRequested(remotePeer) {
					p.channelSubscribers[remotePeer] = append(p.channelSubscribers[remotePeer], remoteProtoID)
				}
				p.channelSubscribersMutex.Unlock()
			}
		}
		p.handlePeerConn(remotePeer, newPeerTransport.MessageChan())
	}
}

func (p ProtocolManager) RegisterTransportListener(listener transport.Transport) {
	p.listeningTransports = append(p.listeningTransports, listener)
}

func (p ProtocolManager) RegisterProtocol(protocol protocol.Protocol) Error {
	_, ok := p.protocols.Load(protocol.ID())
	if ok {
		return utils.FatalError(409, "Protocol already registered", ProtoManagerCaller)
	}
	p.protocols.Store(protocol.ID(), protocol)
	p.protoIds = append(p.protoIds, protocol.ID())
	return nil
}

func (p ProtocolManager) RegisterNotificationHandler(protoID protocol.ID, notificationID notification.ID, handler handlers.NotificationHandler) Error {
	proto, ok := p.protocols.Load(protoID)
	if ok {
		return utils.FatalError(409, "Protocol already registered", ProtoManagerCaller)
	}
	p.notificationHub.AddListener(notificationID, proto.(protocolValueType))
	proto.(protocolValueType).RegisterNotificationHandler(notificationID, handler)
	return nil
}

func (p ProtocolManager) RegisterTimerHandler(protoID protocol.ID, timer timer.ID, handler handlers.TimerHandler) Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).RegisterTimerHandler(timer, handler)
	return nil
}

func (p ProtocolManager) RegisterRequestHandler(protoID protocol.ID, request request.ID, handler handlers.RequestHandler) Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).RegisterRequestHandler(request, handler)
	return nil
}

func (p ProtocolManager) RegisterMessageHandler(protoID protocol.ID, messageID message.ID, handler handlers.MessageHandler) Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).RegisterMessageHandler(messageID, handler)
	return nil
}

func (p ProtocolManager) SendMessage(toSend message.Message, peer peer.Peer, origin protocol.ID, destinations []protocol.ID) Error {
	conn, ok := p.activeTransports.Load(peer)
	if !ok {
		return utils.NonFatalError(404, "No active connection to peer", ProtoManagerCaller)
	}
	go conn.(transport.Transport).SendMessage(message.NewAppMessageWrapper(origin, destinations, toSend))
	return nil
}

func (p ProtocolManager) SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) Error {
	_, ok := p.protocols.Load(origin)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto, ok := p.protocols.Load(destination)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	go proto.(protocolValueType).DeliverRequest(origin, request)
	return nil
}

func (p ProtocolManager) SendRequestReply(reply request.Reply, origin protocol.ID, destination protocol.ID) Error {
	_, ok := p.protocols.Load(origin)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto, ok := p.protocols.Load(destination)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).DeliverRequestReply(reply)
	return nil
}

func (p ProtocolManager) SendNotification(notification notification.Notification) Error {
	p.notificationHub.AddNotification(notification)
	return nil
}

func (p ProtocolManager) RegisterTimer(origin protocol.ID, timer timer.Timer) Error {
	callerProto, ok := p.protocols.Load(origin)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	defer func() {
		timer.Wait()
		callerProto.(protocolValueType).DeliverTimer(timer)
	}()
	return nil
}

func (p ProtocolManager) RegisteredProtos() []protocol.ID {
	return p.protoIds
}

func (p ProtocolManager) Dial(peer peer.Peer, sourceProtoID protocol.ID, t transport.Transport) Error {
	sourceProto, ok := p.protocols.Load(sourceProtoID)
	if !ok {
		return utils.FatalError(409, "Protocol not registered", ProtoManagerCaller)
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

	errChan := t.Dial(peer)
	go func() {
		err := <-errChan
		if err != nil {
			decodedSourceProto.DialFailed(peer)
			close(done)
		}
		destProtos := transport.ExchangeProtos(t, p.protoIds)
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
		p.handlePeerConn(peer, t.MessageChan())
	}()
	return nil
}

func (p ProtocolManager) handlePeerConn(peer peer.Peer, msgChan <-chan message.Message) {
	for {
		msg, ok := <-msgChan
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
		serialized := msg.(*message.GenericMessage)
		deserialized := &message.AppMessageWrapper{}
		deserialized.Deserialize(serialized.MSgBytes)
		for _, toNotifyID := range deserialized.Metadata.DestProtos {
			if toNotify, ok := p.protocols.Load(toNotifyID); ok {
				toNotify.(protocolValueType).DeliverMessage(deserialized.WrappedMessage)
			}
		}
	}
}

func (p ProtocolManager) Disconnect(source protocol.ID, peer peer.Peer) {
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

func (p *ProtocolManager) Start() {

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	for _, listener := range p.listeningTransports {
		p.StartTransportListener(listener)
	}

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})

	select {} // in order to hang forever
}
