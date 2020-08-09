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
	"time"
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
	Write(message message.Message, peer peer.Peer, origin protocol.ID, destinations []protocol.ID) Error
	SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) Error
	Dial(peer peer.Peer, sourceProto protocol.ID, transport transport.Stream) Error
	dialFailed(peer peer.Peer, sourceProto protocol.ID)
	dialSuccess(peer peer.Peer, sourceProto protocol.ID)
	inConnRequested(peerProtos []protocol.ID, peer peer.Peer, transport transport.Stream) bool
	transportFailure(peerProtos []protocol.ID, transport transport.Stream)
}
*/

type protocolValueType = *internalProto.WrapperProtocol

var hbProtoInternalID = protocol.ID(1)
var reservedProtos = []protocol.ID{hbProtoInternalID}

type ProtoManager struct {
	selfPeer                peer.Peer
	config                  configs.ProtocolManagerConfig
	notificationHub         notificationHub.NotificationHub
	serializationManager    *serialization.Manager
	protocols               *sync.Map
	protoIds                []protocol.ID
	streamManager           StreamManager
	channelSubscribers      map[string]map[protocol.ID]bool
	channelSubscribersMutex *sync.Mutex
	listener                transport.Stream
}

var p *ProtoManager
var protoMsgSerializer = message.ProtoHandshakeMessageSerializer{}
var appMsgSerializer = message.AppMessageWrapperSerializer{}

func InitProtoManager(configs configs.ProtocolManagerConfig, listener transport.Stream) *ProtoManager {
	p = &ProtoManager{
		config:                  configs,
		selfPeer:                peer.NewPeer(listener.ListenAddr()),
		notificationHub:         notificationHub.NewNotificationHub(),
		serializationManager:    serialization.NewSerializationManager(),
		protocols:               &sync.Map{},
		protoIds:                []protocol.ID{},
		streamManager:           NewStreamManager(),
		listener:                listener,
		channelSubscribers:      make(map[string]map[protocol.ID]bool),
		channelSubscribersMutex: &sync.Mutex{},
	}
	p.serializationManager.RegisterSerializer(message.HeartbeatMessageType, message.HeartbeatSerializer{})
	return p
}

func RegisterProtocol(protocol protocol.Protocol) errors.Error {
	_, ok := p.protocols.Load(protocol.ID())
	for _, protoID := range reservedProtos {
		if protocol.ID() == protoID {
			log.Panicf("Trying to add protocol with invalid ID (reserved by internal mechanisms). Reserved protos: %+v", reservedProtos)
		}
	}

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
	//log.Infof("Sending message of type %s", reflect.TypeOf(toSend))
	go func() {
		msgBytes := p.serializationManager.Serialize(toSend)
		wrapper := message.NewAppMessageWrapper(toSend.Type(), origin, destinations, msgBytes)
		// log.Infof("Sending: %+v", wrapper)
		toSendBytes := appMsgSerializer.Serialize(wrapper)
		// log.Infof("Sending (bytes): %+v", toSendBytes)
		p.streamManager.SendMessage(toSendBytes, destPeer)
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

func SendMessageTempTransport(toSend message.Message, targetPeer peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, t transport.Stream) {
	go func() {
		if err := t.Dial(targetPeer); err != nil {
			log.Errorf("Failed to establish temporary stream due to: %s", err.Reason())
			return
		}
		sendHandshakeMessage(t, destProtos, TemporaryTunnel)
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := message.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
		// log.Info("Sending message sideChannel")
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		_, _ = t.Write(wrappedBytes)
		t.Close()
	}()
}

func Dial(toDial peer.Peer, sourceProtoID protocol.ID, t transport.Stream) {
	log.Warnf("Dialing new node %s", toDial.Addr())
	go p.streamManager.DialAndNotify(sourceProtoID, toDial, t)
}

func dialError(sourceProto protocol.ID, dialedPeer peer.Peer) {
	callerProto, ok := p.protocols.Load(sourceProto)
	if !ok {
		log.Panicf("Proto %d not found", sourceProto)
	}
	callerProto.(protocolValueType).DialFailed(dialedPeer)
}

func dialSuccess(dialerProto protocol.ID, remoteProtos []protocol.ID, dialedPeer peer.Peer) bool {
	p.channelSubscribersMutex.Lock()
	for _, destProtoID := range remoteProtos {
		proto, ok := p.protocols.Load(destProtoID)
		if ok {
			convertedProto := proto.(protocolValueType)
			if convertedProto.DialSuccess(dialerProto, dialedPeer) {
				subs := p.channelSubscribers[dialedPeer.ToString()]
				if subs == nil {
					subs = make(map[protocol.ID]bool)
				}
				subs[convertedProto.ID()] = true
				p.channelSubscribers[dialedPeer.ToString()] = subs
			}
		}
	}
	if len(p.channelSubscribers[dialedPeer.ToString()]) == 0 {
		delete(p.channelSubscribers, dialedPeer.ToString())
		p.channelSubscribersMutex.Unlock()
		return false
	}
	p.channelSubscribersMutex.Unlock()
	return true
}

func inConnRequested(remoteProtos []protocol.ID, dialer peer.Peer) bool {
	p.channelSubscribersMutex.Lock()
	subs := p.channelSubscribers[dialer.ToString()]
	if subs == nil {
		subs = make(map[protocol.ID]bool)
	}
	for _, remoteProtoID := range remoteProtos {
		if proto, ok := p.protocols.Load(remoteProtoID); ok {
			if proto.(protocolValueType).InConnRequested(dialer) {
				subs[proto.(protocolValueType).ID()] = true
			}
		}
	}
	if len(subs) == 0 {
		delete(p.channelSubscribers, dialer.ToString())
		p.channelSubscribersMutex.Unlock()
		return false
	}
	p.channelSubscribers[dialer.ToString()] = subs
	p.channelSubscribersMutex.Unlock()
	return true
}

func handleTransportFailure(peer peer.Peer) {
	log.Info("Handling transport failure from ", peer.ToString())
	p.channelSubscribersMutex.Lock()
	toNotify := p.channelSubscribers[peer.ToString()]
	for protoID := range toNotify {
		proto, _ := p.protocols.Load(protoID)
		proto.(protocolValueType).TransportFailure(peer)
	}
	delete(p.channelSubscribers, peer.ToString())
	p.channelSubscribersMutex.Unlock()
}

func Disconnect(source protocol.ID, peer peer.Peer) {
	p.channelSubscribersMutex.Lock()
	subs := p.channelSubscribers[peer.ToString()]
	delete(subs, source)
	if len(subs) == 0 {
		p.streamManager.Disconnect(peer)
	}
	p.channelSubscribersMutex.Unlock()
	log.Warnf("Disconnecting from %s", peer.ToString())
}

func SelfPeer() peer.Peer {
	return p.selfPeer
}

func Start() {

	go p.streamManager.AcceptConnectionsAndNotify()

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})

	logTicker := time.NewTicker(time.Second * 3)
	for {
		select {
		case <-logTicker.C:
			/*
				log.Info("------------- Protocol Manager state -------------")
				var toLog string
				toLog = "Active connections : "
				p.activeTransports.Range(func(peer, conn interface{}) bool {
					toLog += fmt.Sprintf("%s, ", peer.(string))
					return true
				})
				log.Info(toLog)
			*/
		}
	}
}
