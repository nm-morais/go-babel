package pkg

import (
	"fmt"
	"github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/internal/notificationHub"
	internalProto "github.com/nm-morais/go-babel/internal/protocol"
	"github.com/nm-morais/go-babel/internal/serialization"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/handlers"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"reflect"
	"sync"
	"time"
)

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
	listener                stream.Stream
	logger                  *log.Logger
}

var p *ProtoManager
var protoMsgSerializer = message.ProtoHandshakeMessageSerializer{}
var appMsgSerializer = message.AppMessageWrapperSerializer{}

func InitProtoManager(configs configs.ProtocolManagerConfig, listener stream.Stream) *ProtoManager {
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
		logger:                  logs.NewLogger(ProtoManagerCaller),
	}
	p.serializationManager.RegisterSerializer(message.HeartbeatMessageType, message.HeartbeatSerializer{})
	return p
}

func RegisterProtocol(protocol protocol.Protocol) errors.Error {
	_, ok := p.protocols.Load(protocol.ID())
	for _, protoID := range reservedProtos {
		if protocol.ID() == protoID {
			p.logger.Panicf("Trying to add protocol with invalid ID (reserved by internal mechanisms). Reserved protos: %+v", reservedProtos)
		}
	}

	if ok {
		return errors.FatalError(409, "Protocol already registered", ProtoManagerCaller)
	}
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
	p.logger.Infof("Protocol %d registered handler for msg %+v", protoID, reflect.TypeOf(message))

	p.serializationManager.RegisterSerializer(message.Type(), message.Serializer())
	p.serializationManager.RegisterDeserializer(message.Type(), message.Deserializer())

	proto.(protocolValueType).RegisterMessageHandler(message.Type(), handler)
	return nil
}

func SendMessage(toSend message.Message, destPeer peer.Peer, origin protocol.ID, destinations []protocol.ID) chan interface{} {
	//p.logger.Infof("Sending message of type %s", reflect.TypeOf(toSend))
	errChan := make(chan interface{})
	go func() {
		msgBytes := p.serializationManager.Serialize(toSend)
		wrapper := message.NewAppMessageWrapper(toSend.Type(), origin, destinations, msgBytes)
		//p.logger.Infof("Sending %s to %s", reflect.TypeOf(toSend), destPeer.ToString())
		toSendBytes := appMsgSerializer.Serialize(wrapper)
		// p.logger.Infof("Sending (bytes): %+v", toSendBytes)
		p.streamManager.SendMessage(toSendBytes, destPeer)
		close(errChan)
	}()
	return errChan
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

func MeasureLatencyTo(nrMessages int, peer peer.Peer, callback func(peer.Peer, []time.Duration)) {
	go p.streamManager.MeasureLatencyTo(nrMessages, peer, callback)
}

func SendMessageSideStream(toSend message.Message, targetPeer peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, t stream.Stream) {
	go p.streamManager.SendMessageSideStream(toSend, targetPeer, sourceProtoID, destProtos, t)
}

func Dial(toDial peer.Peer, sourceProtoID protocol.ID, t stream.Stream) {
	p.logger.Warnf("Dialing new node %s", toDial.Addr())
	go p.streamManager.DialAndNotify(sourceProtoID, toDial, t)
}

func dialError(sourceProto protocol.ID, dialedPeer peer.Peer) {
	callerProto, ok := p.protocols.Load(sourceProto)
	if !ok {
		p.logger.Panicf("Proto %d not found", sourceProto)
	}
	callerProto.(protocolValueType).DialFailed(dialedPeer)
}

func dialSuccess(dialerProto protocol.ID, remoteProtos []protocol.ID, dialedPeer peer.Peer) bool {
	p.channelSubscribersMutex.Lock()
	subs := p.channelSubscribers[dialedPeer.ToString()]
	if subs == nil {
		subs = make(map[protocol.ID]bool)
	}

	for _, destProtoID := range remoteProtos {
		proto, ok := p.protocols.Load(destProtoID)
		if ok {
			convertedProto := proto.(protocolValueType)
			if convertedProto.DialSuccess(dialerProto, dialedPeer) {
				subs[convertedProto.ID()] = true
			}
		}
	}

	if len(subs) == 0 {
		delete(p.channelSubscribers, dialedPeer.ToString())
		p.channelSubscribersMutex.Unlock()
		return false
	}
	p.channelSubscribers[dialedPeer.ToString()] = subs
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

func outTransportFailure(peer peer.Peer) {
	p.logger.Warn("Handling transport failure from ", peer.ToString())
	p.channelSubscribersMutex.Lock()
	toNotify := p.channelSubscribers[peer.ToString()]
	for protoID := range toNotify {
		proto, _ := p.protocols.Load(protoID)
		proto.(protocolValueType).OutConnDown(peer)
	}
	delete(p.channelSubscribers, peer.ToString())
	p.channelSubscribersMutex.Unlock()
}

func Disconnect(source protocol.ID, peer peer.Peer) {
	p.logger.Warnf("Proto %d disconnecting from peer %s", source, peer.ToString())
	p.channelSubscribersMutex.Lock()
	subs := p.channelSubscribers[peer.ToString()]
	delete(subs, source)
	if len(subs) == 0 {
		p.logger.Warnf("Disconnecting from %s", peer.ToString())
		p.streamManager.Disconnect(peer)
	}
	p.channelSubscribersMutex.Unlock()
}

func SelfPeer() peer.Peer {
	return p.selfPeer
}

type NameHook struct {
	name string
}

func setupLoggers() {
	logFolder := p.config.LogFolder + p.listener.ListenAddr().String() + "/"
	err := os.Mkdir(logFolder, 0777)
	if err != nil {
		log.Panic(err)
	}

	allLogsFile, err := os.Create(logFolder + "all.log")
	if err != nil {
		log.Panic(err)
	}
	all := io.MultiWriter(os.Stdout, allLogsFile)

	p.protocols.Range(func(key, proto interface{}) bool {
		protoName := proto.(protocolValueType).Name()
		protoFile, err := os.Create(logFolder + fmt.Sprintf("%s.log", protoName))
		if err != nil {
			log.Panic(err)
		}
		logger := proto.(protocolValueType).Logger()
		mw := io.MultiWriter(all, protoFile)
		logger.SetOutput(mw)
		return true
	})

	protoManagerFile, err := os.Create(logFolder + "protoManager.log")
	if err != nil {
		log.Panic(err)
	}
	pmMw := io.MultiWriter(all, protoManagerFile)
	p.logger.SetOutput(pmMw)
	streamManagerFile, err := os.Create(logFolder + "streamManager.log")
	if err != nil {
		log.Panic(err)
	}
	streamManagerLogger := p.streamManager.Logger()
	smMw := io.MultiWriter(all, streamManagerFile)
	streamManagerLogger.SetOutput(smMw)
	return
}

func Start() {

	setupLoggers()

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
			var toLog string
			toLog = "inbound connections : "
			p.logger.Info("------------- Protocol Manager state -------------")
			p.streamManager.(streamManager).inboundTransports.Range(func(peer, conn interface{}) bool {
				toLog += fmt.Sprintf("%s, ", peer.(string))
				return true
			})
			p.logger.Info(toLog)
			toLog = ""
			toLog = "outbound connections : "
			p.streamManager.(streamManager).outboundTransports.Range(func(peer, conn interface{}) bool {
				toLog += fmt.Sprintf("%s, ", peer.(string))
				return true
			})
			p.logger.Info(toLog)
			p.logger.Info("--------------------------------------------------")
		}
	}
}
