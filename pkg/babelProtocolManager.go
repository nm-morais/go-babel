package pkg

import (
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	internalMsg "github.com/nm-morais/go-babel/internal/message"
	"github.com/nm-morais/go-babel/internal/notificationHub"
	internalProto "github.com/nm-morais/go-babel/internal/protocol"
	"github.com/nm-morais/go-babel/internal/serialization"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/handlers"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/serializationManager"
	"github.com/nm-morais/go-babel/pkg/streamManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	log "github.com/sirupsen/logrus"
)

const ProtoManagerCaller = "ProtoManager"

type Config struct {
	LogStdout        bool
	Cpuprofile       bool
	Memprofile       bool
	LogFolder        string
	HandshakeTimeout time.Duration
	SmConf           StreamManagerConf
	Peer             peer.Peer
}

type protocolValueType = internalProto.WrapperProtocol

type protoManager struct {
	nw                   nodeWatcher.NodeWatcher
	tq                   TimerQueue
	config               Config
	notificationHub      notificationHub.NotificationHub
	serializationManager *serialization.Manager
	protocols            *sync.Map
	protoIds             []protocol.ID
	streamManager        streamManager.StreamManager
	listenAddrs          []net.Addr
	logger               *log.Logger
}

var protoMsgSerializer = internalMsg.ProtoHandshakeMessageSerializer{}
var appMsgSerializer = internalMsg.AppMessageWrapperSerializer{}

func NewProtoManager(configs Config) *protoManager {
	p := &protoManager{
		config:               configs,
		notificationHub:      notificationHub.NewNotificationHub(),
		serializationManager: serialization.NewSerializationManager(),
		protocols:            &sync.Map{},
		protoIds:             []protocol.ID{},
		logger:               logs.NewLogger(ProtoManagerCaller),
	}
	p.tq = NewTimerQueue(p)
	p.streamManager = NewStreamManager(p, p.config.SmConf)
	return p
}

func (p *protoManager) SerializationManager() serializationManager.SerializationManager {
	return p.serializationManager
}

func (p *protoManager) RegisterNodeWatcher(nw nodeWatcher.NodeWatcher) {
	p.nw = nw
}

func (p *protoManager) RegisterListenAddr(addr net.Addr) {
	p.listenAddrs = append(p.listenAddrs, addr)
}

func (p *protoManager) RegisterProtocol(protocol protocol.Protocol) errors.Error {
	_, ok := p.protocols.Load(protocol.ID())
	if ok {
		p.logger.Panicf("Protocol %d already registered", protocol.ID())
	}
	protocolWrapper := internalProto.NewWrapperProtocol(protocol)
	p.protocols.Store(protocol.ID(), protocolWrapper)
	p.protoIds = append(p.protoIds, protocol.ID())
	return nil
}

func (p *protoManager) RegisterNotificationHandler(protoID protocol.ID, notification notification.Notification, handler handlers.NotificationHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	p.notificationHub.AddListener(notification.ID(), proto.(protocolValueType))
	proto.(protocolValueType).RegisterNotificationHandler(notification.ID(), handler)
	return nil
}

func (p *protoManager) RegisterTimerHandler(protoID protocol.ID, timer timer.ID, handler handlers.TimerHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterTimerHandler(timer, handler)
	return nil
}

func (p *protoManager) RegisterRequestHandler(protoID protocol.ID, request request.ID, handler handlers.RequestHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterRequestHandler(request, handler)
	return nil
}

func (p *protoManager) RegisterRequestReplyHandler(protoID protocol.ID, request request.ID, handler handlers.ReplyHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterRequestReplyHandler(request, handler)
	return nil
}

func (p *protoManager) RegisterMessageHandler(protoID protocol.ID, message message.Message, handler handlers.MessageHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	p.logger.Infof("Protocol %d registered handler for msg %+v", protoID, reflect.TypeOf(message))

	p.serializationManager.RegisterSerializer(message.Type(), message.Serializer())
	p.serializationManager.RegisterDeserializer(message.Type(), message.Deserializer())

	proto.(protocolValueType).RegisterMessageHandler(message.Type(), handler)
	return nil
}

func (p *protoManager) SendMessage(toSend message.Message, destPeer peer.Peer, origin protocol.ID, destination protocol.ID) {
	proto, ok := p.protocols.Load(origin)
	defer func() {
		if r := recover(); r != nil {
			proto.(protocolValueType).MessageDeliveryErr(toSend, destPeer, errors.NonFatalError(500, "an error ocurred sending message", ProtoManagerCaller))
		}
	}()

	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}
	go func() {
		err := p.streamManager.SendMessage(toSend, destPeer, origin, destination)
		if err != nil {
			proto.(protocolValueType).MessageDeliveryErr(toSend, destPeer, err)
			return
		}
		proto.(protocolValueType).MessageDelivered(toSend, destPeer)
	}()
}

func (p *protoManager) SendMessageAndDisconnect(toSend message.Message, destPeer peer.Peer, origin protocol.ID, destination protocol.ID) {
	proto, ok := p.protocols.Load(origin)
	defer func() {
		if r := recover(); r != nil {
			proto.(protocolValueType).MessageDeliveryErr(toSend, destPeer, errors.NonFatalError(500, "an error ocurred sending message", ProtoManagerCaller))
		}
	}()

	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}
	go func() {
		defer p.Disconnect(origin, destPeer)
		err := p.streamManager.SendMessage(toSend, destPeer, origin, destination)
		if err != nil {
			proto.(protocolValueType).MessageDeliveryErr(toSend, destPeer, err)
			return
		}
		proto.(protocolValueType).MessageDelivered(toSend, destPeer)
	}()
}

func (p *protoManager) SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) errors.Error {
	originProto, ok := p.protocols.Load(origin)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}
	destProto, ok := p.protocols.Load(destination)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}

	go func() {
		respChan := destProto.(protocolValueType).DeliverRequest(request)
		reply := <-respChan
		originProto.(protocolValueType).DeliverRequestReply(reply)
	}()
	return nil
}

func (p *protoManager) SendNotification(notification notification.Notification) errors.Error {
	p.notificationHub.AddNotification(notification)
	return nil
}

func (p *protoManager) RegisteredProtos() []protocol.ID {
	return p.protoIds
}

func (p *protoManager) SendMessageSideStream(toSend message.Message, peer peer.Peer, addr net.Addr, sourceProtoID protocol.ID, destProto protocol.ID) {
	callerProto, ok := p.protocols.Load(sourceProtoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", sourceProtoID)
	}
	go func() {
		err := p.streamManager.SendMessageSideStream(toSend, peer, addr, sourceProtoID, destProto)
		if err != nil {
			callerProto.(protocolValueType).MessageDeliveryErr(toSend, peer, err)
			return
		}
		callerProto.(protocolValueType).MessageDelivered(toSend, peer)
	}()

}

func (p *protoManager) Dial(dialingProto protocol.ID, peer peer.Peer, toDial net.Addr) errors.Error {
	p.logger.Infof("Dialing new node %s", toDial.String())
	return p.streamManager.DialAndNotify(dialingProto, peer, toDial)
}

func (p *protoManager) Disconnect(source protocol.ID, toDc peer.Peer) {
	p.streamManager.Disconnect(source, toDc)
}

func (p *protoManager) SelfPeer() peer.Peer {
	return p.config.Peer
}

func (p *protoManager) InConnRequested(targetProto protocol.ID, dialer peer.Peer) bool {
	if proto, ok := p.protocols.Load(targetProto); ok {
		return proto.(protocolValueType).InConnRequested(targetProto, dialer)
	}
	panic("No proto for dialer proto")
}

// protoMsg.DestProtos

func (p *protoManager) DeliverTimer(t timer.Timer, destProto protocol.ID) {
	if proto, ok := p.protocols.Load(destProto); ok {
		proto.(protocolValueType).DeliverTimer(t)
	}
}

func (p *protoManager) DeliverMessage(sender peer.Peer, message message.Message, destProto protocol.ID) {
	if toNotify, ok := p.protocols.Load(destProto); ok {
		toNotify.(protocolValueType).DeliverMessage(sender, message)
	} else {
		p.logger.Panicf("Ignored message: %+v", message)
	}
}

func (p *protoManager) DialError(sourceProto protocol.ID, dialedPeer peer.Peer) {
	callerProto, ok := p.protocols.Load(sourceProto)
	if !ok {
		p.logger.Panicf("Proto %d not found", sourceProto)
	}
	callerProto.(protocolValueType).DialFailed(dialedPeer)
}

func (p *protoManager) DialSuccess(dialerProto protocol.ID, dialedPeer peer.Peer) bool {
	proto, _ := p.protocols.Load(dialerProto)
	convertedProto := proto.(protocolValueType)
	return convertedProto.DialSuccess(dialerProto, dialedPeer)
}

func (p *protoManager) OutTransportFailure(dialerProto protocol.ID, peer peer.Peer) {
	p.logger.Warn("Handling transport failure from ", peer.String())
	proto, _ := p.protocols.Load(dialerProto)
	proto.(protocolValueType).OutConnDown(peer)
}

func (p *protoManager) setupLoggers() {
	logFolder := p.config.LogFolder + p.config.Peer.String() + "/"
	err := os.Mkdir(logFolder, 0777)
	if err != nil {
		log.Panic(err)
	}
	allLogsFile, err := os.Create(logFolder + "all.log")
	if err != nil {
		log.Panic(err)
	}
	// all := io.MultiWriter(os.Stdout, allLogsFile)
	all := io.MultiWriter(allLogsFile)

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

	timerQueueFile, err := os.Create(logFolder + "timerQueue.log")
	if err != nil {
		log.Panic(err)
	}
	timerQueueLogger := p.tq.Logger()
	tqMw := io.MultiWriter(all, timerQueueFile)
	timerQueueLogger.SetOutput(tqMw)

	if p.nw != nil {
		nodeWatcherLogger := p.nw.Logger()
		nodeWatcherFile, err := os.Create(logFolder + "nodeWatcher.log")
		if err != nil {
			log.Panic(err)
		}
		nmMw := io.MultiWriter(all, nodeWatcherFile)
		nodeWatcherLogger.SetOutput(nmMw)
	}
}

func (p *protoManager) CancelTimer(timerID int) errors.Error {
	return p.tq.CancelTimer(timerID)
}

func (p *protoManager) RegisterTimer(origin protocol.ID, timer timer.Timer) int {
	return p.tq.AddTimer(timer, origin)
}

func (p *protoManager) StartBackground() {
	p.setupLoggers()
	for _, l := range p.listenAddrs {
		p.logger.Infof("Starting listener: %s", reflect.TypeOf(l))
		done := p.streamManager.AcceptConnectionsAndNotify(l)
		<-done
	}

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})
}

func (p *protoManager) Start() {
	p.setupLoggers()
	for _, l := range p.listenAddrs {
		p.logger.Infof("Starting listener: %s", reflect.TypeOf(l))
		done := p.streamManager.AcceptConnectionsAndNotify(l)
		<-done
	}

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})
	select {}
}
