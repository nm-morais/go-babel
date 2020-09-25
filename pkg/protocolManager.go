package pkg

import (
	"fmt"
	"io"

	_ "net/http/pprof"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
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
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
	log "github.com/sirupsen/logrus"
)

const ProtoManagerCaller = "ProtoManager"

type ProtocolManagerConfig struct {
	Cpuprofile       bool
	Memprofile       bool
	LogFolder        string
	HandshakeTimeout time.Duration
	DialTimeout      time.Duration
	Peer             peer.Peer
}

type protocolValueType = internalProto.WrapperProtocol

type ProtoManager struct {
	tq                      TimerQueue
	nodeWatcher             NodeWatcher
	config                  ProtocolManagerConfig
	notificationHub         notificationHub.NotificationHub
	serializationManager    *serialization.Manager
	protocols               *sync.Map
	protoIds                []protocol.ID
	streamManager           StreamManager
	listeners               []stream.Stream
	channelSubscribers      map[string]map[protocol.ID]bool
	channelSubscribersMutex *sync.Mutex
	logger                  *log.Logger
}

var p ProtoManager
var protoMsgSerializer = internalMsg.ProtoHandshakeMessageSerializer{}
var appMsgSerializer = internalMsg.AppMessageWrapperSerializer{}

func InitProtoManager(configs ProtocolManagerConfig) ProtoManager {
	p = ProtoManager{
		config:                  configs,
		notificationHub:         notificationHub.NewNotificationHub(),
		serializationManager:    serialization.NewSerializationManager(),
		protocols:               &sync.Map{},
		protoIds:                []protocol.ID{},
		streamManager:           NewStreamManager(),
		channelSubscribers:      make(map[string]map[protocol.ID]bool),
		channelSubscribersMutex: &sync.Mutex{},
		logger:                  logs.NewLogger(ProtoManagerCaller),
		tq:                      NewTimerQueue(),
	}
	return p
}

func RegisterListener(listener stream.Stream) {
	p.listeners = append(p.listeners, listener)
}

func RegisterProtocol(protocol protocol.Protocol) errors.Error {
	_, ok := p.protocols.Load(protocol.ID())

	if ok {
		p.logger.Panicf("Protocol %d already registered", protocol.ID())
	}
	protocolWrapper := internalProto.NewWrapperProtocol(protocol)
	p.protocols.Store(protocol.ID(), protocolWrapper)
	p.protoIds = append(p.protoIds, protocol.ID())
	return nil
}

func RegisterNotificationHandler(protoID protocol.ID, notification notification.Notification, handler handlers.NotificationHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	p.notificationHub.AddListener(notification.ID(), proto.(protocolValueType))
	proto.(protocolValueType).RegisterNotificationHandler(notification.ID(), handler)
	return nil
}

func RegisterTimerHandler(protoID protocol.ID, timer timer.ID, handler handlers.TimerHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterTimerHandler(timer, handler)
	return nil
}

func RegisterRequestHandler(protoID protocol.ID, request request.ID, handler handlers.RequestHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterRequestHandler(request, handler)
	return nil
}

func RegisterMessageHandler(protoID protocol.ID, message message.Message, handler handlers.MessageHandler) errors.Error {
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

func SendMessage(toSend message.Message, destPeer peer.Peer, origin protocol.ID, destinations []protocol.ID) {
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
		//p.logger.Infof("Sending message of type %s", reflect.TypeOf(toSend))
		msgBytes := p.serializationManager.Serialize(toSend)
		wrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), origin, destinations, msgBytes)
		//p.logger.Infof("Sending %s to %s", reflect.TypeOf(toSend), destPeer.ToString())
		toSendBytes := appMsgSerializer.Serialize(wrapper)
		// p.logger.Infof("Sending (bytes): %+v", toSendBytes)
		err := p.streamManager.SendMessage(toSendBytes, destPeer)
		if err != nil {
			proto.(protocolValueType).MessageDeliveryErr(toSend, destPeer, err)
			return
		}
		proto.(protocolValueType).MessageDelivered(toSend, destPeer)
	}()
}

func SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) errors.Error {
	_, ok := p.protocols.Load(origin)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}
	proto, ok := p.protocols.Load(destination)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}

	go func() {
		respChan := proto.(protocolValueType).DeliverRequest(request)
		reply := <-respChan
		proto.(protocolValueType).DeliverRequestReply(reply)
	}()
	return nil
}

func SendNotification(notification notification.Notification) errors.Error {
	p.notificationHub.AddNotification(notification)
	return nil
}

func RegisteredProtos() []protocol.ID {
	return p.protoIds
}

func InitNodeWatcher(conf NodeWatcherConf) {
	p.nodeWatcher = NewNodeWatcher(p.config.Peer, conf)
}

func GetNodeWatcher() NodeWatcher {
	return p.nodeWatcher
}

func SendMessageSideStream(toSend message.Message, targetPeer peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, t stream.Stream) {
	callerProto, ok := p.protocols.Load(sourceProtoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", sourceProtoID)
	}
	go func() {
		err := p.streamManager.SendMessageSideStream(toSend, targetPeer, sourceProtoID, destProtos, t)
		if err != nil {
			callerProto.(protocolValueType).MessageDeliveryErr(toSend, targetPeer, err)
			return
		}
		callerProto.(protocolValueType).MessageDelivered(toSend, targetPeer)
	}()

}

func Dial(toDial peer.Peer, sourceProtoID protocol.ID, t stream.Stream) {
	p.logger.Warnf("Dialing new node %s", toDial.ToString())
	go p.streamManager.DialAndNotify(sourceProtoID, toDial, t)
}

func Disconnect(source protocol.ID, peer peer.Peer) {
	go func() {
		p.logger.Warnf("Proto %d disconnecting from peer %s", source, peer.ToString())
		p.channelSubscribersMutex.Lock()
		subs := p.channelSubscribers[peer.ToString()]
		delete(subs, source)
		if len(subs) == 0 {
			p.logger.Warnf("Disconnecting from %s", peer.ToString())
			p.streamManager.Disconnect(peer)
		}
		p.channelSubscribersMutex.Unlock()
	}()
}

func SelfPeer() peer.Peer {
	return p.config.Peer
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

func setupLoggers() {
	logFolder := p.config.LogFolder + p.config.Peer.ToString() + "/"

	os.RemoveAll(logFolder)
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

	timerQueueFile, err := os.Create(logFolder + "timerQueue.log")
	if err != nil {
		log.Panic(err)
	}
	timerQueueLogger := p.tq.Logger()
	tqMw := io.MultiWriter(all, timerQueueFile)
	timerQueueLogger.SetOutput(tqMw)

	if p.nodeWatcher != nil {
		nodeWatcherLogger := p.nodeWatcher.Logger()
		nodeWatcherFile, err := os.Create(logFolder + "nodeWatcher.log")
		if err != nil {
			log.Panic(err)
		}
		nmMw := io.MultiWriter(all, nodeWatcherFile)
		nodeWatcherLogger.SetOutput(nmMw)
	}
}

func CancelTimer(timerID int) errors.Error {
	return p.tq.CancelTimer(timerID)
}

func RegisterTimer(origin protocol.ID, timer timer.Timer) int {
	return p.tq.AddTimer(timer, origin)
}

func Start() {

	setupLoggers()
	setupPerformanceProfiling(p.config.Cpuprofile, p.config.Memprofile)

	for _, l := range p.listeners {
		p.logger.Infof("Starting listener: %s", reflect.TypeOf(l))
		go p.streamManager.AcceptConnectionsAndNotify(l)
	}

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})

	deadline := time.Now().Add(7 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	logFolder := p.config.LogFolder + p.config.Peer.ToString() + "/"
	for {
		select {
		case <-ticker.C:
			fmt.Printf("Remaining time until performance metrics:%d\n", time.Until(deadline))
			fmt.Printf("Nr goroutines=%d\n", runtime.NumGoroutine())
		case <-time.After(time.Until(deadline)):
			if p.config.Cpuprofile {
				pprof.StopCPUProfile()
			}
			if p.config.Memprofile {
				f, err := os.Create(logFolder + "memprofile")
				if err != nil {
					log.Fatal(err)
				}
				err = pprof.WriteHeapProfile(f)
				if err != nil {
					log.Fatal(err)
				}
				f.Close()
			}
		}
	}
}

func setupPerformanceProfiling(doCpuprofile, doMemprofile bool) {

	logFolder := p.config.LogFolder + p.config.Peer.ToString() + "/"
	if doCpuprofile {
		f, err := os.Create(logFolder + "cpuprofile")
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
	}
}
