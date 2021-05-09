package pkg

import (
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

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
	Silent           bool              // Silent represents wether the logger outputs to stdout
	LogFolder        string            // Path to the logs folder
	HandshakeTimeout time.Duration     // time to wait for the handshake process to a peer
	SmConf           StreamManagerConf // configurations for the networking layer
	Peer             peer.Peer         // the advertised address in the handshake process
	PoolSize         int               // the thread pool size
}

type protocolValueType = *internalProto.WrapperProtocol

type protoManager struct {
	// pool                 *ants.Pool
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

func NewProtoManager(configs Config) *protoManager {
	p := &protoManager{
		config:               configs,
		notificationHub:      notificationHub.NewNotificationHub(),
		serializationManager: serialization.NewSerializationManager(),
		protocols:            &sync.Map{},
		protoIds:             []protocol.ID{},
		logger:               logs.NewLogger(ProtoManagerCaller),
	}

	// pool, err := ants.NewPool(256)
	// if err != nil {
	// 	panic(err)
	// }
	// p.pool = pool
	p.tq = NewTimerQueue(p)
	p.streamManager = NewStreamManager(p, configs.SmConf)
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

func (p *protoManager) RegisterProtocol(proto protocol.Protocol) errors.Error {
	_, ok := p.protocols.Load(proto.ID())
	if ok {
		p.logger.Panicf("Protocol %d already registered", proto.ID())
	}
	protocolWrapper := internalProto.NewWrapperProtocol(proto, p)
	p.protocols.Store(proto.ID(), protocolWrapper)
	p.protoIds = append(p.protoIds, proto.ID())
	return nil
}

func (p *protoManager) RegisterNotificationHandler(
	protoID protocol.ID,
	n notification.Notification,
	handler handlers.NotificationHandler,
) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	p.notificationHub.AddListener(n.ID(), proto.(protocolValueType))
	proto.(protocolValueType).RegisterNotificationHandler(n.ID(), handler)
	return nil
}

func (p *protoManager) RegisterTimerHandler(
	protoID protocol.ID,
	t timer.ID,
	handler handlers.TimerHandler,
) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterTimerHandler(t, handler)
	return nil
}

func (p *protoManager) RegisterRequestHandler(
	protoID protocol.ID,
	r request.ID,
	handler handlers.RequestHandler,
) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterRequestHandler(r, handler)
	return nil
}

func (p *protoManager) RegisterRequestReplyHandler(
	protoID protocol.ID,
	r request.ID,
	handler handlers.ReplyHandler,
) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	proto.(protocolValueType).RegisterRequestReplyHandler(r, handler)
	return nil
}

func (p *protoManager) RegisterMessageHandler(
	protoID protocol.ID,
	m message.Message,
	handler handlers.MessageHandler,
) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", protoID)
	}
	p.logger.Infof("Protocol %d registered handler for msg %+v", protoID, reflect.TypeOf(m))

	p.serializationManager.RegisterSerializer(m.Type(), m.Serializer())
	p.serializationManager.RegisterDeserializer(m.Type(), m.Deserializer())

	proto.(protocolValueType).RegisterMessageHandler(m.Type(), handler)
	return nil
}

func (p *protoManager) SendMessageAndDisconnect(
	toSend message.Message,
	destPeer peer.Peer,
	origin protocol.ID,
	destination protocol.ID,
) {
	// p.submitOrWait(func() {
	p.streamManager.SendMessage(toSend, destPeer, origin, destination, false)
	go p.Disconnect(origin, destPeer)
	// })
}

func (p *protoManager) SendMessage(
	toSend message.Message,
	destPeer peer.Peer,
	origin protocol.ID,
	destination protocol.ID,
	batch bool,
) {
	// p.submitOrWait(func() {
	// p.logger.Info("Sending message")
	// defer p.logger.Info("Done Sending message")
	p.streamManager.SendMessage(toSend, destPeer, origin, destination, batch)
	// })
}

// type babelRunnable struct {
// 	f func()
// }

// func (r *babelRunnable) Run() {
// 	r.f()
// }

func (p *protoManager) submitOrWait(f func()) {
	// nrRetries := 5
	// for i := 0; i < 5; i++ {
	// err := p.pool.Execute(&babelRunnable{f: f})
	// err := p.pool.Submit(f)
	// if err != nil {
	// if err.Error() == threadpool.ErrQueueFull.Error() {
	// 	time.Sleep(15 * time.Millisecond)
	// 	continue
	// }
	// p.logger.Panic(err.Error())
	// }
	// return
	// }
	// p.logger.Panicf("Failed to submit task after %d attempts", nrRetries)
	f()
}

func (p *protoManager) SendRequest(r request.Request, origin, destination protocol.ID) errors.Error {
	// p.submitOrWait(func() {
	// p.logger.Info("Sending request")
	// defer p.logger.Info("Done sending request")
	destProto, ok := p.protocols.Load(destination)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}
	destProto.(protocolValueType).DeliverRequest(r, origin)
	// })
	return nil
}

func (p *protoManager) SendRequestReply(r request.Reply, origin, destination protocol.ID) errors.Error {
	// p.submitOrWait(func() {
	p.logger.Info("Sending request reply")
	defer p.logger.Info("Done sending request reply")
	destProto, ok := p.protocols.Load(destination)
	if !ok {
		p.logger.Panicf("Protocol %d not registered", origin)
	}
	destProto.(protocolValueType).DeliverRequestReply(r)
	// })
	return nil
}

func (p *protoManager) SendNotification(n notification.Notification) errors.Error {
	p.submitOrWait(func() {
		// p.logger.Info("CancelTimer")
		// defer p.logger.Info("Done CancelTimer")
		p.notificationHub.AddNotification(n)
	})
	return nil
}

func (p *protoManager) RegisteredProtos() []protocol.ID {
	return p.protoIds
}

func (p *protoManager) SendMessageSideStream(
	toSend message.Message,
	dest peer.Peer,
	addr net.Addr,
	sourceProtoID protocol.ID,
	destProto protocol.ID,
) {
	go p.streamManager.SendMessageSideStream(toSend, dest, addr, sourceProtoID, destProto)
}

func (p *protoManager) Dial(dialingProto protocol.ID, peer peer.Peer, toDial net.Addr) errors.Error {
	// p.logger.Infof("Dialing new node %s", toDial.String())
	return p.streamManager.DialAndNotify(dialingProto, peer, toDial)
}

func (p *protoManager) MessageDelivered(sendingProto protocol.ID, msg message.Message, peer peer.Peer) {
	// p.submitOrWait(func() {
	callerProto, ok := p.protocols.Load(sendingProto)
	if !ok {
		p.logger.Panicf("Protocol %d not registered, message type: %s, contents:%+v", sendingProto, reflect.TypeOf(msg), msg)
	}
	callerProto.(protocolValueType).MessageDelivered(msg, peer)
	// })
}

func (p *protoManager) MessageDeliveryErr(sendingProto protocol.ID, msg message.Message, peer peer.Peer, err errors.Error) {
	// p.submitOrWait(func() {
	callerProto, ok := p.protocols.Load(sendingProto)
	if !ok {
		p.logger.Panicf("Protocol %d not registered, message type: %s, contents:%+v", sendingProto, reflect.TypeOf(msg), msg)

	}
	callerProto.(protocolValueType).MessageDeliveryErr(msg, peer, err)
	// })
}

func (p *protoManager) Disconnect(source protocol.ID, toDc peer.Peer) {
	// p.submitOrWait(func() {
	// p.logger.Info("Disconnecting from ", toDc.String())
	// defer p.logger.Info("Done disconnecting from ", toDc.String())
	go p.streamManager.Disconnect(source, toDc)
	// })
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
	// p.submitOrWait(func() {
	// p.logger.Info("DeliverTimer")
	// defer p.logger.Info("Done DeliverTimer")
	if proto, ok := p.protocols.Load(destProto); ok {
		proto.(protocolValueType).DeliverTimer(t)
	}
	// })
}

func (p *protoManager) DeliverMessage(sender peer.Peer, msg message.Message, destProto protocol.ID) {
	// p.submitOrWait(func() {
	// p.logger.Info("DeliverMessage")
	// defer p.logger.Info("Done DeliverMessage")
	if toNotify, ok := p.protocols.Load(destProto); ok {
		toNotify.(protocolValueType).DeliverMessage(sender, msg)
	} else {
		p.logger.Errorf("Protocol %d not registered, ignoring message type: %s, contents:%+v", destProto, reflect.TypeOf(msg), msg)
	}
	// })
}

func (p *protoManager) DialError(sourceProto protocol.ID, dialedPeer peer.Peer) {
	// p.submitOrWait(func() {
	callerProto, ok := p.protocols.Load(sourceProto)
	if !ok {
		p.logger.Panicf("Proto %d not found", sourceProto)
	}
	callerProto.(protocolValueType).DialFailed(dialedPeer)
	// })
}

func (p *protoManager) DialSuccess(dialerProto protocol.ID, dialedPeer peer.Peer) bool {
	proto, _ := p.protocols.Load(dialerProto)
	convertedProto := proto.(protocolValueType)
	return convertedProto.DialSuccess(dialerProto, dialedPeer)
}

func (p *protoManager) OutTransportFailure(dialerProto protocol.ID, peer peer.Peer) {
	// p.submitOrWait(func() {
	p.logger.Warn("Handling transport failure from ", peer.String())
	proto, _ := p.protocols.Load(dialerProto)
	proto.(protocolValueType).OutConnDown(peer)
	// })
}

func (p *protoManager) setupLoggers() {
	err := os.MkdirAll(p.config.LogFolder, 0777)
	if err != nil {
		log.Panic("Err creating dir", err)
	}
	allLogsFile, err := os.Create(p.config.LogFolder + "/all.log")
	if err != nil {
		log.Panic(err)
	}
	all := io.MultiWriter(allLogsFile)

	if !p.config.Silent {
		all = io.MultiWriter(os.Stdout, all)
	}

	p.protocols.Range(
		func(key, proto interface{}) bool {
			protoName := proto.(protocolValueType).Name()
			protoFile, err := os.Create(p.config.LogFolder + fmt.Sprintf("/%s.log", protoName))
			if err != nil {
				log.Panic(err)
			}
			logger := proto.(protocolValueType).Logger()
			mw := io.MultiWriter(all, protoFile)
			logger.SetOutput(mw)
			return true
		},
	)
	protoManagerFile, err := os.Create(p.config.LogFolder + "/protoManager.log")
	if err != nil {
		log.Panic(err)
	}
	pmMw := io.MultiWriter(all, protoManagerFile)
	p.logger.SetOutput(pmMw)
	streamManagerFile, err := os.Create(p.config.LogFolder + "/streamManager.log")
	if err != nil {
		log.Panic(err)
	}
	streamManagerLogger := p.streamManager.Logger()
	smMw := io.MultiWriter(all, streamManagerFile)
	streamManagerLogger.SetOutput(smMw)

	timerQueueFile, err := os.Create(p.config.LogFolder + "/timerQueue.log")
	if err != nil {
		log.Panic(err)
	}
	timerQueueLogger := p.tq.Logger()
	tqMw := io.MultiWriter(all, timerQueueFile)
	timerQueueLogger.SetOutput(tqMw)
	p.logger.Warn("4")
	if p.nw != nil {
		nodeWatcherLogger := p.nw.Logger()
		nodeWatcherFile, err := os.Create(p.config.LogFolder + "/nodeWatcher.log")
		if err != nil {
			log.Panic(err)
		}
		nmMw := io.MultiWriter(all, nodeWatcherFile)
		nodeWatcherLogger.SetOutput(nmMw)
	}
}

func (p *protoManager) CancelTimer(timerID int) errors.Error {
	p.submitOrWait(func() {
		p.logger.Info("Sending notification")
		defer p.logger.Info("Done sending notification")
		p.tq.CancelTimer(timerID)
	})
	return nil
}

func (p *protoManager) RegisterTimer(origin protocol.ID, timer timer.Timer) int {
	return p.tq.AddTimer(timer, origin)
}

func (p *protoManager) RegisterPeriodicTimer(origin protocol.ID, timer timer.Timer, triggerAtTimeZero bool) int {
	if triggerAtTimeZero {
		// p.logger.Info("Setting periodic timer")
		// defer p.logger.Info("Setting periodic timer")
		defer func() { p.DeliverTimer(timer, origin) }()
	}
	return p.tq.AddPeriodicTimer(timer, origin)
}

func (p *protoManager) StartAsync() {
	go p.StartSync()
}

func (p *protoManager) StartSync() {

	// p.logger.Infof("Starting Sync...")
	p.logger.Infof("Setting up loggers...")
	p.setupLoggers()

	p.logger.Infof("Setting up listeners...")
	for _, l := range p.listenAddrs {
		p.logger.Infof("Starting listener: %s", reflect.TypeOf(l))
		done := p.streamManager.AcceptConnectionsAndNotify(l)
		<-done
	}

	p.logger.Infof("Initializing protocols...")
	p.protocols.Range(
		func(_, proto interface{}) bool {
			proto.(protocolValueType).Init()
			return true
		},
	)

	p.logger.Infof("Starting protocols...")
	p.protocols.Range(
		func(_, proto interface{}) bool {
			go proto.(protocolValueType).Start()
			return true
		},
	)

	select {}
}
