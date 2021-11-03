package frontend

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	// blockingqueue "github.com/nm-morais/go-babel/pkg/dataStructures/blockingQueue"
	// blockingqueue "github.com/nm-morais/go-babel/pkg/dataStructures/blockingQueue"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/handlers"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
)

type event struct {
	eventype     eventType
	eventContent interface{}
}

type eventType uint8

const (
	notificationEvent = eventType(iota)
	requestEvent
	requestReplyEvent
	messageEvent
	timerEvent

	messageDeliverySucessEvent
	messageDeliveryFailureEvent
	inConnRequestedEvent
	dialFailedEvent
	dialSuccessEvent
	outConnDownEvent
)

const QueueSize = 10_000_000

type WrapperProtocol struct {
	id              protocol.ID
	wrappedProtocol protocol.Protocol

	// handlers
	notificationHandlers map[notification.ID]handlers.NotificationHandler
	requestHandlers      map[request.ID]handlers.RequestHandler
	messageHandlers      map[message.ID]handlers.MessageHandler
	replyHandlers        map[request.ID]handlers.ReplyHandler
	timerHandlers        map[timer.ID]handlers.TimerHandler

	// event queues
	networkEventsQueue chan *event
	eventQueue         chan *event

	babel protocolManager.ProtocolManager
}

type reqWithOriginProto struct {
	req         request.Request
	originProto protocol.ID
}

type dialSuccessWithBoolReplyChan struct {
	dialingProto protocol.ID
	peer         peer.Peer
	respChan     chan bool
}

type reqWithReplyChan struct {
	req      request.Request
	respChan chan request.Reply
}

type messageWithPeer struct {
	peer    peer.Peer
	message message.Message
}

type messageWithPeerAndErr struct {
	peer    peer.Peer
	message message.Message
	err     errors.Error
}

type inConnReqEventWithBoolReply struct {
	dialerProto protocol.ID
	peer        peer.Peer
	respChan    chan bool
}

func NewWrapperProtocol(p protocol.Protocol, babel protocolManager.ProtocolManager) *WrapperProtocol {
	wp := &WrapperProtocol{
		id:                   p.ID(),
		wrappedProtocol:      p,
		notificationHandlers: make(map[notification.ID]handlers.NotificationHandler),
		requestHandlers:      make(map[request.ID]handlers.RequestHandler),
		messageHandlers:      make(map[message.ID]handlers.MessageHandler),
		replyHandlers:        make(map[request.ID]handlers.ReplyHandler),
		timerHandlers:        make(map[timer.ID]handlers.TimerHandler),
		eventQueue:           make(chan *event, QueueSize),
		networkEventsQueue:   make(chan *event, QueueSize),
		babel:                babel,
	}

	return wp
}

func (pw *WrapperProtocol) Enqueue(ev *event, canDetach bool) {

	switch ev.eventype {
	case dialSuccessEvent, dialFailedEvent, inConnRequestedEvent, outConnDownEvent:
		if !canDetach {
			pw.networkEventsQueue <- ev
		} else {
			select {
			case pw.networkEventsQueue <- ev:
			default:
				go pw.Enqueue(ev, false)
			}
		}
	default:
		if !canDetach {
			pw.eventQueue <- ev
		} else {
			select {
			case pw.eventQueue <- ev:
			default:
				go pw.Enqueue(ev, false)
			}
		}
	}
	// select {
	// case pw.eventQueue <- ev:
	// 	return
	// case <-time.After(3 * time.Second):
	// 	pw.Logger().Panicf("Protocol is overloaded with too many events: %d", len(pw.eventQueue))
	// }
}

//  channel Deliverers

func (pw *WrapperProtocol) DeliverRequestReply(reply request.Reply) {
	pw.Enqueue(NewEvent(requestReplyEvent, reply), true)
}

func (pw *WrapperProtocol) DeliverRequestSync(req request.Request) request.Reply {
	aux := &reqWithReplyChan{
		req:      req,
		respChan: make(chan request.Reply),
	}
	go pw.Enqueue(NewEvent(requestEvent, aux), true)
	return <-aux.respChan
}

func (pw *WrapperProtocol) DeliverNotification(n notification.Notification) {
	pw.Enqueue(NewEvent(notificationEvent, n), true)
}

func (pw *WrapperProtocol) DeliverMessage(sender peer.Peer, msg message.Message) {
	pw.Enqueue(
		NewEvent(
			messageEvent, messageWithPeer{
				peer:    sender,
				message: msg,
			},
		), true,
	)
}

func (pw *WrapperProtocol) DeliverTimer(t timer.Timer) {
	pw.Enqueue(NewEvent(timerEvent, t), true)
}

func (pw *WrapperProtocol) DeliverRequest(req request.Request, originProto protocol.ID) {
	aux := &reqWithOriginProto{
		req:         req,
		originProto: originProto,
	}
	pw.Enqueue(NewEvent(requestEvent, aux), true)
}

// network events

func (pw *WrapperProtocol) MessageDelivered(m message.Message, p peer.Peer) {
	pw.Enqueue(
		NewEvent(
			messageDeliverySucessEvent, messageWithPeer{
				peer:    p,
				message: m,
			},
		), true,
	)
}

func (pw *WrapperProtocol) MessageDeliveryErr(m message.Message, p peer.Peer, err errors.Error) {
	pw.Enqueue(
		NewEvent(
			messageDeliveryFailureEvent, messageWithPeerAndErr{
				peer:    p,
				message: m,
				err:     err,
			},
		), true,
	)
}

func (pw *WrapperProtocol) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	event := &inConnReqEventWithBoolReply{
		dialerProto: dialerProto,
		peer:        p,
		respChan:    make(chan bool, 1),
	}
	go pw.Enqueue(NewEvent(inConnRequestedEvent, event), true)
	reply := <-event.respChan
	return reply
}

func (pw *WrapperProtocol) DialSuccess(dialerProto protocol.ID, p peer.Peer) bool {
	event := &dialSuccessWithBoolReplyChan{
		dialingProto: dialerProto,
		peer:         p,
		respChan:     make(chan bool, 1),
	}
	go pw.Enqueue(NewEvent(dialSuccessEvent, event), true)
	reply := <-event.respChan
	return reply
}

func (pw *WrapperProtocol) DialFailed(p peer.Peer) {
	pw.Enqueue(NewEvent(dialFailedEvent, p), true)
}

func (pw *WrapperProtocol) OutConnDown(p peer.Peer) {
	pw.Enqueue(NewEvent(outConnDownEvent, p), true)
}

func (pw *WrapperProtocol) ID() protocol.ID {
	return pw.wrappedProtocol.ID()
}

func (pw *WrapperProtocol) Start() {
	go pw.handleChannels()
}

func (pw *WrapperProtocol) Init() {
	pw.wrappedProtocol.Init()
}

// channel handler

func (pw *WrapperProtocol) handleEvent(nrEvents *int64, nextEvent *event) {
	atomic.AddInt64(nrEvents, 1)
	switch nextEvent.eventype {
	case inConnRequestedEvent:
		event := nextEvent.eventContent.(*inConnReqEventWithBoolReply)
		ok := pw.wrappedProtocol.InConnRequested(event.dialerProto, event.peer)
		select {
		case event.respChan <- ok:
		default:
		}
	case dialSuccessEvent:
		event := nextEvent.eventContent.(*dialSuccessWithBoolReplyChan)
		ok := pw.wrappedProtocol.DialSuccess(event.dialingProto, event.peer)
		select {
		case event.respChan <- ok:
		default:
		}
	case dialFailedEvent:
		pw.wrappedProtocol.DialFailed(nextEvent.eventContent.(peer.Peer))
	case outConnDownEvent:
		pw.wrappedProtocol.OutConnDown(nextEvent.eventContent.(peer.Peer))
	case messageDeliverySucessEvent:
		event := nextEvent.eventContent.(messageWithPeer)
		pw.wrappedProtocol.MessageDelivered(event.message, event.peer)
	case messageDeliveryFailureEvent:
		event := nextEvent.eventContent.(messageWithPeerAndErr)
		pw.wrappedProtocol.MessageDeliveryErr(event.message, event.peer, event.err)
	case requestEvent:
		switch event := nextEvent.eventContent.(type) {
		case *reqWithOriginProto:
			reply := pw.handleRequest(event.req)
			if reply != nil {
				go pw.babel.SendRequestReply(reply, pw.ID(), event.originProto)
			}
		case *reqWithReplyChan:
			select {
			case event.respChan <- pw.handleRequest(event.req):
			default:
			}

		}
	case requestReplyEvent:
		pw.handleReply(nextEvent.eventContent.(request.Reply))
	case timerEvent:
		pw.handleTimer(nextEvent.eventContent.(timer.Timer))
	case messageEvent:
		event := nextEvent.eventContent.(messageWithPeer)
		pw.handleMessage(event.peer, event.message)
	case notificationEvent:
		pw.handleNotification(nextEvent.eventContent.(notification.Notification))
	default:
		pw.Logger().Panic("unregistered id of event")
	}
}

func (pw *WrapperProtocol) handleChannels() {
	var nrEvents int64 = 0

	defer func() {
		if x := recover(); x != nil {
			pw.Logger().Panicf("Panic in run loop: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()

	go func() {
		for range time.NewTicker(5 * time.Second).C {
			pw.Logger().Infof("Nr routines: %d ,Nr events in queue: %d, nrProcessedEvents: %d", runtime.NumGoroutine(),
				len(pw.eventQueue), atomic.LoadInt64(&nrEvents))
		}
	}()
	pw.wrappedProtocol.Start()

	for {
		select {
		case nextEvent := <-pw.networkEventsQueue:
			pw.handleEvent(&nrEvents, nextEvent)
		default:
			select {
			case nextEvent := <-pw.networkEventsQueue:
				pw.handleEvent(&nrEvents, nextEvent)
			case nextEvent := <-pw.eventQueue:
				pw.handleEvent(&nrEvents, nextEvent)
			}
		}
	}
}

// internal handlers

func (pw *WrapperProtocol) handleNotification(n notification.Notification) {
	handler, ok := pw.notificationHandlers[n.ID()]
	if !ok {
		pw.Logger().Panic("notification handler not found")
	}

	handler(n)
}

func (pw *WrapperProtocol) handleTimer(t timer.Timer) {
	handler, ok := pw.timerHandlers[t.ID()]
	if !ok {
		pw.Logger().Panic("timer handler not found")
	}

	handler(t)
}

func (pw *WrapperProtocol) handleReply(reply request.Reply) {
	handler, ok := pw.replyHandlers[reply.ID()]
	if !ok {
		pw.Logger().Panic("reply handler not found")
	}
	handler(reply)
}

func (pw *WrapperProtocol) handleMessage(p peer.Peer, receivedMsg message.Message) {
	handler, ok := pw.messageHandlers[receivedMsg.Type()]
	if !ok {
		pw.Logger().Panic(
			fmt.Sprintf(
				"message handler for message id %d not found in protocol %d",
				receivedMsg.Type(),
				pw.id,
			),
		)
	}
	handler(p, receivedMsg)
}

func (pw *WrapperProtocol) handleRequest(r request.Request) request.Reply {
	handler, ok := pw.requestHandlers[r.ID()]
	if !ok {
		pw.Logger().Panic(
			errors.FatalError(
				404, "request handler not found",
				strconv.Itoa(int(pw.wrappedProtocol.ID())),
			),
		)
	}
	return handler(r)
}

// Register handlers

// messages
// notifications
// requests
// replies
// timer

func (pw *WrapperProtocol) RegisterMessageHandler(messageID message.ID, handler handlers.MessageHandler) errors.Error {
	_, exists := pw.messageHandlers[messageID]
	if exists {
		return errors.FatalError(
			409,
			fmt.Sprintf("Message handler with MsgID: %d already exists", messageID),
			strconv.Itoa(int(pw.wrappedProtocol.ID())),
		)
	}
	pw.messageHandlers[messageID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterNotificationHandler(
	notificationID notification.ID,
	handler handlers.NotificationHandler,
) errors.Error {
	_, exists := pw.notificationHandlers[notificationID]
	if exists {
		return errors.FatalError(
			409,
			fmt.Sprintf("Notification handler with notificationID: %d already exists", notificationID),
			strconv.Itoa(int(pw.wrappedProtocol.ID())),
		)
	}
	pw.notificationHandlers[notificationID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterRequestReplyHandler(replyID request.ID, handler handlers.ReplyHandler) errors.Error {
	_, exists := pw.replyHandlers[replyID]
	if exists {
		return errors.FatalError(
			409,
			fmt.Sprintf("Request handler with replyID: %d already exists", replyID),
			strconv.Itoa(int(pw.wrappedProtocol.ID())),
		)
	}
	pw.replyHandlers[replyID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterRequestHandler(requestID request.ID, handler handlers.RequestHandler) errors.Error {
	_, exists := pw.requestHandlers[requestID]
	if exists {
		return errors.FatalError(
			409,
			fmt.Sprintf("Request handler with MsgID: %d already exists", requestID),
			strconv.Itoa(int(pw.wrappedProtocol.ID())),
		)
	}
	pw.requestHandlers[requestID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterTimerHandler(timerID timer.ID, handler handlers.TimerHandler) errors.Error {
	_, exists := pw.timerHandlers[timerID]
	if exists {
		return errors.FatalError(
			409,
			fmt.Sprintf("Request handler with timerID: %d already exists", timerID),
			strconv.Itoa(int(pw.wrappedProtocol.ID())),
		)
	}
	pw.timerHandlers[timerID] = handler
	return nil
}

func (pw *WrapperProtocol) Logger() *log.Logger {
	return pw.wrappedProtocol.Logger()
}

func (pw *WrapperProtocol) Name() string {
	return pw.wrappedProtocol.Name()
}

func NewEvent(et eventType, content interface{}) *event {
	return &event{
		eventype:     et,
		eventContent: content,
	}
}
