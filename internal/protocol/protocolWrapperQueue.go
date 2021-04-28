package frontend

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	// blockingqueue "github.com/nm-morais/go-babel/pkg/dataStructures/blockingQueue"
	// blockingqueue "github.com/nm-morais/go-babel/pkg/dataStructures/blockingQueue"
	"github.com/enriquebris/goconcurrentqueue"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/handlers"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
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

const ChannelSize = 100 // buffer 10 events in channel
const QueueSize = 1024  // buffer 10 events in channel

type WrapperProtocol struct {
	id              protocol.ID
	wrappedProtocol protocol.Protocol

	// handlers
	notificationHandlers map[notification.ID]handlers.NotificationHandler
	requestHandlers      map[request.ID]handlers.RequestHandler
	messageHandlers      map[message.ID]handlers.MessageHandler
	replyHandlers        map[request.ID]handlers.ReplyHandler
	timerHandlers        map[timer.ID]handlers.TimerHandler

	// event queue
	eventQueue *goconcurrentqueue.FIFO
}

type reqWithReplyCHan struct {
	req      request.Request
	respChan chan request.Reply
}

type dialSuccessWithBoolReplyChan struct {
	dialingProto protocol.ID
	peer         peer.Peer
	respChan     chan bool
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

func NewWrapperProtocol(p protocol.Protocol) *WrapperProtocol {
	wp := &WrapperProtocol{
		id:              p.ID(),
		wrappedProtocol: p,

		notificationHandlers: make(map[notification.ID]handlers.NotificationHandler),
		requestHandlers:      make(map[request.ID]handlers.RequestHandler),
		messageHandlers:      make(map[message.ID]handlers.MessageHandler),
		replyHandlers:        make(map[request.ID]handlers.ReplyHandler),
		timerHandlers:        make(map[timer.ID]handlers.TimerHandler),
	}
	newEventQueue := goconcurrentqueue.NewFIFO()
	wp.eventQueue = newEventQueue

	return wp
}

//  channel Deliverers

func (pw *WrapperProtocol) DeliverRequestReply(reply request.Reply) {
	if err := pw.eventQueue.Enqueue(NewEvent(requestReplyEvent, reply)); err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) DeliverNotification(n notification.Notification) {
	if err := pw.eventQueue.Enqueue(NewEvent(notificationEvent, n)); err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) DeliverMessage(sender peer.Peer, msg message.Message) {
	err := pw.eventQueue.Enqueue(
		NewEvent(
			messageEvent, messageWithPeer{
				peer:    sender,
				message: msg,
			},
		),
	)
	if err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) DeliverTimer(t timer.Timer) {
	if err := pw.eventQueue.Enqueue(NewEvent(timerEvent, t)); err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) DeliverRequest(req request.Request) <-chan request.Reply {
	aux := reqWithReplyCHan{
		req:      req,
		respChan: make(chan request.Reply),
	}
	if err := pw.eventQueue.Enqueue(NewEvent(requestEvent, aux)); err != nil {
		pw.Logger().Panic(err)
	}
	return aux.respChan
}

// network events

func (pw *WrapperProtocol) MessageDelivered(m message.Message, p peer.Peer) {
	if err := pw.eventQueue.Enqueue(
		NewEvent(
			messageDeliverySucessEvent, messageWithPeer{
				peer:    p,
				message: m,
			},
		),
	); err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) MessageDeliveryErr(m message.Message, p peer.Peer, err errors.Error) {
	if err := pw.eventQueue.Enqueue(
		NewEvent(
			messageDeliveryFailureEvent, messageWithPeerAndErr{
				peer:    p,
				message: m,
				err:     err,
			},
		),
	); err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	event := inConnReqEventWithBoolReply{
		dialerProto: dialerProto,
		peer:        p,
		respChan:    make(chan bool),
	}
	if err := pw.eventQueue.Enqueue(NewEvent(inConnRequestedEvent, event)); err != nil {
		pw.Logger().Panic(err)
	}

	reply := <-event.respChan
	return reply
}

func (pw *WrapperProtocol) DialSuccess(dialerProto protocol.ID, p peer.Peer) bool {
	event := dialSuccessWithBoolReplyChan{
		dialingProto: dialerProto,
		peer:         p,
		respChan:     make(chan bool),
	}
	if err := pw.eventQueue.Enqueue(NewEvent(dialSuccessEvent, event)); err != nil {
		pw.Logger().Panic(err)
	}

	reply := <-event.respChan
	return reply
}

func (pw *WrapperProtocol) DialFailed(p peer.Peer) {
	if err := pw.eventQueue.Enqueue(NewEvent(dialFailedEvent, p)); err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) OutConnDown(p peer.Peer) {
	if err := pw.eventQueue.Enqueue(NewEvent(outConnDownEvent, p)); err != nil {
		pw.Logger().Panic(err)
	}
}

func (pw *WrapperProtocol) ID() protocol.ID {
	return pw.wrappedProtocol.ID()
}

func (pw *WrapperProtocol) Start() {
	pw.wrappedProtocol.Start()
	go pw.handleChannels()
}

func (pw *WrapperProtocol) Init() {
	pw.wrappedProtocol.Init()
}

// channel handler

func (pw *WrapperProtocol) handleChannels() {
	go func() {
		for range time.NewTicker(5 * time.Second).C {
			pw.Logger().Infof("Nr events in queue: %d", pw.eventQueue.GetLen())
		}
	}()

	for {
		nextEventInterface, err := pw.eventQueue.DequeueOrWaitForNextElement()
		if err != nil {
			pw.Logger().Panic(err)
		}
		nextEvent := nextEventInterface.(*event)

		switch nextEvent.eventype {
		case inConnRequestedEvent:
			event := nextEvent.eventContent.(inConnReqEventWithBoolReply)
			event.respChan <- pw.wrappedProtocol.InConnRequested(event.dialerProto, event.peer)
		case dialSuccessEvent:
			event := nextEvent.eventContent.(dialSuccessWithBoolReplyChan)
			event.respChan <- pw.wrappedProtocol.DialSuccess(event.dialingProto, event.peer)
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
			event := nextEvent.eventContent.(reqWithReplyCHan)
			event.respChan <- pw.handleRequest(event.req)
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
