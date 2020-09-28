package protocol

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/nm-morais/go-babel/pkg/dataStructures"
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
	eventQueue *dataStructures.BlockingQueue
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
	peer     peer.Peer
	respChan chan bool
}

func NewWrapperProtocol(protocol protocol.Protocol) WrapperProtocol {
	wp := WrapperProtocol{
		id:              protocol.ID(),
		wrappedProtocol: protocol,

		notificationHandlers: make(map[notification.ID]handlers.NotificationHandler),
		requestHandlers:      make(map[request.ID]handlers.RequestHandler),
		messageHandlers:      make(map[message.ID]handlers.MessageHandler),
		replyHandlers:        make(map[request.ID]handlers.ReplyHandler),
		timerHandlers:        make(map[timer.ID]handlers.TimerHandler),
	}
	newEventQueue, err := dataStructures.NewArrayBlockingQueue(QueueSize)
	if err != nil {
		panic(err)
	}

	wp.eventQueue = newEventQueue

	return wp
}

//  channel Deliverers

func (pw WrapperProtocol) DeliverRequestReply(reply request.Reply) {
	if _, err := pw.eventQueue.Put(NewEvent(requestReplyEvent, reply)); err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) DeliverNotification(notification notification.Notification) {
	if _, err := pw.eventQueue.Put(NewEvent(notificationEvent, notification)); err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) DeliverMessage(sender peer.Peer, msg message.Message) {
	_, err := pw.eventQueue.Put(NewEvent(messageEvent, messageWithPeer{
		peer:    sender,
		message: msg,
	}))
	if err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) DeliverTimer(timer timer.Timer) {
	if _, err := pw.eventQueue.Put(NewEvent(timerEvent, timer)); err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) DeliverRequest(req request.Request) <-chan request.Reply {
	aux := reqWithReplyCHan{
		req:      req,
		respChan: make(chan request.Reply),
	}
	if _, err := pw.eventQueue.Put(NewEvent(requestEvent, aux)); err != nil {
		panic(err)
	}
	return aux.respChan
}

// network events

func (pw WrapperProtocol) MessageDelivered(message message.Message, peer peer.Peer) {
	if _, err := pw.eventQueue.Put(NewEvent(messageDeliverySucessEvent, messageWithPeer{
		peer:    peer,
		message: message,
	})); err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {

	if _, err := pw.eventQueue.Put(NewEvent(messageDeliveryFailureEvent, messageWithPeerAndErr{
		peer:    peer,
		message: message,
		err:     error,
	})); err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) InConnRequested(peer peer.Peer) bool {
	event := inConnReqEventWithBoolReply{
		peer:     peer,
		respChan: make(chan bool),
	}
	if _, err := pw.eventQueue.Put(NewEvent(inConnRequestedEvent, event)); err != nil {
		panic(err)
	}

	reply := <-event.respChan
	return reply
}

func (pw WrapperProtocol) DialSuccess(dialerProto protocol.ID, peer peer.Peer) bool {
	event := dialSuccessWithBoolReplyChan{
		dialingProto: dialerProto,
		peer:         peer,
		respChan:     make(chan bool),
	}
	if _, err := pw.eventQueue.Put(NewEvent(dialSuccessEvent, event)); err != nil {
		panic(err)
	}

	reply := <-event.respChan
	return reply
}

func (pw WrapperProtocol) DialFailed(peer peer.Peer) {
	if _, err := pw.eventQueue.Put(NewEvent(dialFailedEvent, peer)); err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) OutConnDown(peer peer.Peer) {
	if _, err := pw.eventQueue.Put(NewEvent(outConnDownEvent, peer)); err != nil {
		panic(err)
	}
}

func (pw WrapperProtocol) ID() protocol.ID {
	return pw.wrappedProtocol.ID()
}

func (pw WrapperProtocol) Start() {
	pw.wrappedProtocol.Start()
	go pw.handleChannels()
}

func (pw WrapperProtocol) Init() {
	pw.wrappedProtocol.Init()
}

// channel handler

func (pw WrapperProtocol) handleChannels() {
	for {
		nextEventInterface, err := pw.eventQueue.Get()
		if err != nil {
			panic(err)
		}
		nextEvent := nextEventInterface.(*event)
		switch nextEvent.eventype {
		case inConnRequestedEvent:
			event := nextEvent.eventContent.(inConnReqEventWithBoolReply)
			event.respChan <- pw.wrappedProtocol.InConnRequested(event.peer)
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
			panic("unregistered id of event")
		}
	}
}

// internal handlers

func (pw WrapperProtocol) handleNotification(notification notification.Notification) {
	handler, ok := pw.notificationHandlers[notification.ID()]
	if !ok {
		panic(errors.FatalError(404, "reply handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(notification)
}

func (pw WrapperProtocol) handleTimer(timer timer.Timer) {
	handler, ok := pw.timerHandlers[timer.ID()]
	if !ok {
		panic(errors.FatalError(404, "reply handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(timer)
}

func (pw WrapperProtocol) handleReply(reply request.Reply) {
	handler, ok := pw.requestHandlers[reply.ID()]
	if !ok {
		panic(errors.FatalError(404, "reply handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(reply)
}

func (pw WrapperProtocol) handleMessage(peer peer.Peer, receivedMsg message.Message) {
	handler, ok := pw.messageHandlers[receivedMsg.Type()]
	if !ok {
		panic(errors.FatalError(404, "receivedMsg handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(peer, receivedMsg)
}

func (pw WrapperProtocol) handleRequest(request request.Request) request.Reply {
	handler, ok := pw.requestHandlers[request.ID()]
	if !ok {
		panic(errors.FatalError(404, "request handler not found", string(pw.wrappedProtocol.ID())))
	}
	return handler(request)
}

// Register handlers

// messages
// notifications
// requests
// replies
// timer

func (pw WrapperProtocol) RegisterMessageHandler(messageID message.ID, handler handlers.MessageHandler) errors.Error {
	_, exists := pw.messageHandlers[messageID]
	if exists {
		return errors.FatalError(409, fmt.Sprintf("Message handler with MsgID: %d already exists", messageID), string(pw.wrappedProtocol.ID()))
	}
	pw.messageHandlers[messageID] = handler
	return nil
}

func (pw WrapperProtocol) RegisterNotificationHandler(notificationID notification.ID, handler handlers.NotificationHandler) errors.Error {
	_, exists := pw.notificationHandlers[notificationID]
	if exists {
		return errors.FatalError(409, fmt.Sprintf("Notification handler with notificationID: %d already exists", notificationID), string(pw.wrappedProtocol.ID()))
	}
	pw.notificationHandlers[notificationID] = handler
	return nil
}

func (pw WrapperProtocol) RegisterRequestReplyHandler(replyID request.ID, handler handlers.ReplyHandler) errors.Error {
	_, exists := pw.replyHandlers[replyID]
	if exists {
		return errors.FatalError(409, fmt.Sprintf("Request handler with replyID: %d already exists", replyID), string(pw.wrappedProtocol.ID()))
	}
	pw.replyHandlers[replyID] = handler
	return nil
}

func (pw WrapperProtocol) RegisterRequestHandler(requestID request.ID, handler handlers.RequestHandler) errors.Error {
	_, exists := pw.requestHandlers[requestID]
	if exists {
		return errors.FatalError(409, fmt.Sprintf("Request handler with MsgID: %d already exists", requestID), string(pw.wrappedProtocol.ID()))
	}
	pw.requestHandlers[requestID] = handler
	return nil
}

func (pw WrapperProtocol) RegisterTimerHandler(timerID timer.ID, handler handlers.TimerHandler) errors.Error {
	_, exists := pw.timerHandlers[timerID]
	if exists {
		return errors.FatalError(409, fmt.Sprintf("Request handler with timerID: %d already exists", timerID), string(pw.wrappedProtocol.ID()))
	}
	pw.timerHandlers[timerID] = handler
	return nil
}

func (pw WrapperProtocol) Logger() *log.Logger {
	return pw.wrappedProtocol.Logger()
}

func (pw WrapperProtocol) Name() string {
	return pw.wrappedProtocol.Name()
}

func NewEvent(et eventType, content interface{}) *event {
	return &event{
		eventype:     et,
		eventContent: content,
	}

}
