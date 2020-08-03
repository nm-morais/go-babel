package protocol

import (
	"fmt"
	"github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/handlers"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/notification"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/protocol"
	"github.com/DeMMon/go-babel/pkg/request"
	"github.com/DeMMon/go-babel/pkg/timer"
	"github.com/DeMMon/go-babel/pkg/utils"
)

type WrapperProtocol struct {
	id              protocol.ID
	wrappedProtocol protocol.Protocol
	manager         pkg.ProtocolManager

	// handlers
	notificationHandlers map[notification.ID]handlers.NotificationHandler
	requestHandlers      map[request.ID]handlers.RequestHandler
	messageHandlers      map[message.ID]handlers.MessageHandler
	replyHandlers        map[request.ID]handlers.ReplyHandler
	timerHandlers        map[timer.ID]handlers.TimerHandler

	// channels (internal only)
	messageChan      chan message.Message
	requestChan      chan reqWithReplyCHan
	timerChan        chan timer.Timer
	replyChan        chan request.Reply
	notificationChan chan notification.Notification

	dialSuccess     chan dialSuccessWithBoolReplyChan
	inConnRequested chan inConnReqEventWithBoolReply
	connEstablished chan peer.Peer
	dialFailed      chan peer.Peer
	connDown        chan peer.Peer
}

const ChannelSize = 10 // buffer 10 events in channel
type reqWithReplyCHan struct {
	req      request.Request
	respChan chan request.Reply
}

type dialSuccessWithBoolReplyChan struct {
	dialingProto protocol.ID
	peer         peer.Peer
	respChan     chan bool
}

type inConnReqEventWithBoolReply struct {
	peer     peer.Peer
	respChan chan bool
}

func NewWrapperProtocol(protocol protocol.Protocol, manager pkg.ProtocolManager) *WrapperProtocol {
	return &WrapperProtocol{
		id:              protocol.ID(),
		wrappedProtocol: protocol,
		manager:         manager,

		notificationHandlers: make(map[notification.ID]handlers.NotificationHandler),
		requestHandlers:      make(map[request.ID]handlers.RequestHandler),
		messageHandlers:      make(map[message.ID]handlers.MessageHandler),
		replyHandlers:        make(map[request.ID]handlers.ReplyHandler),
		timerHandlers:        make(map[timer.ID]handlers.TimerHandler),

		// applicational event channels
		messageChan:      make(chan message.Message, ChannelSize),
		requestChan:      make(chan reqWithReplyCHan, ChannelSize),
		replyChan:        make(chan request.Reply, ChannelSize),
		timerChan:        make(chan timer.Timer, ChannelSize),
		notificationChan: make(chan notification.Notification, ChannelSize),

		// transport event channels
		dialFailed:      make(chan peer.Peer, ChannelSize),
		connDown:        make(chan peer.Peer, ChannelSize),
		connEstablished: make(chan peer.Peer, ChannelSize),
		dialSuccess:     make(chan dialSuccessWithBoolReplyChan), // interactive channel
		inConnRequested: make(chan inConnReqEventWithBoolReply),  // interactive channel
	}
}

//  channel Deliverers

func (pw *WrapperProtocol) DeliverRequestReply(reply request.Reply) {
	pw.replyChan <- reply
}

func (pw *WrapperProtocol) DeliverNotification(notification notification.Notification) {
	pw.notificationChan <- notification
}

func (pw *WrapperProtocol) DeliverMessage(message message.Message) {
	pw.messageChan <- message
}

func (pw *WrapperProtocol) DeliverTimer(timer timer.Timer) {
	pw.timerChan <- timer
}

func (pw *WrapperProtocol) DeliverRequest(requestingProto protocol.ID, req request.Request) {
	aux := reqWithReplyCHan{
		req:      req,
		respChan: make(chan request.Reply),
	}
	pw.requestChan <- aux
	defer func() {
		reply := <-aux.respChan
		pw.manager.SendRequestReply(reply, pw.id, requestingProto)
	}()
}

// channel handler

func (pw *WrapperProtocol) handleChannels() {

	for {
		select {

		// net events
		case peerUp := <-pw.connEstablished:
			pw.wrappedProtocol.InConnEstablished(peerUp)
		case event := <-pw.inConnRequested:
			event.respChan <- pw.wrappedProtocol.InConnRequested(event.peer)
		case event := <-pw.dialSuccess:
			event.respChan <- pw.wrappedProtocol.DialSuccess(event.dialingProto, event.peer)
		case peerDialed := <-pw.dialFailed:
			pw.wrappedProtocol.DialFailed(peerDialed)
		case failedPeer := <-pw.connDown:
			pw.wrappedProtocol.PeerDown(failedPeer)

		// applicational events
		case req := <-pw.requestChan:
			req.respChan <- pw.handleRequest(req.req)
		case reply := <-pw.replyChan:
			pw.handleReply(reply)
		case t := <-pw.timerChan:
			pw.handleTimer(t)
		case m := <-pw.messageChan:
			pw.handleMessage(m)
		case n := <-pw.notificationChan:
			pw.handleNotification(n)
		}
	}

}

// internal handlers

func (pw *WrapperProtocol) handleNotification(notification notification.Notification) {
	handler, ok := pw.notificationHandlers[notification.ID()]
	if !ok {
		panic(utils.FatalError(404, "reply handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(notification)
}

func (pw *WrapperProtocol) handleTimer(timer timer.Timer) {
	handler, ok := pw.timerHandlers[timer.ID()]
	if !ok {
		panic(utils.FatalError(404, "reply handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(timer)
}

func (pw *WrapperProtocol) handleReply(reply request.Reply) {
	handler, ok := pw.requestHandlers[reply.ID()]
	if !ok {
		panic(utils.FatalError(404, "reply handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(reply)
}

func (pw *WrapperProtocol) handleMessage(receivedMsg message.Message) {
	handler, ok := pw.messageHandlers[receivedMsg.Type()]
	if !ok {
		panic(utils.FatalError(404, "receivedMsg handler not found", string(pw.wrappedProtocol.ID())))
	}
	handler(receivedMsg)
}

func (pw *WrapperProtocol) handleRequest(request request.Request) request.Reply {
	handler, ok := pw.requestHandlers[request.ID()]
	if !ok {
		panic(utils.FatalError(404, "request handler not found", string(pw.wrappedProtocol.ID())))
	}
	return handler(request)
}

// Register handlers

// messages
// notifications
// requests
// replies
// timer

func (pw *WrapperProtocol) RegisterMessageHandler(messageID message.ID, handler handlers.MessageHandler) pkg.Error {
	_, exists := pw.messageHandlers[messageID]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Message handler with MsgID: %d already exists", messageID), string(pw.wrappedProtocol.ID()))
	}
	pw.messageHandlers[messageID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterNotificationHandler(notificationID notification.ID, handler handlers.NotificationHandler) pkg.Error {
	_, exists := pw.notificationHandlers[notificationID]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Notification handler with notificationID: %d already exists", notificationID), string(pw.wrappedProtocol.ID()))
	}
	pw.notificationHandlers[notificationID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterRequestReplyHandler(replyID request.ID, handler handlers.ReplyHandler) pkg.Error {
	_, exists := pw.replyHandlers[replyID]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Request handler with replyID: %d already exists", replyID), string(pw.wrappedProtocol.ID()))
	}
	pw.replyHandlers[replyID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterRequestHandler(requestID request.ID, handler handlers.RequestHandler) pkg.Error {
	_, exists := pw.requestHandlers[requestID]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Request handler with MsgID: %d already exists", requestID), string(pw.wrappedProtocol.ID()))
	}
	pw.requestHandlers[requestID] = handler
	return nil
}

func (pw *WrapperProtocol) RegisterTimerHandler(timerID timer.ID, handler handlers.TimerHandler) pkg.Error {
	_, exists := pw.timerHandlers[timerID]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Request handler with timerID: %d already exists", timerID), string(pw.wrappedProtocol.ID()))
	}
	pw.timerHandlers[timerID] = handler
	return nil
}

//

func (pw *WrapperProtocol) ID() protocol.ID {
	return pw.wrappedProtocol.ID()
}

func (pw *WrapperProtocol) Start() {
	pw.wrappedProtocol.Start()
}

func (pw *WrapperProtocol) Init() {
	pw.wrappedProtocol.Init()
}

func (pw *WrapperProtocol) InConnRequested(peer peer.Peer) bool {
	event := inConnReqEventWithBoolReply{
		peer:     peer,
		respChan: make(chan bool),
	}
	pw.inConnRequested <- event
	reply := <-event.respChan
	return reply
}

func (pw *WrapperProtocol) DialSuccess(dialerProto protocol.ID, peer peer.Peer) bool {
	event := dialSuccessWithBoolReplyChan{
		dialingProto: dialerProto,
		peer:         peer,
		respChan:     make(chan bool),
	}
	pw.dialSuccess <- event
	reply := <-event.respChan
	return reply
}

func (pw *WrapperProtocol) InnConnEstablished(peer peer.Peer) {
	pw.connEstablished <- peer
}

func (pw *WrapperProtocol) DialFailed(peer peer.Peer) {
	pw.dialFailed <- peer
}

func (pw *WrapperProtocol) TransportFailure(peer peer.Peer) {
	pw.connDown <- peer
}
