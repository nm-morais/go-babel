package protocol

import (
	"fmt"
	. "github.com/DeMMon/go-babel/pkg"
	. "github.com/DeMMon/go-babel/pkg/handlers"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/request"
	"github.com/DeMMon/go-babel/pkg/timer"
	"github.com/DeMMon/go-babel/pkg/utils"
)

const caller = "ProtocolManager"

type Protocol interface {
	ID() ID
	Start()

	InConnRequested(peer peer.Peer) Error
	OutConnEstablished(peer peer.Peer) Error
	NodeDown(peer peer.Peer)

	handleTimer(timer timer.Timer) Error
	handleRequest(request request.Request) Error
	handleMessage(message message.Message) Error
}

type ProtocolWrapper struct {
	id              ID
	protocol        Protocol
	requestHandlers map[ID]RequestHandler
	messageHandlers map[ID]MessageHandler
	timerHandlers   map[ID]TimerHandler
}

func NewProtocolWrapper(protocol Protocol) Protocol {
	return &ProtocolWrapper{
		id:              protocol.ID(),
		protocol:        protocol,
		requestHandlers: make(map[ID]RequestHandler),
		messageHandlers: make(map[ID]MessageHandler),
		timerHandlers:   make(map[ID]TimerHandler),
	}
}

func (pm *ProtocolWrapper) handleTimer(timer timer.Timer) Error {
	handler, ok := pm.timerHandlers[timer.ID()]
	if !ok {
		return utils.FatalError(404, "timer handler not found", pm.protocol.ID())
	}
	return handler.Handle(timer)
}

func (pm *ProtocolWrapper) handleRequest(request request.Request) Error {
	handler, ok := pm.requestHandlers[request.ID()]
	if !ok {
		return utils.FatalError(404, "request handler not found", pm.protocol.ID())
	}
	return handler.Handle(request)
}

func (pm *ProtocolWrapper) handleMessage(message message.Message) Error {
	handler, ok := pm.messageHandlers[message.ID()]
	if !ok {
		return utils.FatalError(404, "message handler not found", pm.protocol.ID())
	}
	return handler.Handle(message)
}

func (pm *ProtocolWrapper) ID() ID {
	return pm.ID()
}

func (pm *ProtocolWrapper) RegisterMessageHandler(message message.Message, handler MessageHandler) Error {
	_, exists := pm.messageHandlers[message.ID()]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Message handler with ID: %s already exists", handler.ID()), caller)
	}
	pm.messageHandlers[message.ID()] = handler
	return nil
}

func (pm *ProtocolWrapper) RegisterTimerHandler(timer timer.Timer, handler TimerHandler) Error {
	_, exists := pm.timerHandlers[timer.ID()]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Timer handler with ID: %s already exists", timer.ID()), caller)
	}
	pm.timerHandlers[timer.ID()] = handler
	return nil
}

func (pm *ProtocolWrapper) RegisterRequestHandler(request request.Request, handler RequestHandler) Error {
	_, exists := pm.requestHandlers[request.ID()]
	if exists {
		return utils.FatalError(409, fmt.Sprintf("Request handler with ID: %s already exists", request.ID()), caller)
	}
	pm.requestHandlers[request.ID()] = handler
	return nil
}

func (pm *ProtocolWrapper) Start() {
	pm.Start()
}

func (pm *ProtocolWrapper) InConnRequested(peer peer.Peer) Error {
	return pm.InConnRequested(peer)
}

func (pm *ProtocolWrapper) OutConnEstablished(peer peer.Peer) Error {
	return pm.OutConnEstablished(peer)
}

func (pm *ProtocolWrapper) NodeDown(peer peer.Peer) {
	pm.NodeDown(peer)
}
