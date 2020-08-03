package handlers

import (
	"github.com/nm-morais/DeMMon/go-babel/pkg/message"
	"github.com/nm-morais/DeMMon/go-babel/pkg/notification"
	"github.com/nm-morais/DeMMon/go-babel/pkg/peer"
	"github.com/nm-morais/DeMMon/go-babel/pkg/request"
	"github.com/nm-morais/DeMMon/go-babel/pkg/timer"
)

type RequestHandler func(request request.Request) request.Reply
type MessageHandler func(sender peer.Peer, message message.Message)
type ReplyHandler func(reply request.Reply)
type NotificationHandler func(notification notification.Notification)
type TimerHandler func(timer timer.Timer)
