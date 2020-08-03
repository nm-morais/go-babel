package handlers

import (
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/notification"
	"github.com/DeMMon/go-babel/pkg/request"
	"github.com/DeMMon/go-babel/pkg/timer"
)

type MessageHandler func(message message.Message)
type RequestHandler func(request request.Request) request.Reply
type ReplyHandler func(reply request.Reply)
type NotificationHandler func(notification notification.Notification)
type TimerHandler func(timer timer.Timer)
