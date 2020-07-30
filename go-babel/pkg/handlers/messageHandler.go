package handlers

import (
	. "github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/message"
)

type MessageHandler interface {
	ID() ID
	HandledObjectID() ID
	Handle(message message.Message) Error
}
