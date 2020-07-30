package handlers

import (
	. "github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/timer"
)

type TimerHandler interface {
	ID() ID
	HandledObjectID() ID
	Handle(timer timer.Timer) Error
}
