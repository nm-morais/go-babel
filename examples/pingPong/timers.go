package pingPong

import (
	"github.com/nm-morais/DeMMon/go-babel/pkg/timer"
	"time"
)

const PingTimerID = 1001

type PingTimer struct {
	timer *time.Timer
}

func (p PingTimer) ID() timer.ID {
	return PingTimerID
}

func (p PingTimer) Wait() {
	<-p.timer.C
}
