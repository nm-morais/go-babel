package heartbeat

import (
	"github.com/nm-morais/go-babel/pkg/timer"
	"time"
)

const PingTimerID = 1001

type HeartbeatTimer struct {
	timer *time.Timer
}

func (p HeartbeatTimer) ID() timer.ID {
	return PingTimerID
}

func (p HeartbeatTimer) Wait() {
	<-p.timer.C
}
