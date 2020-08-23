package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
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
