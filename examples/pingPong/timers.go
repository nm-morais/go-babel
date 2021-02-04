package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const PingTimerID = 1001

type PingTimer struct {
	duration time.Duration
}

func (p PingTimer) ID() timer.ID {
	return PingTimerID
}

func (p PingTimer) Duration() time.Duration {
	return p.duration
}
