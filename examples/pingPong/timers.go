package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const PingTimerID = 1001

type PingTimer struct {
	deadline time.Time
}

func (p PingTimer) ID() timer.ID {
	return PingTimerID
}

func (p PingTimer) Deadline() time.Time {
	return p.deadline
}
