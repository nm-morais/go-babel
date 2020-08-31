package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const ShuffleTimerID = 2001

type ShuffleTimer struct {
	deadline time.Time
}

func (ShuffleTimer) ID() timer.ID {
	return ShuffleTimerID
}

func (s ShuffleTimer) Deadline() time.Time {
	return s.deadline
}
