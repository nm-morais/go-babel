package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const ShuffleTimerID = 2001

type ShuffleTimer struct {
	duration time.Duration
}

func (ShuffleTimer) ID() timer.ID {
	return ShuffleTimerID
}

func (s ShuffleTimer) Duration() time.Duration {
	return s.duration
}
