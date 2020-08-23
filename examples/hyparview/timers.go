package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/timer"
)

const ShuffleTimerID = 2001

type ShuffleTimer struct {
	timer *time.Timer
}

func (ShuffleTimer) ID() timer.ID {
	return ShuffleTimerID
}

func (s ShuffleTimer) Wait() {
	<-s.timer.C
}
