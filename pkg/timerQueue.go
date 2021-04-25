package pkg

import (
	"fmt"
	"math/rand"
	"time"

	timerQueue "github.com/nm-morais/go-babel/pkg/dataStructures/timedEventQueue"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const timerQueueCaller = "timerQueue"

type TimerQueue interface {
	AddPeriodicTimer(timer timer.Timer, protocolID protocol.ID) int
	AddTimer(timer timer.Timer, protocolID protocol.ID) int
	CancelTimer(int) errors.Error
	Logger() *logrus.Logger
}

type timerQueueImpl struct {
	babel  protocolManager.ProtocolManager
	teq    timerQueue.TimedEventQueue
	logger *logrus.Logger
}

func NewTimerQueue(protoManager protocolManager.ProtocolManager) TimerQueue {
	logger := logs.NewLogger(timerQueueCaller)
	tq := &timerQueueImpl{
		babel:  protoManager,
		logger: logger,
		teq:    timerQueue.NewTimedEventQueue(logger),
	}
	return tq
}

type timerWrapper struct {
	periodic bool
	id       int
	protoID  protocol.ID
	timer    timer.Timer
	babel    protocolManager.ProtocolManager
}

func (tw *timerWrapper) Periodicity() time.Duration {
	return tw.timer.Duration()
}

func (tw *timerWrapper) ID() string {
	return fmt.Sprintf("%d", tw.id)
}

func (tw *timerWrapper) OnTrigger() (bool, *time.Time) {
	tw.babel.DeliverTimer(tw.timer, tw.protoID)
	if !tw.periodic {
		return false, nil
	}
	nextTrigger := time.Now().Add(tw.Periodicity())
	return true, &nextTrigger
}

func (tq *timerQueueImpl) AddPeriodicTimer(timer timer.Timer, protocolId protocol.ID) int {
	newTimerID := rand.Int()
	timerWrapper := &timerWrapper{
		periodic: true,
		id:       newTimerID,
		protoID:  protocolId,
		timer:    timer,
		babel:    tq.babel,
	}
	nextTrigger := time.Now().Add(timerWrapper.Periodicity())
	go tq.teq.Add(timerWrapper, nextTrigger)
	return newTimerID
}

func (tq *timerQueueImpl) AddTimer(timer timer.Timer, protocolId protocol.ID) int {
	newTimerID := rand.Int()

	timerWrapper := &timerWrapper{
		periodic: false,
		id:       newTimerID,
		protoID:  protocolId,
		timer:    timer,
		babel:    tq.babel,
	}
	nextTrigger := time.Now().Add(timerWrapper.Periodicity())
	go tq.teq.Add(timerWrapper, nextTrigger)
	return newTimerID
}

func (tq *timerQueueImpl) CancelTimer(timerID int) errors.Error {
	found := tq.teq.Remove(fmt.Sprintf("%d", timerID))
	if !found {
		return errors.NonFatalError(404, "timer not found", timerQueueCaller)
	}
	return nil
}

func (tq *timerQueueImpl) Logger() *logrus.Logger {
	return tq.logger
}
