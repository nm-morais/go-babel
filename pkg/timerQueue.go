package pkg

import (
	"container/heap"
	"math"
	"math/rand"
	"time"

	priorityqueue "github.com/nm-morais/go-babel/pkg/dataStructures/priorityQueue"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const timerQueueCaller = "timerQueue"

type cancelTimerReq struct {
	key     int
	removed chan int
}

type pqItemValue struct {
	protoID protocol.ID
	timer   timer.Timer
}

type TimerQueue interface {
	AddTimer(timer timer.Timer, protocolId protocol.ID) int
	CancelTimer(int) errors.Error
	Logger() *logrus.Logger
}

type timerQueueImpl struct {
	babel           protocolManager.ProtocolManager
	pq              priorityqueue.PriorityQueue
	addTimerChan    chan *priorityqueue.Item
	cancelTimerChan chan *cancelTimerReq
	logger          *logrus.Logger
}

func NewTimerQueue(protoManager protocolManager.ProtocolManager) TimerQueue {
	tq := &timerQueueImpl{
		babel:           protoManager,
		pq:              make(priorityqueue.PriorityQueue, 0),
		addTimerChan:    make(chan *priorityqueue.Item),
		cancelTimerChan: make(chan *cancelTimerReq),
		logger:          logs.NewLogger(timerQueueCaller),
	}
	go tq.start()
	return tq
}

func (tq *timerQueueImpl) AddTimer(timer timer.Timer, protocolId protocol.ID) int {
	newTimerId := rand.Int()
	pqItem := &priorityqueue.Item{
		Value: &pqItemValue{
			protoID: protocolId,
			timer:   timer,
		},
		Key:      newTimerId,
		Priority: timer.Deadline().UnixNano(),
	}
	// tq.logger.Infof("Adding timer with ID %d", newTimerId)
	tq.addTimerChan <- pqItem
	return newTimerId
}

func (tq *timerQueueImpl) removeItem(timerID int) int {
	// tq.logger.Infof("Canceling timer with ID %d", timerID)
	for idx, entry := range tq.pq {
		if entry.Key == timerID {
			heap.Remove(&tq.pq, idx)
			return entry.Key
		}
	}
	heap.Init(&tq.pq)
	return -1
}

func (tq *timerQueueImpl) CancelTimer(timerID int) errors.Error {
	responseChan := make(chan int)
	defer close(responseChan)
	tq.cancelTimerChan <- &cancelTimerReq{key: timerID, removed: responseChan}
	response := <-responseChan
	if response == -1 {
		return errors.NonFatalError(404, "timer not found", timerQueueCaller)
	}
	return nil
}

func (tq *timerQueueImpl) Logger() *logrus.Logger {
	return tq.logger
}

func (tq *timerQueueImpl) start() {

LOOP:
	for {
		// tq.logger.Infof("timer queue loop")

		var nextItem *priorityqueue.Item
		var waitTime time.Duration
		var currTimer = time.NewTimer(math.MaxInt64)

		if tq.pq.Len() > 0 {
			nextItem = heap.Pop(&tq.pq).(*priorityqueue.Item)
			value := nextItem.Value.(*pqItemValue)
			waitTime = time.Until(value.timer.Deadline())
			currTimer = time.NewTimer(waitTime)
			//tq.logger.Infof("Waiting %s for timer of type %s with id %d", waitTime, reflect.TypeOf(value.timer), nextItem.Key)
		}

		select {
		case req := <-tq.cancelTimerChan:
			// tq.logger.Infof("Received cancel timer signal...")
			if nextItem == nil {
				req.removed <- -1
				continue LOOP
			}

			if req.key == nextItem.Key {
				req.removed <- req.key
				// tq.logger.Infof("Removed timer %d successfully", req.key)
				continue LOOP
			} else {
				heap.Push(&tq.pq, nextItem)
			}
			aux := tq.removeItem(req.key)

			// if aux != -1 {
			// 	tq.logger.Infof("Removed timer %d successfully", req.key)
			// } else {
			// 	tq.logger.Warnf("Removing timer %d failure: not found", req.key)
			// }
			req.removed <- aux
			//tq.pq.LogEntries(tq.logger)
		case newItem := <-tq.addTimerChan:
			// tq.logger.Infof("Received add timer signal...")
			// tq.logger.Infof("Adding timer %d", newItem.Key)
			heap.Push(&tq.pq, newItem)
			if nextItem != nil {
				// tq.logger.Infof("nextItem (%d) was not nil, re-adding to timer list", nextItem.Key)
				heap.Push(&tq.pq, nextItem)
			}
			//tq.pq.LogEntries(tq.logger)
		case <-currTimer.C:
			// tq.logger.Infof("Processing timer %+v", *nextItem)
			value := nextItem.Value.(*pqItemValue)
			tq.babel.DeliverTimer(value.timer, value.protoID)
			//tq.pq.LogEntries(tq.logger)
		}
	}
}
