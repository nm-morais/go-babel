package timedEventQueue

import (
	"container/heap"
	"math"
	"time"

	priorityqueue "github.com/nm-morais/go-babel/pkg/dataStructures/priorityQueue"
	"github.com/sirupsen/logrus"
)

type TimedEventQueue interface {
	Add(item Item, deadline time.Time)
	Remove(string) bool
}

type Item interface {
	ID() string
	OnTrigger() (reAdd bool, nextDeadline *time.Time)
}

type removeItemReq struct {
	key      string
	respChan chan bool
}

type addItemReq struct {
	item     Item
	deadline time.Time
}

type timedEventQueue struct {
	pq                   *priorityqueue.PriorityQueue
	addTimedEventChan    chan addItemReq
	removeTimedEventChan chan removeItemReq
	logger               *logrus.Logger
}

func NewTimedEventQueue(logger *logrus.Logger) TimedEventQueue {
	tq := &timedEventQueue{
		pq:                   &priorityqueue.PriorityQueue{},
		addTimedEventChan:    make(chan addItemReq, 5),
		removeTimedEventChan: make(chan removeItemReq, 5),
		logger:               logger,
	}
	go tq.run()
	return tq

}

func (tq *timedEventQueue) Add(item Item, deadline time.Time) {
	tq.addTimedEventChan <- addItemReq{
		item:     item,
		deadline: deadline,
	}
}

func (tq *timedEventQueue) Remove(itemID string) bool {
	req := removeItemReq{
		key:      itemID,
		respChan: make(chan bool),
	}
	tq.removeTimedEventChan <- req
	return <-req.respChan
}

func (tq *timedEventQueue) run() {

	addNew := func(newItem Item, nextTrigger time.Time) {
		if newItem != nil {
			aux := &priorityqueue.Item{
				Value:    newItem,
				Key:      newItem.ID(),
				Priority: nextTrigger.UnixNano(),
			}
			// tq.logger.Infof("Adding item %s", newItem.ID())
			heap.Push(tq.pq, aux)
			heap.Init(tq.pq)
		}
	}

	reAdd := func(item *priorityqueue.Item) {
		if item != nil {
			heap.Push(tq.pq, item)
			heap.Init(tq.pq)
		}
	}

	for {
		var waitTime time.Duration
		var nextItem *priorityqueue.Item
		var nextItemTimer = time.NewTimer(math.MaxInt64)

		if tq.pq.Len() > 0 {
			nextItem = heap.Pop(tq.pq).(*priorityqueue.Item)
			waitTime = time.Until(time.Unix(0, nextItem.Priority))
			nextItemTimer.Stop()
			nextItemTimer = time.NewTimer(waitTime)
		}

		select {
		case newItem := <-tq.addTimedEventChan:
			addNew(newItem.item, newItem.deadline)
			reAdd(nextItem)
		case toRemove := <-tq.removeTimedEventChan:
			tq.logger.Infof("removing item: %s", toRemove.key)

			if nextItem != nil {
				if toRemove.key == nextItem.Key {
					tq.logger.Infof("Removed item %d successfully", toRemove.key)
					toRemove.respChan <- true
					goto finish
				}
				reAdd(nextItem)
			}

			for idx, item := range *tq.pq {
				if item != nil {
					if toRemove.key == item.Key {
						heap.Remove(tq.pq, idx)
						heap.Init(tq.pq)
						toRemove.respChan <- true
						goto finish
					}
				}
			}

		case req := <-tq.removeTimedEventChan:
			tq.logger.Info("Received cancel item signal...")
			if nextItem != nil {
				if req.key == nextItem.Key {
					tq.logger.Infof("Removed item %d successfully", req.key)
					req.respChan <- true
					goto finish
				}
				reAdd(nextItem)
			}
			// nm.logger.Infof("Canceling timer with ID %d", condId)
			for idx, entry := range *tq.pq {
				if entry.Key == req.key {
					tq.logger.Infof("Removed item %d successfully", req.key)
					heap.Remove(tq.pq, idx)
					heap.Init(tq.pq)
					req.respChan <- true
					goto finish
				}
			}
			req.respChan <- false
			tq.logger.Warnf("Removing item %d failure: not found", req.key)
			// tq.pq.LogEntries(tq.logger)

		case <-nextItemTimer.C:
			item := nextItem.Value.(Item)
			if ok, nextDeadline := item.OnTrigger(); ok {
				nextItem.Priority = nextDeadline.UnixNano()
				reAdd(nextItem)
			}
		}
	finish:
		nextItemTimer.Stop()
	}
}
