package timedEventQueue

import (
	"container/heap"
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
		addTimedEventChan:    make(chan addItemReq, 100),
		removeTimedEventChan: make(chan removeItemReq, 100),
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

		if tq.pq.Len() == 0 {
			select {
			case newItem := <-tq.addTimedEventChan:
				addNew(newItem.item, newItem.deadline)
				// tq.logger.Infof("Added new item %s successfully", newItem.item.ID())
			case req := <-tq.removeTimedEventChan:
				req.respChan <- true
			}
			continue
		}

		nextItem = heap.Pop(tq.pq).(*priorityqueue.Item)
		waitTime = time.Until(time.Unix(0, nextItem.Priority))
		nextItemTimer := time.NewTimer(waitTime)

	outer:
		select {
		case newItem := <-tq.addTimedEventChan:
			// tq.logger.Infof("Added new item %s successfully", newItem.item.ID())
			addNew(newItem.item, newItem.deadline)
			reAdd(nextItem)
		case req := <-tq.removeTimedEventChan:
			// tq.logger.Info("Received cancel item signal...")

			if nextItem != nil {
				if req.key == nextItem.Key {
					// tq.logger.Infof("Removed item %s successfully", req.key)
					req.respChan <- true

					break outer
				}
				reAdd(nextItem)
			}

			for idx, entry := range *tq.pq {
				if entry.Key != req.key {
					continue
				}

				heap.Remove(tq.pq, idx)
				heap.Init(tq.pq)
				// tq.logger.Infof("Removed item %s successfully", req.key)
				req.respChan <- true
				break outer
			}

			req.respChan <- false
			// tq.logger.Warnf("Removing item %s failure: not found", req.key)

		case <-nextItemTimer.C:
			item := nextItem.Value.(Item)
			go func() {
				if ok, nextDeadline := item.OnTrigger(); ok {
					nextItem.Priority = nextDeadline.UnixNano()
					tq.Add(item, *nextDeadline)
				}
			}()
		}
		nextItemTimer.Stop()
	}
}
