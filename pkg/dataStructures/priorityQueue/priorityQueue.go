package priorityqueue

import (
	"container/heap"
	"time"

	"github.com/sirupsen/logrus"
)

type Item struct {
	Value    interface{} // The value of the item; arbitrary.
	Key      string
	Priority int64 // The priority of the item in the queue.
	Index    int   // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Peek() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	return item
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *Item, value interface{}, priority int64) {
	item.Value = value
	item.Priority = priority
	heap.Fix(pq, item.Index)
}

func (pq *PriorityQueue) LogEntries(logger *logrus.Logger) {
	for _, item := range *pq {
		logger.Infof(
			"Item %d, key: %d: , prio: %d , time until: %s",
			item.Index,
			item.Key,
			item.Priority,
			time.Until(time.Unix(0, item.Priority)),
		)
	}
}
