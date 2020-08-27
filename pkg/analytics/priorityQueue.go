package analytics

import (
	"container/heap"
)

type Item struct {
	Value    interface{} // The value of the item; arbitrary.
	Key      int
	Priority int64 // The priority of the item in the queue.
	Index    int   // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Priority > pq[j].Priority
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

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Remove(toRemove int) {
	old := *pq
	for i := 0; i < len(old); i++ {
		n := old[i]
		if n.Key == toRemove {
			old[i], old[len(old)-1] = old[len(old)-1], old[i]
			pq = &old
		}
	}
	heap.Init(pq)
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *Item, value interface{}, priority int64) {
	item.Value = value
	item.Priority = priority
	heap.Fix(pq, item.Index)
}
