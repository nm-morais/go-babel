package analytics

import "math"

type SlidingWindow struct {
	data []float64
	head int

	length int
	sum    float64
}

func NewSlidingWindow(capacity int) *SlidingWindow {
	return &SlidingWindow{
		data: make([]float64, capacity),
	}
}

func (w *SlidingWindow) Push(n float64) float64 {
	old := w.data[w.head]

	w.length++

	w.data[w.head] = n
	w.head++
	if w.head >= len(w.data) {
		w.head = 0
	}

	w.sum -= old
	w.sum += n

	return old
}

func (w *SlidingWindow) Len() int {
	if w.length < len(w.data) {
		return w.length
	}

	return len(w.data)
}

func (w *SlidingWindow) Mean() float64 {
	return w.sum / float64(w.Len())
}

func (w *SlidingWindow) Var() float64 {
	n := float64(w.Len())
	mean := w.Mean()
	l := w.Len()

	sum1 := 0.0
	sum2 := 0.0

	for i := 0; i < l; i++ {
		xm := w.data[i] - mean
		sum1 += xm * xm
		sum2 += xm
	}

	return (sum1 - (sum2*sum2)/n) / (n - 1)
}

func (w *SlidingWindow) Stddev() float64 {
	return math.Sqrt(w.Var())
}

// Set sets the current windowed data.  The length is reset, and Push() will start overwriting the first element of the array.
func (w *SlidingWindow) Set(data []float64) {
	if w.data != nil {
		w.data = w.data[:0]
	}

	w.data = append(w.data, data...)

	w.sum = 0
	for _, v := range w.data {
		w.sum += v
	}

	w.head = 0
	w.length = len(data)
}
