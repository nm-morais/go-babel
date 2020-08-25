package analytics

import (
	"math"
	"sync"
	"time"
)

// Detector is a failure detector
type Detector struct {
	w          *SlidingWindow
	last       time.Time
	minSamples int
	mu         sync.Mutex
}

// New returns a new failure detector that considers the last windowSize
// samples, and ensures there are at least minSamples in the window before
// returning an answer
func NewDetector(windowSize, minSamples int) *Detector {
	d := &Detector{
		w:          NewSlidingWindow(windowSize),
		minSamples: minSamples,
	}
	return d
}

// Ping registers a heart-beat at time now
func (d *Detector) Ping(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.last.IsZero() {
		d.w.Push(now.Sub(d.last).Seconds())
	}
	d.last = now
}

// Phi calculates the suspicion level at time 'now' that the remote end has failed
func (d *Detector) Phi(now time.Time) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.w.Len() < d.minSamples {
		return 0
	}

	t := now.Sub(d.last).Seconds()
	pLater := 1 - cdf(d.w.Mean(), d.w.Stddev(), t)
	phi := -math.Log10(pLater)

	return phi
}

// cdf is the cumulative distribution function of a normally distributed random
// variable with the given mean and standard deviation
func cdf(mean, stddev, x float64) float64 {
	return 0.5 + 0.5*math.Erf((x-mean)/(stddev*math.Sqrt2))
}
