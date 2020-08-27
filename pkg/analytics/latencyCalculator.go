package analytics

import (
	"sync"
	"time"
)

type LatencyCalculator struct {
	mu                    sync.Mutex
	newMeasutementsWeight float32
	oldMeasutementsWeight float32
	currValue             time.Duration
}

func NewLatencyCalculator(newMeasutementsWeight float32, oldMeasutementsWeight float32) *LatencyCalculator {
	return &LatencyCalculator{
		mu:                    sync.Mutex{},
		currValue:             0,
		newMeasutementsWeight: newMeasutementsWeight,
		oldMeasutementsWeight: newMeasutementsWeight,
	}
}

func (l *LatencyCalculator) AddMeasurement(measurement time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.currValue == 0 {
		l.currValue = measurement
		return
	}

	l.currValue = time.Duration(float32(measurement)*l.newMeasutementsWeight + float32(l.currValue)*l.oldMeasutementsWeight)

}

func (l *LatencyCalculator) GetCurrMeasurement() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.currValue
}
