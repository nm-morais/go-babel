package analytics

import (
	"sync"
	"time"
)

type LatencyCalculator struct {
	mu                    sync.Mutex
	newMeasurementsWeight float32
	oldMeasurementsWeight float32
	currValue             time.Duration
}

func NewLatencyCalculator(newMeasurementsWeight float32, oldMeasurementsWeight float32) *LatencyCalculator {
	return &LatencyCalculator{
		mu:                    sync.Mutex{},
		currValue:             0,
		newMeasurementsWeight: newMeasurementsWeight,
		oldMeasurementsWeight: oldMeasurementsWeight,
	}
}

func NewLatencyCalculatorWithValue(
	newMeasurementsWeight float32,
	oldMeasurementsWeight float32,
	value time.Duration,
) *LatencyCalculator {
	return &LatencyCalculator{
		mu:                    sync.Mutex{},
		currValue:             value,
		newMeasurementsWeight: newMeasurementsWeight,
		oldMeasurementsWeight: oldMeasurementsWeight,
	}
}

func (l *LatencyCalculator) AddMeasurement(measurement time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.currValue == 0 {
		l.currValue = measurement
		return
	}
	l.currValue = time.Duration(float32(measurement)*l.newMeasurementsWeight + float32(l.currValue)*l.oldMeasurementsWeight)
}

func (l *LatencyCalculator) CurrValue() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.currValue
}
