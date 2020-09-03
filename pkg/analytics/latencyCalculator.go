package analytics

import (
	"sync"
	"time"
)

type LatencyCalculator struct {
	mu                    sync.Mutex
	newMeasutementsWeight float32
	oldMeasutementsWeight float32
	nMeasurements         int
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

func NewLatencyCalculatorWithValue(newMeasutementsWeight float32, oldMeasutementsWeight float32, value time.Duration) *LatencyCalculator {
	return &LatencyCalculator{
		mu:                    sync.Mutex{},
		currValue:             value,
		newMeasutementsWeight: newMeasutementsWeight,
		oldMeasutementsWeight: newMeasutementsWeight,
	}
}

func (l *LatencyCalculator) AddMeasurement(measurement time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.currValue == 0 {
		l.nMeasurements++
		l.currValue = measurement
		return
	}
	l.nMeasurements++
	l.currValue = time.Duration(float32(measurement)*l.newMeasutementsWeight + float32(l.currValue)*l.oldMeasutementsWeight)
}

func (l *LatencyCalculator) CurrValue() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.nMeasurements == 0 {
		panic("have no latency measurements")
	}

	return l.currValue
}

func (l *LatencyCalculator) NrMeasurements() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nMeasurements
}
