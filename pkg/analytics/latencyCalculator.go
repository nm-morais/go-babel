package analytics

import (
	"sync"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
)

const latencyCalculatorCaller = "latencyCalculator"

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

func (l *LatencyCalculator) AddMeasurement(measurement time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.currValue == 0 {
		l.currValue = measurement
		return
	}
	l.nMeasurements++
	l.currValue = time.Duration(float32(measurement)*l.newMeasutementsWeight + float32(l.currValue)*l.oldMeasutementsWeight)
}

func (l *LatencyCalculator) CurrValue() (time.Duration, errors.Error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.nMeasurements == 0 {
		return 0, errors.NonFatalError(404, "have no latency measurements", latencyCalculatorCaller)
	}

	return l.currValue, nil
}

func (l *LatencyCalculator) NrMeasurements() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nMeasurements
}
