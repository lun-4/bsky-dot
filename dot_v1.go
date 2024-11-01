package main

import (
	"time"
)

type DotV1 struct {
	d float64
}

type DotV1State float64

func NewDotV1() DotV1 {
	return DotV1{}
}

func (d *DotV1) TimePeriod() time.Duration {
	return 1 * time.Minute
}

func (d *DotV1) Forward(sentiments []string) error {
	counters := make(map[string]uint)
	for _, sentiment := range sentiments {
		counters[sentiment]++
	}
	proportions := make(map[string]float64)
	for sentiment, count := range counters {
		proportion := float64(count) / float64(len(sentiments))
		proportions[sentiment] = proportion
	}

	epsilon := 0.005 // a small value to increase/decrease the dot on each time step
	for _, proportion := range proportions {
		// NOTE this is a very special magical number whose tweaking leads to collapses on
		// either side of the dot spectrum (either everyone stays at 0 because no sentiment can breach the threshold,
		// or everyone's a 1 because a sentiment wins at every timestamp)
		if proportion > 0.405 {
			// the network is converging itself towards a common goal, increase dot by epsilon
			d.d = limitDot(d.d + epsilon)
			return nil
		}
	}

	// no convergence, decrease by epsilon
	d.d = limitDot(d.d - epsilon)
	return nil
}

func limitDot(d float64) float64 {
	if d > 1 {
		return 1
	}
	if d < 0 {
		return 0
	}
	return d
}
