package main

import (
	"fmt"
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

	fmt.Println("proportions:", proportions)
	sumProportions := 0.0

	epsilon := 0.005 // a small value to increase/decrease the dot on each time step
	for label, proportion := range proportions {
		sumProportions += proportion
		if proportion > 0.405 {
			// the network is converging itself towards a common goal, increase dot by epsilon
			fmt.Println(label)
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
