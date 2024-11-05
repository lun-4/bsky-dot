package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

type DotV4Sentiment struct {
	Timestamp int64  `json:"t"`
	Value     string `json:"v"`
}

type DotV4 struct {
	D               float64         `json:"d"`
	CurrentBaseline CurrentBaseline `json:"b"`
	NextBaseline    NextBaseline    `json:"n"`
}

func NewDotV4(state map[string]any) DotV4 {
	v, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	var givenState DotV4
	err = json.Unmarshal(v, &givenState)
	if err != nil {
		panic(err)
	}
	return givenState
}

func NewEmptyDotV4() DotV4 {
	return DotV4{
		D:               0,
		CurrentBaseline: CurrentBaseline{},
		NextBaseline:    NextBaseline{},
	}
}

func (d *DotV4) Serialize() map[string]any {
	v, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	var serializedState map[string]any
	err = json.Unmarshal(v, &serializedState)
	if err != nil {
		panic(err)
	}
	return serializedState
}

func (d *DotV4) TimePeriod() time.Duration {
	return 1 * time.Minute
}
func (d *DotV4) Version() string {
	return "v3"
}

func (d *DotV4) Value() float64 {
	return d.D
}

func (d *DotV4) Debug() {
	fmt.Println(d)
}

func (d *DotV4) Forward(timestamp time.Time, sentiments []string) error {
	currentBaselineTimestamp := time.Unix(d.CurrentBaseline.Timestamp, 0)

	proportions := sentimentToProportionMap(sentiments)
	var avgs map[string]float64
	if !d.CurrentBaseline.Initialized {
		fmt.Println("CurrentBaseline is not initialized")
		// initialize with defaults for the current date
		d.CurrentBaseline.Averages = make(map[string]float64)
		d.CurrentBaseline.Initialized = true
		d.CurrentBaseline.Timestamp = timestamp.Unix()
		d.CurrentBaseline.Averages["positive"] = 0.45
		d.CurrentBaseline.Averages["negative"] = 0.35
	}

	delta := timestamp.Sub(currentBaselineTimestamp)
	nextStateReady := delta.Minutes() >= 60
	if !nextStateReady {
		// use CurrentBaseline as it is still current to the given date
		avgs = d.CurrentBaseline.Averages

		// add the current proportions to the next baseline
		d.NextBaseline.Forward(proportions)
	} else {
		// compute new baseline from previous baseline (currently called "next"), then reset next
		avgs = d.NextBaseline.Averages()

		// copy avgs over currentBaseline
		d.CurrentBaseline.Averages = avgs
		d.CurrentBaseline.Initialized = true
		d.CurrentBaseline.Timestamp = timestamp.Unix()

		// we reset the baseline, so shift it around
		if d.D > 0.5 {
			d.D -= 0.08
		} else {
			d.D += 0.08
		}

		// reset NextBaseline
		d.NextBaseline.Reset()
	}

	for sentiment, proportion := range proportions {
		avgProportion := avgs[sentiment]
		// if its not neutral AND it's acting above the average for the last day, increase the dot value because of that sentiment
		// proportion has to win against its average
		if sentiment == "positive" || sentiment == "negative" {
			proportionDelta := math.Abs(avgProportion - proportion)
			fmt.Println("pd", proportionDelta)
			scalingFactor := 0.05 * proportionDelta
			if proportion > avgProportion {
				d.D = limitDot(d.D + (0.01 + scalingFactor))
				return nil
			} else {
				d.D = limitDot(d.D - (0.01 + scalingFactor))
				return nil
			}
		}
	}

	// no sentiment won at this tick, so the network is feeling less emotions
	d.D = limitDot(d.D - 0.005)
	return nil
}
