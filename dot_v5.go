package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

type DotV5 struct {
	D               float64           `json:"d"`
	CurrentBaseline V5CurrentBaseline `json:"b"`
	NextBaseline    V5NextBaseline    `json:"n"`
}

type V5CurrentBaseline struct {
	Timestamp   int64   `json:"t"`
	Initialized bool    `json:"i"`
	Average     float64 `json:"a"`
}

type V5NextBaseline struct {
	Sum   float64 `json:"s"`
	Count float64 `json:"c"`
}

func (nb V5NextBaseline) Average() float64 {
	if nb.Count == 0 {
		return 0.5 // default to 0.5 (hald emotion) in case we're running at a no-data period
	}
	return nb.Sum / nb.Count
}

func (nb *V5NextBaseline) Forward(proportions map[string]float64) {
	for label, v := range proportions {
		if label == "neutral" {
			continue
		}
		nb.Sum += v
	}
	nb.Count += 1
}

func (nb *V5NextBaseline) Reset() {
	nb.Sum = 0
	nb.Count = 0
}

func NewDotV5(state map[string]any) DotV5 {
	v, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	var givenState DotV5
	err = json.Unmarshal(v, &givenState)
	if err != nil {
		panic(err)
	}
	return givenState
}

func NewEmptyDotV5() DotV5 {
	return DotV5{
		D:               0,
		CurrentBaseline: V5CurrentBaseline{},
		NextBaseline:    V5NextBaseline{},
	}
}

func (d *DotV5) Serialize() map[string]any {
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

func (d *DotV5) TimePeriod() time.Duration {
	return 1 * time.Minute
}
func (d *DotV5) Version() string {
	return "v5"
}

func (d *DotV5) Value() float64 {
	return d.D
}

func (d *DotV5) Debug() {
	fmt.Println(d)
}

func (d *DotV5) Forward(timestamp time.Time, sentiments []string) error {
	currentBaselineTimestamp := time.Unix(d.CurrentBaseline.Timestamp, 0)

	proportions := sentimentToProportionMap(sentiments)
	var avg float64
	if !d.CurrentBaseline.Initialized {
		fmt.Println("CurrentBaseline is not initialized")
		// initialize with defaults for the current date
		d.CurrentBaseline.Average = 0.5
		d.CurrentBaseline.Initialized = true
		d.CurrentBaseline.Timestamp = timestamp.Unix()
	}

	delta := timestamp.Sub(currentBaselineTimestamp)
	nextStateReady := delta.Minutes() >= 40
	if !nextStateReady {
		// use CurrentBaseline as it is still current to the given date
		avg = d.CurrentBaseline.Average

		// add the current proportions to the next baseline
		d.NextBaseline.Forward(proportions)
	} else {
		// compute new baseline from previous baseline (currently called "next"), then reset next
		avg = d.NextBaseline.Average()

		// copy avgs over currentBaseline
		d.CurrentBaseline.Average = avg
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

	emotionProportion := 0.0
	for sentiment, proportion := range proportions {
		if sentiment == "neutral" {
			continue
		}
		emotionProportion += proportion
	}

	proportionDelta := math.Abs(avg - emotionProportion)
	scalingFactor := 0.5 * proportionDelta

	if emotionProportion > avg {
		d.D = limitDot(d.D + (scalingFactor))
		return nil
	} else {
		d.D = limitDot(d.D - (scalingFactor))
		return nil
	}
}
