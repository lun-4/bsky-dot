package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type DotV3Sentiment struct {
	Timestamp int64  `json:"t"`
	Value     string `json:"v"`
}

type CurrentBaseline struct {
	Timestamp   int64              `json:"t"`
	Initialized bool               `json:"i"`
	Averages    map[string]float64 `json:"a"`
}

type NextBaseline struct {
	Sums   map[string]float64 `json:"s"`
	Counts map[string]float64 `json:"c"`
}

func (nb NextBaseline) Averages() map[string]float64 {
	result := make(map[string]float64)
	for k, v := range nb.Sums {
		result[k] = v / nb.Counts[k]
	}
	return result
}

func (nb *NextBaseline) Forward(proportions map[string]float64) {
	if nb.Sums == nil {
		nb.Sums = make(map[string]float64)
	}
	if nb.Counts == nil {
		nb.Counts = make(map[string]float64)
	}
	for k, v := range proportions {
		nb.Sums[k] += v
		nb.Counts[k] += 1
	}
}

func (nb *NextBaseline) Reset() {
	for k := range nb.Sums {
		nb.Sums[k] = 0
		nb.Counts[k] = 0
	}
}

type DotV3 struct {
	D               float64         `json:"d"`
	CurrentBaseline CurrentBaseline `json:"b"`
	NextBaseline    NextBaseline    `json:"n"`
}

func NewDotV3(state map[string]any) DotV3 {
	v, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	var givenState DotV3
	err = json.Unmarshal(v, &givenState)
	if err != nil {
		panic(err)
	}
	return givenState
}

func NewEmptyDotV3() DotV3 {
	return DotV3{
		D:               0,
		CurrentBaseline: CurrentBaseline{},
		NextBaseline:    NextBaseline{},
	}
}

func (d *DotV3) Serialize() map[string]any {
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

func (d *DotV3) TimePeriod() time.Duration {
	return 1 * time.Minute
}
func (d *DotV3) Version() string {
	return "v3"
}

func (d *DotV3) Value() float64 {
	return d.D
}

func (d *DotV3) Debug() {
	fmt.Println(d)
}

func (d *DotV3) Forward(timestamp time.Time, sentiments []string) error {
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
		fmt.Println("CurrentBaseline is equal to new date, and its initialized")
		// use CurrentBaseline as it is still current to the given date
		avgs = d.CurrentBaseline.Averages

		// add the current proportions to the next baseline
		d.NextBaseline.Forward(proportions)
	} else {
		fmt.Println("ticking forward to the next date")
		// compute new baseline from previous baseline (currently called "next"), then reset next
		avgs = d.NextBaseline.Averages()

		// copy avgs over currentBaseline
		d.CurrentBaseline.Averages = avgs
		d.CurrentBaseline.Initialized = true
		d.CurrentBaseline.Timestamp = timestamp.Unix()

		// since we reset the baseline, set dot to 0.5
		d.D = 0.5

		// reset NextBaseline
		d.NextBaseline.Reset()
	}

	for sentiment, proportion := range proportions {
		avgProportion := avgs[sentiment]
		// if its not neutral AND it's acting above the average for the last day, increase the dot value because of that sentiment
		// proportion has to win against its average
		if sentiment == "positive" || sentiment == "negative" {
			if proportion > avgProportion {
				d.D = limitDot(d.D + 0.01)
				return nil
			} else {
				d.D = limitDot(d.D - 0.01)
				return nil
			}
		}
	}

	// no sentiment won at this tick, so the network is feeling less emotions
	d.D = limitDot(d.D - 0.005)
	return nil
}
