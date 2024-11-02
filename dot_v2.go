package main

import (
	"time"

	"github.com/samber/lo"
)

type DotV2 struct {
	d              float64
	lastSentiments []string
}

type DotVwState float64

func NewDotV2(previousState map[string]any) DotV2 {
	sentimentsStrArray := make([]string, 0)

	for _, sentimentAny := range previousState["s"].([]any) {
		sentimentsStrArray = append(sentimentsStrArray, sentimentAny.(string))
	}
	return DotV2{
		d:              previousState["d"].(float64),
		lastSentiments: sentimentsStrArray,
	}
}

func NewEmptyDotV2() DotV2 {
	return DotV2{
		d:              0.0,
		lastSentiments: make([]string, 0),
	}
}

func (d *DotV2) Serialize() map[string]any {
	return map[string]any{
		"d": d.d,
		"s": lo.ToAnySlice(d.lastSentiments),
	}
}

func (d *DotV2) TimePeriod() time.Duration {
	return 1 * time.Minute
}
func (d *DotV2) Version() string {
	return "v2"
}

func appendToMaxLen(array []string, maxLen int, value string) []string {
	if len(array) >= maxLen {
		array = array[1:]
	}
	array = append(array, value)
	if len(array) > maxLen {
		panic("array is too long")
	}
	return array
}

func (d *DotV2) Value() float64 {
	return d.d
}

func (d *DotV2) lastWinningSentiment() string {

	winCounter := make(map[string]int)
	for _, lastSentiment := range d.lastSentiments {
		winCounter[lastSentiment]++
	}

	var winningSentiment string
	var winningCount int
	for sentiment, count := range winCounter {
		if count > winningCount {
			winningSentiment = sentiment
			winningCount = count
		}
	}

	return winningSentiment
}

func (d *DotV2) Forward(sentiments []string) error {
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
	var winningSentiment string
	for sentiment, proportion := range proportions {
		// NOTE this is a very special magical number whose tweaking leads to collapses on
		// either side of the dot spectrum (either everyone stays at 0 because no sentiment can breach the threshold,
		// or everyone's a 1 because a sentiment wins at every timestamp)
		if proportion > 0.405 {
			// the network is converging itself towards a common goal, increase dot by epsilon
			winningSentiment = sentiment
		}
	}

	lastWinningSentiment := d.lastWinningSentiment()
	currentSentimentStillWins := lastWinningSentiment == winningSentiment
	d.lastSentiments = appendToMaxLen(d.lastSentiments, 60, winningSentiment)

	if currentSentimentStillWins {
		d.d = limitDot(d.d + epsilon)
		return nil
	} else {
		// no convergence, decrease by epsilon
		d.d = limitDot(d.d - epsilon)
		return nil
	}
}
