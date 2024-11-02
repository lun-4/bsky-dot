package main

import (
	"fmt"
	"time"

	"github.com/samber/lo"
)

type DotV2Sentiment struct {
	timestamp int64  `json:"t"`
	value     string `json:"v"`
}

type DotV2 struct {
	d              float64          `json:"d"`
	lastSentiments []DotV2Sentiment `json:"s"`
}

type DotVwState float64

func NewDotV2(previousState map[string]any) DotV2 {
	sentimentsStrArray := make([]DotV2Sentiment, 0)

	for _, sentimentAny := range previousState["s"].([]any) {
		sentimentMap := sentimentAny.(map[string]any)
		sentimentsStrArray = append(sentimentsStrArray, DotV2Sentiment{
			timestamp: sentimentMap["t"].(int64),
			value:     sentimentMap["v"].(string),
		})
	}
	return DotV2{
		d:              previousState["d"].(float64),
		lastSentiments: sentimentsStrArray,
	}
}

func NewEmptyDotV2() DotV2 {
	return DotV2{
		d:              0.0,
		lastSentiments: make([]DotV2Sentiment, 0),
	}
}

func (d *DotV2) Serialize() map[string]any {
	asSentimentMap := make([]map[string]any, 0)
	for _, s := range d.lastSentiments {
		asSentimentMap = append(asSentimentMap, map[string]any{
			"t": s.timestamp,
			"v": s.value,
		})
	}
	return map[string]any{
		"d": d.d,
		"s": lo.ToAnySlice(asSentimentMap),
	}
}

func (d *DotV2) TimePeriod() time.Duration {
	return 1 * time.Minute
}
func (d *DotV2) Version() string {
	return "v2"
}

func appendToMaxLen(array []DotV2Sentiment, maxLen int, value DotV2Sentiment) []DotV2Sentiment {
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

	now := time.Now().Unix()

	winCounter := make(map[string]int)
	for _, lastSentiment := range d.lastSentiments {
		delta := now - lastSentiment.timestamp
		if delta > 20*60 {
			continue
		}
		winCounter[lastSentiment.value]++
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

	epsilonIncrease := 0.02
	epsilonDecrease := 0.007
	var winningSentiment string
	for sentiment, proportion := range proportions {
		if proportion > 0.35 {
			winningSentiment = sentiment
		}
	}

	fmt.Println("wins", winningSentiment)
	if winningSentiment == "positive" || winningSentiment == "negative" {
		winningSentimentPreviousRun := d.lastWinningSentiment()
		currentSentimentStillWins := winningSentimentPreviousRun == winningSentiment
		d.lastSentiments = appendToMaxLen(d.lastSentiments, 60, DotV2Sentiment{timestamp: time.Now().Unix(), value: winningSentiment})
		if currentSentimentStillWins {
			d.d = limitDot(d.d + epsilonIncrease)
		}
		return nil
	} else {
		// no convergence, decrease by epsilon
		d.d = limitDot(d.d - epsilonDecrease)
		return nil
	}
}
