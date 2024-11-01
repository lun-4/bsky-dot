package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"time"
)

func testDotAlgorithm(state *State) {
	var dotAction string
	if len(os.Args) > 2 {
		dotAction = os.Args[2]
	}

	switch dotAction {
	case "test":
		dotTest(state)
	case "backfill":
		dotBackfill(state)
	default:
		slog.Error("Invalid action")
	}
}

func dotTest(state *State) {
	now := time.Now()
	startAll := now.Add(-48 * time.Hour)
	//startAll := now.Add(-1 * time.Hour)
	endAll := now

	dotState := NewDotV1()
	dotValues := make([]float64, 0)
	for t := startAll; t.After(endAll) == false; t = t.Add(dotState.TimePeriod()) {
		startT := t
		endT := t.Add(dotState.TimePeriod())
		rows, err := state.db.Query(`SELECT post_hash FROM sentiment_events WHERE timestamp > ? and timestamp < ? and sentiment_analyst = ?`,
			startT.UnixMilli(), endT.UnixMilli(), "v3")
		if err != nil {
			panic(err)
		}
		sentiments := make([]string, 0)
		for rows.Next() {
			var postHash string
			err := rows.Scan(&postHash)
			if err != nil {
				panic(err)
			}

			// for each event, get its sentiment (this should be a join maybe)
			row := state.db.QueryRow(`SELECT sentiment_data FROM sentiment_data WHERE post_hash=? AND sentiment_analyst=?`, postHash, state.cfg.embeddingVersion)
			var sentimentData string
			err = row.Scan(&sentimentData)
			if err != nil {
				slog.Error("no sentiment data found in sentiment_data table", slog.String("postHash", postHash))
				continue
			}
			sentiments = append(sentiments, sentimentData)
		}

		if len(sentiments) > 0 {
			dotState.Forward(sentiments)
			dotValues = append(dotValues, dotState.d)
			fmt.Println(t, dotState.d)
		} else {
			dotValues = append(dotValues, dotState.d)
		}
	}
	fmt.Println(dotState.d)
	fname, err := GenerateDotPlot(dotValues)
	if err != nil {
		panic(err)
	}
	fmt.Println(fname)

	// Create the command
	cmd := exec.Command("feh", fname)

	// Start the process
	if err := cmd.Start(); err != nil {
		panic(err)
	}

	if err := cmd.Wait(); err != nil {
		panic(err)
	}

}

func dotBackfill(state *State) {
	now := time.Now()

	row := state.db.QueryRow(`SELECT min(timestamp) FROM sentiment_events WHERE sentiment_analyst = ?`, "v3")
	var minTimestamp int64
	err := row.Scan(&minTimestamp)
	if err != nil {
		panic(err)
	}

	startAll := time.UnixMilli(minTimestamp)
	// always start at the next available hour, on 00 seconds too
	startAll = startAll.Add(1 * time.Hour).Add(-time.Duration(startAll.Minute()) * time.Minute).Add(-1 * time.Duration(startAll.Second()) * time.Second)

	// -30mins so that the webapp is the one doing the final rebuild of data
	endAll := now.Add(-30 * time.Minute)

	dotState := NewDotV1()
	for t := startAll; t.After(endAll) == false; t = t.Add(dotState.TimePeriod()) {
		startT := t
		endT := t.Add(dotState.TimePeriod())
		rows, err := state.db.Query(`SELECT post_hash FROM sentiment_events WHERE timestamp > ? and timestamp < ? and sentiment_analyst = ?`,
			startT.UnixMilli(), endT.UnixMilli(), "v3")
		if err != nil {
			panic(err)
		}
		sentiments := make([]string, 0)
		for rows.Next() {
			var postHash string
			err := rows.Scan(&postHash)
			if err != nil {
				panic(err)
			}

			// for each event, get its sentiment (this should be a join maybe)
			row := state.db.QueryRow(`SELECT sentiment_data FROM sentiment_data WHERE post_hash=? AND sentiment_analyst=?`, postHash, state.cfg.embeddingVersion)
			var sentimentData string
			err = row.Scan(&sentimentData)
			if err != nil {
				slog.Error("no sentiment data found in sentiment_data table", slog.String("postHash", postHash))
				continue
			}
			sentiments = append(sentiments, sentimentData)
		}

		currentDotValue := dotState.d
		if len(sentiments) > 0 {
			dotState.Forward(sentiments)
			currentDotValue = dotState.d
		}

		wrapped := map[string]any{
			"d": currentDotValue,
		}

		encoded, err := json.Marshal(wrapped)
		if err != nil {
			panic(err)
		}

		fmt.Println(startT.UnixMilli(), currentDotValue)
		_, err = state.db.Exec(`INSERT INTO dot_data (timestamp, dot_analyst, data) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`, startT.UnixMilli(), "v1", string(encoded))
		if err != nil {
			panic(err)
		}

	}
	fmt.Println("ok")

}
