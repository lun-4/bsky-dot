package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SentimentFromText_V1(t *testing.T) {
	cfg := Config{
		databasePath: "./dot.db",
		upstreamType: UpstreamType_BLUESKY,
		httpPort:     "n/a",
		debug:        true,
		embeddingUrl: os.Getenv("LLAMACPP_EMBEDDING_URL"),
		numWorkers:   0,
	}
	cfg.Defaults()
	if len(os.Args) < 2 {
		panic("need more args")
	}
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx := context.Background()
	db, err := sql.Open("sqlite3", cfg.databasePath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fakeState := &State{
		cfg: cfg,
		ctx: ctx,
		db:  db,
	}

	primaryEmbeddings := getPrimaryEmbeddings(fakeState)

	tests := []struct {
		text string
		want string
	}{
		{
			text: "i love the world",
			want: "positive",
		},
		{
			text: "i hate the world",
			want: "negative",
		},
		{
			text: "i am fine",
			want: "neutral",
		},
	}

	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			got := sentimentFromText_V1(fakeState.cfg, tt.text, primaryEmbeddings)
			assert.Equal(t, tt.want, got)
		})
	}
}