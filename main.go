package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/bluesky-social/indigo/repo"
	"github.com/gorilla/websocket"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	_ "gorgonia.org/tensor"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	_ "github.com/mattn/go-sqlite3"
)

type UpstreamType int

const (
	UpstreamType_BLUESKY UpstreamType = 1
	//UpstreamType_MISSKEY = "misskey"
)

func upstreamTypeFromString(s string) UpstreamType {
	switch s {
	case "bluesky":
		return UpstreamType_BLUESKY
	default:
		panic("invalid upstream type " + s)
	}
}

type Config struct {
	databasePath string
	upstreamType UpstreamType
	httpPort     string
	debug        bool
}

func (c *Config) Defaults() {
	if c.httpPort == "" {
		c.httpPort = "8080"
	}
}

type LockedInt struct {
	mut sync.Mutex
	ui  uint
}

func (li *LockedInt) Lock() {
	li.mut.Lock()
}
func (li *LockedInt) Unlock() {
	li.mut.Unlock()
}
func (li *LockedInt) Get() uint {
	return li.ui

}

func (li *LockedInt) Reset() uint {
	li.Lock()
	defer li.Unlock()
	val := li.ui
	li.ui = 0
	return val
}

type State struct {
	cfg              Config
	incomingCounter  LockedInt
	processedCounter LockedInt
	postCounter      LockedInt
	ctx              context.Context
	db               *sql.DB
}

func eventMetrics(state *State) {
	ticker := time.Tick(time.Second * 1)
	for {
		<-ticker

		func() {
			incoming := state.incomingCounter.Reset()
			processed := state.processedCounter.Reset()
			posts := state.postCounter.Reset()
			log.Printf("%d from upstream, %d posts, %d processed events a second",
				incoming, posts, processed)
		}()
	}
}

func blueskyUpstream(state *State, eventChannel chan string) {

	uri := "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	con, _, err := websocket.DefaultDialer.Dial(uri, http.Header{})
	if err != nil {
		panic(err)
	}

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			state.incomingCounter.Lock()
			defer state.incomingCounter.Unlock()
			state.incomingCounter.ui++

			rr, err := repo.ReadRepoFromCar(state.ctx, bytes.NewReader(evt.Blocks))
			if err != nil {
				return nil
			}
			for _, op := range evt.Ops {
				slog.Debug("incoming event", slog.String("path", op.Path), slog.Any("cid", op.Cid), slog.String("action", op.Action))
				rcid, recBytes, err := rr.GetRecordBytes(state.ctx, op.Path)
				if err != nil {
					return nil
				}
				if state.cfg.debug {
					fmt.Println(rcid)
				}

				recordType, recordData, err := data.ExtractTypeCBORReader(bytes.NewReader(*recBytes))
				if err != nil {
					return nil
				}

				switch recordType {
				case "app.bsky.feed.post":
					state.postCounter.Lock()
					defer state.postCounter.Unlock()
					state.postCounter.ui++

					rec, err := data.UnmarshalCBOR(recordData)
					if err != nil {
						return nil
					}

					recJSON, err := json.Marshal(rec)
					if err != nil {
						return nil
					}
					slog.Debug("event", slog.String("text", string(recJSON)))

					eventChannel <- rec["text"].(string)
				}
			}

			return nil
		},
	}
	sched := sequential.NewScheduler("myfirehose", rsc.EventHandler)
	events.HandleRepoStream(state.ctx, con, sched)
}

func eventProcessor(state *State, eventChannel <-chan string) {
	for {
		text := <-eventChannel
		slog.Debug("processing event", slog.String("text", text))
		func() {
			state.processedCounter.Lock()
			defer state.processedCounter.Unlock()
			state.processedCounter.ui++
		}()
	}
}

func main() {
	cfg := Config{
		databasePath: "./dot.db",
		upstreamType: upstreamTypeFromString(os.Getenv("UPSTREAM_TYPE")),
		httpPort:     os.Getenv("HTTP_PORT"),
		debug:        os.Getenv("DEBUG") != "",
	}
	cfg.Defaults()
	if len(os.Args) < 2 {
		panic("need more args")
	}
	arg := os.Args[1]
	if cfg.debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	ctx := context.Background()
	db, err := sql.Open("sqlite3", cfg.databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// TODO rest of tables
	_, err = db.Exec(`
	PRAGMA journal_mode=WAL;
	CREATE TABLE IF NOT EXISTS original_embeddings (
		post text primary key,
		embedding text -- json encoded
	) STRICT;
	`)
	if err != nil {
		panic(err)
	}
	state := State{
		cfg: cfg,
		ctx: ctx,
		db:  db,
	}

	switch arg {
	case "embed-everything":
		embedEverything(&state, cfg)
	case "run":
		run(&state, cfg)
	default:
		fmt.Println("Usage: bskydot [embed-everything | run]")
	}
}

type TweetEvalRow struct {
	Text  *string `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label *int64  `parquet:"name=label, type=INT64"`
}

func getUpstreamEmbedding(client http.Client, text string) []float64 {

	body := map[string]any{
		"content": text,
	}
	encodedBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", "http://kotys:4575/embedding", bytes.NewReader(encodedBody))
	if err != nil {
		panic(err)
	}
	res, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	responseBytes, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	if res.StatusCode != http.StatusOK {
		slog.Error("HTTP error", slog.Int("status", res.StatusCode), slog.String("response", string(responseBytes)))
		panic("http status")
	}

	var resJson map[string]any
	err = json.Unmarshal(responseBytes, &resJson)
	if err != nil {
		panic(err)
	}
	embeddingAny := resJson["embedding"].([]any)
	embedding := make([]float64, len(embeddingAny))
	for i, item := range embeddingAny {
		value := item.(float64)
		embedding[i] = value
	}
	return embedding

}

func embedEverything(state *State, cfg Config) {
	fr, err := local.NewLocalFileReader("dataset/train.parquet")
	if err != nil {
		panic(err)
	}

	pr, err := reader.NewParquetReader(fr, new(TweetEvalRow), 4)
	if err != nil {
		panic(err)
	}

	cli := http.Client{}
	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		stus := make([]TweetEvalRow, 1)
		if err = pr.Read(&stus); err != nil {
			slog.Error("Read error", slog.String("err", err.Error()))
			panic(err)
		}
		for _, row := range stus {
			text := *row.Text
			label := *row.Label
			fmt.Println(label, text)
			row := state.db.QueryRow("SELECT embedding FROM original_embeddings WHERE post=?", text)
			var databaseEmbedding string
			var embedding []float64
			err := row.Scan(&databaseEmbedding)
			if errors.Is(err, sql.ErrNoRows) {
				// get embedding from upstream llama.cpp
				embedding = getUpstreamEmbedding(cli, text)
				data, err := json.Marshal(embedding)
				if err != nil {
					panic(err)
				}
				_, err = state.db.Exec("INSERT INTO original_embeddings (post, embedding) VALUES (?, ?)", text, string(data))
				if err != nil {
					panic(err)
				}
			} else if err != nil {
				panic(err)
			} else {
				err := json.Unmarshal([]byte(databaseEmbedding), &embedding)
				if err != nil {
					panic(err)
				}
			}
			// faster
			//embeddingT := tensor.New(tensor.WithShape(1, len(embedding)), tensor.WithBacking(embedding))
			// faster
			//fmt.Println(embeddingT)
		}
	}
	pr.ReadStop()
	fr.Close()
}

func run(state *State, cfg Config) {
	eventChannel := make(chan string, 1000)

	if cfg.upstreamType == UpstreamType_BLUESKY {
		go blueskyUpstream(state, eventChannel)
	} else {
		panic("unsupported upstream type. this is a bug")
	}

	go eventMetrics(state)
	go eventProcessor(state, eventChannel)

	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.GET("/", hello)
	slog.Info("Starting server", slog.String("port", cfg.httpPort))
	e.Logger.Fatal(e.Start(":" + cfg.httpPort))
}

// Handler
func hello(c echo.Context) error {
	return c.String(http.StatusOK, "Hello, World!")
}
