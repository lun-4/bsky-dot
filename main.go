package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
	if cfg.debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	ctx := context.Background()
	state := State{
		cfg: cfg,
		ctx: ctx,
	}
	db, err := sql.Open("sqlite3", cfg.databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// TODO rest of tables
	_, err = db.Exec(`
	PRAGMA journal_mode=WAL;
	`)
	if err != nil {
		panic(err)
	}

	eventChannel := make(chan string, 1000)

	if cfg.upstreamType == UpstreamType_BLUESKY {
		go blueskyUpstream(&state, eventChannel)
	}

	go eventMetrics(&state)
	go eventProcessor(&state, eventChannel)

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
