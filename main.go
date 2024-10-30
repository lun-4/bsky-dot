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
	"math"
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
	"github.com/samber/lo"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"gorgonia.org/tensor"

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
	embeddingUrl string
	debug        bool
}

func (c *Config) Defaults() {
	if c.embeddingUrl == "" {
		panic("must have LLAMACPP_EMBEDDING_URL set")
	}
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

func eventProcessor(state *State, eventChannel <-chan string, sentimentChannel chan string) {
	primaryEmbeddings := make(map[string]tensor.Tensor)
	rows, err := state.db.Query(`SELECT label, embedding FROM primary_sentiment_vectors`)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var label string
		var embeddingJson string
		err := rows.Scan(&label, &embeddingJson)
		if err != nil {
			panic(err)
		}
		var embeddingA []any
		err = json.Unmarshal([]byte(embeddingJson), &embeddingA)
		if err != nil {
			panic(err)
		}
		embeddingF, ok := lo.FromAnySlice[float64](embeddingA)
		if !ok {
			panic("failed to convert from any to f64 array")
		}
		primaryEmbeddingT := tensor.New(tensor.WithShape(1, 768), tensor.WithBacking(embeddingF))
		primaryEmbeddings[label] = primaryEmbeddingT
	}

	for {
		text := <-eventChannel
		slog.Debug("processing event", slog.String("text", text))
		embeddingData := getUpstreamEmbedding(state.cfg, http.Client{}, text)
		embeddingT := tensor.New(tensor.WithShape(1, 768), tensor.WithBacking(embeddingData))
		maxLabel, maxDistance := "", math.Inf(1)
		//fmt.Println(text)
		for label, primaryEmbedding := range primaryEmbeddings {
			distance, err := CosineSimilarity(primaryEmbedding, embeddingT)
			if err != nil {
				panic(err)
			}
			if distance < maxDistance {
				maxLabel = label
				maxDistance = distance
			}
			//fmt.Println(label, distance)
		}
		_, err = state.db.Exec(`INSERT INTO sentiment_data (post, sentiment_analyst, sentiment_data) VALUES (?, ?, ?)
			ON CONFLICT DO NOTHING`,
			text, "v1", maxLabel)
		if err != nil {
			slog.Error("error in db insert to sentiment_data", slog.String("err", err.Error()))
		}
		_, err = state.db.Exec(`INSERT INTO sentiment_events (timestamp, post, sentiment_analyst) VALUES (?, ?, ?)`,
			time.Now().UnixMilli(), text, "v1")
		if err != nil {
			slog.Error("error in db insert", slog.String("err", err.Error()))
		}
		sentimentChannel <- maxLabel
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
		embeddingUrl: os.Getenv("LLAMACPP_EMBEDDING_URL"),
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
	CREATE TABLE IF NOT EXISTS primary_sentiment_vectors (
		label text,
		embedding text
	) STRICT;
	CREATE TABLE IF NOT EXISTS sentiment_data (
		post text primary key,
		sentiment_analyst text,
		sentiment_data text
	) STRICT;
	CREATE TABLE IF NOT EXISTS sentiment_events (
		timestamp integer,
		post text,
		sentiment_analyst text,
		primary key (timestamp, post)
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

func getUpstreamEmbedding(cfg Config, client http.Client, text string) []float64 {

	body := map[string]any{
		"content": text,
	}
	encodedBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/embedding", cfg.embeddingUrl), bytes.NewReader(encodedBody))
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

var SENTIMENT_MAP = map[int]string{
	0: "negative",
	1: "neutral",
	2: "positive",
}

func zeroArray() []float64 {
	zeroes := make([]float64, 768)
	for idx, _ := range zeroes {
		zeroes[idx] = 0.0
	}
	return zeroes
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

	type EmbeddingHolder struct {
		negative      tensor.Tensor
		negativeCount float64
		neutral       tensor.Tensor
		neutralCount  float64
		positive      tensor.Tensor
		positiveCount float64
	}
	labelEmbeddings := EmbeddingHolder{
		negative: tensor.New(tensor.WithShape(1, 768), tensor.WithBacking(zeroArray())),
		neutral:  tensor.New(tensor.WithShape(1, 768), tensor.WithBacking(zeroArray())),
		positive: tensor.New(tensor.WithShape(1, 768), tensor.WithBacking(zeroArray())),
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
			row := state.db.QueryRow("SELECT embedding FROM original_embeddings WHERE post=?", text)
			var databaseEmbedding string
			var embedding []float64
			err := row.Scan(&databaseEmbedding)
			if errors.Is(err, sql.ErrNoRows) {
				// get embedding from upstream llama.cpp
				embedding = getUpstreamEmbedding(cfg, cli, text)
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
			embeddingT := tensor.New(tensor.WithShape(1, len(embedding)), tensor.WithBacking(embedding))
			sentiment := SENTIMENT_MAP[int(label)]
			fmt.Println(label, embeddingT)
			switch sentiment {
			case "negative":
				labelEmbeddings.negative, err = tensor.Add(labelEmbeddings.negative, embeddingT)
				if err != nil {
					panic(err)
				}
				labelEmbeddings.negativeCount++
			case "neutral":
				labelEmbeddings.neutral, err = tensor.Add(labelEmbeddings.neutral, embeddingT)
				if err != nil {
					panic(err)
				}
				labelEmbeddings.neutralCount++
			case "positive":
				labelEmbeddings.positive, err = tensor.Add(labelEmbeddings.positive, embeddingT)
				if err != nil {
					panic(err)

				}
				labelEmbeddings.positiveCount++
			default:
				panic("unknown sentiment")
			}
		}
	}
	pr.ReadStop()
	fr.Close()

	negativeAverage, err := tensor.Div(labelEmbeddings.negative, labelEmbeddings.negativeCount)
	if err != nil {
		panic(err)
	}
	neutralAverage, err := tensor.Div(labelEmbeddings.neutral, labelEmbeddings.neutralCount)
	if err != nil {
		panic(err)
	}
	positiveAverage, err := tensor.Div(labelEmbeddings.positive, labelEmbeddings.positiveCount)
	if err != nil {
		panic(err)
	}

	storePrimaryEmbedding(state, "negative", negativeAverage)
	storePrimaryEmbedding(state, "neutral", neutralAverage)
	storePrimaryEmbedding(state, "positive", positiveAverage)
	fmt.Println("Finished")
}

func storePrimaryEmbedding(state *State, sentiment string, primaryEmbedding tensor.Tensor) {
	array := primaryEmbedding.Data().([]float64)
	encodedB, err := json.Marshal(array)
	if err != nil {
		panic(err)
	}
	encoded := string(encodedB)
	_, err = state.db.Exec(`INSERT INTO primary_sentiment_vectors (label, embedding) VALUES (?, ?)
					ON CONFLICT DO UPDATE SET embedding=?`, sentiment, encoded, encoded)
	if err != nil {
		panic(err)
	}
}

func sentimentProcessor(state *State, sentimentChannel <-chan string) {
	sentimentCounters := make(map[string]uint)
	ticker := time.Tick(2 * time.Second)
	for {
		select {
		case sentiment := <-sentimentChannel:
			sentimentCounters[sentiment]++
		case <-ticker:
			var sum uint
			for _, count := range sentimentCounters {
				sum += count
			}
			for sentiment, count := range sentimentCounters {
				score := float64(count) / float64(sum)
				fmt.Println(sentiment, score)
			}

			sentimentCounters = make(map[string]uint)
		}
	}
}

func run(state *State, cfg Config) {
	eventChannel := make(chan string, 1000)
	sentimentChannel := make(chan string, 1000)

	if cfg.upstreamType == UpstreamType_BLUESKY {
		go blueskyUpstream(state, eventChannel)
	} else {
		panic("unsupported upstream type. this is a bug")
	}

	go eventMetrics(state)
	go eventProcessor(state, eventChannel, sentimentChannel)
	go sentimentProcessor(state, sentimentChannel)

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

func CosineSimilarity(a, b tensor.Tensor) (float64, error) {
	// Check if tensors are vectors
	//if len(a.Shape()) != 1 || len(b.Shape()) != 1 {
	//	return 0, fmt.Errorf("inputs must be 1D tensors, got shapes %v and %v", a.Shape(), b.Shape())
	//}

	// Check if vectors have same length
	if a.Shape()[0] != b.Shape()[0] {
		return 0, fmt.Errorf("vectors must have equal length, got lengths %d and %d", a.Shape()[0], b.Shape()[0])
	}

	// Calculate dot product
	dotProduct, err := tensor.Dot(a, b)
	if err != nil {
		return 0, fmt.Errorf("error calculating dot product ab: %v", err)
	}
	da, err := tensor.Dot(a, a)
	if err != nil {
		return 0, fmt.Errorf("error calculating dot product da: %v", err)
	}

	db, err := tensor.Dot(b, b)
	if err != nil {
		return 0, fmt.Errorf("error calculating dot product db: %v", err)
	}
	vda := da.ScalarValue()
	vdb := db.ScalarValue()
	magnitudeA := math.Sqrt(vda.(float64))
	magnitudeB := math.Sqrt(vdb.(float64))

	similarityT, err := tensor.Div(dotProduct, magnitudeA*magnitudeB)
	if err != nil {
		return 0, fmt.Errorf("error calculating similarity: %v", err)
	}

	similarityA := similarityT.ScalarValue()
	similarity := similarityA.(float64)

	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}
	return similarity, nil
}
