package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/bluesky-social/indigo/repo"
	"github.com/enescakir/emoji"
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
	databasePath     string
	upstreamType     UpstreamType
	httpPort         string
	embeddingUrl     string
	embeddingToken   string
	embeddingVersion string
	debug            bool
	numWorkers       uint
}

const DEFAULT_DATABASE_PATH = "./dot.db"

func (c *Config) Defaults() {
	if c.embeddingUrl == "" {
		panic("must have LLAMACPP_EMBEDDING_URL set")
	}
	if c.httpPort == "" {
		c.httpPort = "8080"
	}
	if c.databasePath == "" {
		c.databasePath = DEFAULT_DATABASE_PATH
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
func (li *LockedInt) UnlockedGet() uint {
	return li.ui
}

func (li *LockedInt) LockAndGet() uint {
	li.Lock()
	defer li.Unlock()
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

func (s *State) PrintState() {
	slog.Info("current state, attempting to lock ints")
	slog.Info("  incoming", slog.Any("count", s.incomingCounter.LockAndGet()))
	slog.Info("  processed", slog.Any("count", s.processedCounter.LockAndGet()))
	slog.Info("  posts", slog.Any("count", s.postCounter.LockAndGet()))
}

func eventMetrics(state *State, eventChan chan Post) {
	ticker := time.Tick(time.Second * 1)
	for {
		<-ticker

		func() {
			incoming := state.incomingCounter.Reset()
			processed := state.processedCounter.Reset()
			posts := state.postCounter.Reset()
			events := len(eventChan)
			if events > 999 {
				slog.Warn("too many events! system is bottlenecked..", slog.Int("events", events))
			}
			log.Printf("%d from upstream, %d posts, %d processed events a second (channel len %d)",
				incoming, posts, processed, events)
		}()
	}
}

func blueskyUpstream(state *State, eventChannel chan Post, errorChannel chan error) {
	uri := "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
	con, _, err := websocket.DefaultDialer.Dial(uri, http.Header{})
	if err != nil {
		panic(err)
	}

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			func() {
				state.incomingCounter.Lock()
				defer state.incomingCounter.Unlock()
				state.incomingCounter.ui++
			}()

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
				slog.Debug("event", slog.String("rcid", rcid.String()))

				recordType, recordData, err := data.ExtractTypeCBORReader(bytes.NewReader(*recBytes))
				if err != nil {
					return nil
				}

				switch recordType {
				case "app.bsky.feed.post":

					rec, err := data.UnmarshalCBOR(recordData)
					if err != nil {
						return nil
					}

					recJSON, err := json.Marshal(rec)
					if err != nil {
						return nil
					}
					slog.Debug("event", slog.String("text", string(recJSON)))
					// ignore non-english to prevent model crashes
					if rec["langs"] != nil {
						langs := rec["langs"].([]any)
						if len(langs) > 0 {
							primaryLang := langs[0].(string)
							if primaryLang != "en" {
								return nil
							}
						}
					}

					postText, ok := rec["text"].(string)
					if ok {
						if postText == "" {
							return nil
						}

						// it might be english, but still be absolutely fucked, example:
						// ᴀᴡᴡ, ʏᴏᴜ ᴡᴇʀᴇ ᴛᴏᴏ ꜱᴄᴀʀᴇᴅ ᴛᴏ ᴀᴅᴍɪᴛ ʏᴏᴜ ʜᴀᴠᴇ ᴀ ᴄʀᴜꜱʜ ᴏɴ ᴍᴇ? ᴀꜱ ɪꜰ ɪ ᴅɪᴅɴ’ᴛ ᴋɴᴏᴡ, ʏᴏᴜ’ʀᴇ ꜱᴏ ᴏʙᴠɪᴏᴜꜱ ᴡʜᴇɴ ʏᴏᴜ ꜱᴛᴀʀᴇ ꜱᴏᴍᴇᴛɪᴍᴇꜱ ɪ ᴛʜɪɴᴋ ɪ’ʟʟ ʜᴀᴠᴇ ᴛᴏ ᴡɪᴘᴇ ʏᴏᴜʀ ᴅʀᴏᴏʟ ᴀᴡᴀʏ…ʙᴜᴛ ᴡᴇ ᴄᴏᴜʟᴅ ᴀʟᴡᴀʏꜱ ᴀᴅᴅ ᴍᴏʀᴇ ɪɴꜱᴛᴇᴀᴅ. ɪꜰ ʏᴏᴜ ʀᴇᴀʟʟʏ ʟɪᴋᴇ ᴍᴇ, ᴏᴘᴇɴ ᴜᴘ ᴀɴᴅ ᴛᴀᴋᴇ ᴛʜɪꜱ ꜱᴘɪᴛ, ʟᴏꜱᴇʀ.
						asciiCount := 0
						for _, r := range postText {
							if r <= 127 { // ASCII characters are 0-127
								asciiCount++
							}
						}

						ratio := float64(asciiCount) / float64(len(postText))
						if ratio < 0.3 {
							return nil
						}

						textHashBytes := md5.Sum([]byte(postText))
						textHash := hex.EncodeToString(textHashBytes[:])

						func() {
							state.postCounter.Lock()
							defer state.postCounter.Unlock()
							state.postCounter.ui++
						}()
						eventChannel <- Post{text: postText, hash: textHash}
					} else {
						slog.Error("invalid event. expected a text field...", slog.Any("record", rec), slog.Any("text?", rec["text"]))
						return nil
					}

				}
			}

			return nil
		},
	}
	sched := sequential.NewScheduler("myfirehose", rsc.EventHandler)
	err = events.HandleRepoStream(state.ctx, con, sched)
	errorChannel <- err
}

type Post struct {
	text string
	hash string
}

func eventProcessor(state *State, eventChannel chan Post, upstreamUrl string) {
	switch state.cfg.embeddingVersion {
	case "v1":
		eventProcessor_V1(state, eventChannel)
	case "v3":
		eventProcessor_V3(state, eventChannel, upstreamUrl)
	default:
		panic("invalid embedding version...")
	}
}

func eventProcessor_V1(state *State, eventChannel <-chan Post) {
	primaryEmbeddings := getPrimaryEmbeddings_V1(state)

	for {
		post := <-eventChannel
		slog.Debug("processing event", slog.String("text", post.text))
		sentiment := sentimentFromText_V1(state.cfg, post.text, primaryEmbeddings)

		func() {
			tx, err := state.db.Begin()
			defer tx.Commit()
			if err != nil {
				panic(err)
			}
			_, err = tx.Exec(`INSERT INTO sentiment_data (post_hash, post, sentiment_analyst, sentiment_data) VALUES (?, ?, ?, ?)
			ON CONFLICT DO NOTHING`,
				post.hash, post.text, "v1", sentiment)
			if err != nil {
				slog.Error("error in db insert to sentiment_data", slog.String("err", err.Error()))
			}
			_, err = tx.Exec(`INSERT INTO sentiment_events (timestamp, post_hash, sentiment_analyst) VALUES (?, ?, ?)`,
				time.Now().UnixMilli(), post.hash, "v1")
			if err != nil {
				slog.Error("error in db insert", slog.String("err", err.Error()))
			}
		}()

		// TODO maybe bring sentimentChannel back so that dotProcessor doesn't need to execute db queries on every tick...??
		//sentimentChannel <- sentiment
		func() {
			state.processedCounter.Lock()
			defer state.processedCounter.Unlock()
			state.processedCounter.ui++
		}()
	}
}

func eventProcessor_V3(state *State, eventChannel chan Post, upstreamUrl string) {

	newCfg := state.cfg
	newCfg.embeddingUrl = upstreamUrl
	for {
		post := <-eventChannel
		slog.Debug("processing event", slog.String("text", post.text))
		sentiment, err := sentimentFromText_V3(newCfg, post.text)
		if err != nil {
			slog.Error("an error happened while sending post to sentiment worker, resubmitting in 10ms..", slog.String("error", err.Error()))
			time.Sleep(10 * time.Millisecond)
			eventChannel <- post
			continue
		}

		func() {
			tx, err := state.db.Begin()
			defer tx.Commit()
			if err != nil {
				panic(err)
			}
			_, err = tx.Exec(`INSERT INTO sentiment_data (post_hash, post, sentiment_analyst, sentiment_data) VALUES (?, ?, ?, ?)
			ON CONFLICT DO NOTHING`,
				post.hash, post.text, "v3", sentiment)
			if err != nil {
				slog.Error("error in db insert to sentiment_data", slog.String("err", err.Error()))
			}
			_, err = tx.Exec(`INSERT INTO sentiment_events (timestamp, post_hash, sentiment_analyst) VALUES (?, ?, ?)`,
				time.Now().UnixMilli(), post.hash, "v3")
			if err != nil {
				slog.Error("error in db insert", slog.String("err", err.Error()))
			}
		}()

		// TODO maybe bring sentimentChannel back so that dotProcessor doesn't need to execute db queries on every tick...??
		//sentimentChannel <- sentiment
		func() {
			state.processedCounter.Lock()
			defer state.processedCounter.Unlock()
			state.processedCounter.ui++
		}()
	}
}

func parseUint(v string) uint {
	if v == "" {
		panic("empty value")
	}
	vInt, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		panic(err)
	}
	return uint(vInt)
}

func getEnvUint(key string, defaultValue uint) uint {
	value := os.Getenv(key)
	if value == "" {
		value = fmt.Sprintf("%d", defaultValue)
	}
	valueUint := parseUint(value)
	return valueUint
}

func main() {
	cfg := Config{
		databasePath:     os.Getenv("DATABASE_PATH"),
		upstreamType:     upstreamTypeFromString(os.Getenv("UPSTREAM_TYPE")),
		httpPort:         os.Getenv("HTTP_PORT"),
		debug:            os.Getenv("DEBUG") != "",
		embeddingUrl:     os.Getenv("LLAMACPP_EMBEDDING_URL"),
		embeddingToken:   os.Getenv("AUTH_TOKEN"),
		embeddingVersion: "v3",
		numWorkers:       getEnvUint("NUM_WORKERS", 3),
	}
	cfg.Defaults()
	if len(os.Args) < 2 {
		panic("need more args")
	}
	arg := os.Args[1]
	if cfg.debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	f, err := os.OpenFile("dot.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	wrt := io.MultiWriter(os.Stderr, f)

	log.SetOutput(wrt)

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
		embedding_version text,
		embedding text -- json encoded
	) STRICT;
	CREATE TABLE IF NOT EXISTS primary_sentiment_vectors (
		label text,
		embedding text,
		sentiment_analyst text
	) STRICT;
	CREATE TABLE IF NOT EXISTS sentiment_data (
		post_hash text primary key,
		post text,
		sentiment_analyst text,
		sentiment_data text
	) STRICT;
	CREATE TABLE IF NOT EXISTS sentiment_events (
		timestamp integer,
		post_hash text,
		sentiment_analyst text,
		primary key (timestamp, post_hash)
	) STRICT;
	CREATE TABLE IF NOT EXISTS dot_data (
		timestamp integer,
		dot_analyst text,
		data text,
		primary key (timestamp, dot_analyst)
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

	validateEmbeddingModel(cfg)

	switch arg {
	case "embed-everything-v1":
		embedEverything_V1(&state, cfg)
	case "embed-everything-v2":
		embedEverything_V2(&state, cfg)
	case "run":
		run(&state, cfg)
	case "test-dot-algo":
		testDotAlgorithm(&state)
	default:
		fmt.Println("Usage: bskydot [embed-everything-v1 | embed-everything-v2 | run]")
	}
}

type TweetEvalRow struct {
	Text  *string `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Label *int64  `parquet:"name=label, type=INT64"`
}

func validateEmbeddingModel(cfg Config) {
	var url string
	if strings.Contains(cfg.embeddingUrl, ",") {
		urls := strings.Split(cfg.embeddingUrl, ",")
		url = urls[rand.Intn(len(urls))]
	} else {
		url = cfg.embeddingUrl
	}
	req, err := http.NewRequest("GET", url+"/v1/models", nil)
	if err != nil {
		panic(err)
	}
	client := http.Client{}
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
	var modelsMap map[string]any
	err = json.Unmarshal(responseBytes, &modelsMap)
	if err != nil {
		panic(err)
	}
	modelsData := modelsMap["data"].([]any)
	firstModel := modelsData[0].(map[string]any)
	model := firstModel["id"].(string)
	wantedModel := EMBEDDING_META[cfg.embeddingVersion].model
	if !strings.Contains(model, wantedModel) {
		slog.Error("model does not match expected model", slog.String("expected", wantedModel), slog.String("found", model))
		panic("model does not match expected model")
	}
}

func getUpstreamEmbedding(cfg Config, client http.Client, text string) []float64 {

	body := map[string]any{
		"content": text,
	}
	encodedBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	var url string
	if strings.Contains(cfg.embeddingUrl, ",") {
		urls := strings.Split(cfg.embeddingUrl, ",")
		url = urls[rand.Intn(len(urls))]
	} else {
		url = cfg.embeddingUrl
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/embedding", url), bytes.NewReader(encodedBody))
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

func zeroEmbedding(size uint, val float64) []float64 {
	zeroes := make([]float64, size)
	for idx := range zeroes {
		zeroes[idx] = val
	}
	return zeroes
}

func embedEverything_V1(state *State, cfg Config) {
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
	meta := EMBEDDING_META["v1"]
	shape := tensor.WithShape(int(meta.size))
	labelEmbeddings := EmbeddingHolder{
		negative: tensor.New(shape, tensor.WithBacking(zeroEmbedding(meta.size, 0))),
		neutral:  tensor.New(shape, tensor.WithBacking(zeroEmbedding(meta.size, 0))),
		positive: tensor.New(shape, tensor.WithBacking(zeroEmbedding(meta.size, 0))),
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
			embeddingT := tensor.New(tensor.WithShape(len(embedding)), tensor.WithBacking(embedding))
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

	storePrimaryEmbedding(state, "negative", negativeAverage, nil)
	storePrimaryEmbedding(state, "neutral", neutralAverage, nil)
	storePrimaryEmbedding(state, "positive", positiveAverage, nil)
	fmt.Println("Finished")
}

func storePrimaryEmbedding(state *State, sentiment string, primaryEmbedding tensor.Tensor, version *string) {
	array := primaryEmbedding.Data().([]float64)
	encodedB, err := json.Marshal(array)
	if err != nil {
		panic(err)
	}
	encoded := string(encodedB)
	_, err = state.db.Exec(`INSERT INTO primary_sentiment_vectors (label, embedding, sentiment_analyst) VALUES (?, ?, ?)
					ON CONFLICT DO UPDATE SET embedding=?`, sentiment, encoded, version, encoded)
	if err != nil {
		panic(err)
	}
}

// TODO: this is unused and should be removed very soon
func sentimentProcessor(state *State) {
	//sentimentCounters := make(map[string]uint)
	//ticker := time.Tick(1 * time.Minute)
	//	timePeriod := 5 * time.Second
	timePeriod := 1 * time.Minute
	ticker := time.Tick(timePeriod)
	for {
		select {
		//case sentiment := <-sentimentChannel:
		//	sentimentCounters[sentiment]++
		case <-ticker:
			// get all sentiments from the past minute
			slog.Info("computing sentiments..")
			rows, err := state.db.Query(`SELECT post_hash FROM sentiment_events WHERE timestamp > ? and sentiment_analyst = ?`,
				time.Now().Add(-timePeriod).UnixMilli(), state.cfg.embeddingVersion)
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

			if len(sentiments) == 0 {
				slog.Error("no sentiments found!!!!! problem!!! (workers died or not running fast enough)")
				state.PrintState()
				panic("no sentiments found...")
			}

			sentimentCounters := make(map[string]uint)
			for _, label := range sentiments {
				sentimentCounters[label]++
			}
			proportions := make([]float64, 3)
			for sentiment, count := range sentimentCounters {
				proportion := float64(count) / float64(len(sentiments))
				switch sentiment {
				case "negative":
					proportions[0] = proportion
				case "neutral":
					proportions[1] = proportion
				case "positive":
					proportions[2] = proportion
				}
			}

			dotValue := MapProportions(proportions)
			slog.Info("computed", slog.Float64("dot", dotValue))
			encoded, err := json.Marshal(map[string]any{
				"v": dotValue,
			})
			if err != nil {
				panic(err)
			}

			_, err = state.db.Exec(`INSERT INTO dot_data (timestamp, dot_analyst, data) VALUES (?, ?, ?)`,
				time.Now().Unix(), "dot_v1", string(encoded))
			if err != nil {
				slog.Error("error in db insert to sentiment_data", slog.String("err", err.Error()))
			}

		}
	}
}

type CustomContext struct {
	echo.Context
	state *State
}

func (c *CustomContext) State() *State {
	return c.state
}

func upstreamWorker(state *State, eventChannel chan Post) {
	errorChannel := make(chan error, 1)
	for {
		switch state.cfg.upstreamType {
		case UpstreamType_BLUESKY:
			go blueskyUpstream(state, eventChannel, errorChannel)
		default:
			panic("unsupported upstream type. this is a bug")
		}

		err := <-errorChannel
		slog.Error("upstream worker failed, restarting", slog.String("err", err.Error()))
	}
}

func run(state *State, cfg Config) {
	eventChannel := make(chan Post, 1000)
	//sentimentChannel := make(chan string, 1000)

	go upstreamWorker(state, eventChannel)

	go eventMetrics(state, eventChannel)
	slog.Info("event processors", slog.Uint64("workers", uint64(state.cfg.numWorkers)))

	urls := make([]string, 0)
	if strings.Contains(cfg.embeddingUrl, ",") {
		urls = strings.Split(cfg.embeddingUrl, ",")
	} else {
		urls = append(urls, cfg.embeddingUrl)
	}

	for _, url := range urls {
		for idx := range state.cfg.numWorkers {
			slog.Info("spawn worker", slog.Uint64("index", uint64(idx)), slog.String("url", url))
			go eventProcessor(state, eventChannel, url)
		}
	}
	go dotProcessor_V2(state, CURRENT_DOT_VERSION)

	e := echo.New()

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			cc := &CustomContext{c, state}
			return next(cc)
		}
	})
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.GET("/", hello)
	slog.Info("Starting server", slog.String("port", cfg.httpPort))
	e.Logger.Fatal(e.Start(":" + cfg.httpPort))
}

func GetCurrentDot(state *State) (float64, error) {

	var dotDataEncoded string
	err := state.db.QueryRow(`SELECT data FROM dot_data ORDER BY timestamp DESC LIMIT 1`).Scan(&dotDataEncoded)
	if err != nil {
		return 0, err
	}
	var dotData map[string]any
	err = json.Unmarshal([]byte(dotDataEncoded), &dotData)
	if err != nil {
		return 0, err
	}
	dv, ok := dotData["d"].(float64)
	if !ok {
		slog.Error("invalid dot data", slog.String("data", dotDataEncoded))
		panic("invalid dot value")
	}
	return dv, nil
}

type Dot struct {
	UnixTimestamp int64
	Value         map[string]any
}
type ExposedDot struct {
	UnixTimestamp int64   `json:"t"`
	Value         float64 `json:"v"`
}
type ParsedDot struct {
	UnixTimestamp int64
	Dot           DotImpl
}

func GetLastCoupleDots(state *State, version string) ([]Dot, error) {
	rows, err := state.db.Query(`SELECT timestamp, data FROM dot_data WHERE dot_analyst = ? ORDER BY timestamp DESC LIMIT 3000`, version)
	if err != nil {
		return nil, err
	}
	dots := make([]Dot, 0)
	for rows.Next() {
		var dot Dot
		var dotDataEncoded string
		err := rows.Scan(&dot.UnixTimestamp, &dotDataEncoded)
		if err != nil {
			return nil, err
		}

		var dotData map[string]any
		err = json.Unmarshal([]byte(dotDataEncoded), &dotData)
		if err != nil {
			return nil, err
		}
		dot.Value = dotData
		dots = append(dots, dot)
	}
	return dots, nil
}

const CURRENT_DOT_VERSION = "v5"

func dotEmojiFromValue(value float64) (emoji.Emoji, string) {
	if value < 0.1 {
		return emoji.BlueCircle, "the network isn't feeling quite a lot..."
	} else if value < 0.4 {
		return emoji.YellowCircle, "the network is bubbling an emotion..."
	} else if value < 0.7 {
		return emoji.GreenCircle, "the network's humor meter is average."
	} else if value < 0.9 {
		return emoji.YellowCircle, "the network is feeling something."
	} else if value <= 1.0 {
		return emoji.RedCircle, "the network is Feeling It. A lot. it is feeling Something, a lot."
	}
	panic("invalid value")
}

func NewDot(version string, data map[string]any) DotImpl {
	var dotValue DotImpl
	switch version {
	case "v1":
		dotValue = lo.ToPtr(NewDotV1(data))
	case "v2":
		dotValue = lo.ToPtr(NewDotV2(data))
	case "v3":
		dotValue = lo.ToPtr(NewDotV3(data))
	case "v4":
		dotValue = lo.ToPtr(NewDotV4(data))
	case "v5":
		dotValue = lo.ToPtr(NewDotV5(data))
	default:
		panic("unsupported version " + version)
	}
	return dotValue
}

func NewEmptyDot(version string) DotImpl {
	var dotValue DotImpl
	switch version {
	case "v1":
		dotValue = lo.ToPtr(NewEmptyDotV1())
	case "v2":
		dotValue = lo.ToPtr(NewEmptyDotV2())
	case "v3":
		dotValue = lo.ToPtr(NewEmptyDotV3())
	case "v4":
		dotValue = lo.ToPtr(NewEmptyDotV4())
	case "v5":
		dotValue = lo.ToPtr(NewEmptyDotV5())
	default:
		panic("unsupported version " + version)

	}
	return dotValue
}

// Handler
func hello(c echo.Context) error {
	cc := c.(*CustomContext)
	dotValue, err := GetCurrentDot(cc.State())
	if err != nil {
		return err
	}
	dotEmoji, dotEmojiText := dotEmojiFromValue(dotValue)
	dots, err := GetLastCoupleDots(cc.State(), CURRENT_DOT_VERSION)
	if err != nil {
		return err
	}

	filename, err := GenerateDotPlot(dots, CURRENT_DOT_VERSION)
	if err != nil {
		return err
	}
	defer os.Remove(filename)

	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	arr := make([]map[string]any, 0)
	for _, dot := range dots {
		dotValue := NewDot(CURRENT_DOT_VERSION, dot.Value)
		arr = append(arr, map[string]any{
			"timestamp": dot.UnixTimestamp,
			"dot":       dotValue.Value(),
		})
	}
	encodedJsonValues, err := json.Marshal(arr)
	if err != nil {
		panic(err)
	}

	encodedImg := base64.StdEncoding.EncodeToString(data)
	c.Response().Header().Set("content-type", "text/html")
	return c.HTML(http.StatusOK, fmt.Sprintf(`
		<head>
			<meta charset="utf-8">
			<style type="text/css">
			p, h2, h3, h4{
				line-height: 10px;
			}
			h5 {
				line-height: 2px;
			}
			</style>
		</head>
		<body>
			<h1>the bluesky dot</h1>
			<h2>a funny experiment on the firehose and processing data at scale,
			<p>measuring the amount of Emotion in the bluesky network</p><h2>
			<h3>by <a href="https://bsky.app/profile/l4.pm">@l4.pm</a></h3>
			<h5>and <a href="https://bsky.app/profile/natalie.ee">@natalie.ee</a> for the original idea and hardware</h5>
			<h5>and <a href="https://bsky.app/profile/philpax.me">@philpax.me</a> for helping me with architecture design</h5>
			<h4>i am not good at web design and i had a self-imposed deadline to finish this. it is what it is.</h4>
			<p>the current value is <b>%.5f %v %s</b></p>
			<p>value's range is 0 to 1. the higher, the more Emotion the network is feeling.</p>
			<p>last two days:</p>
			<!-- <img src="data:image/png;base64,%s" alt="dot historical" /> -->
			<div id="myplot"></div>
			<p>if you want to provide better algorithms or just look at code, here's the <a href="https://github.com/lun-4/bsky-dot">source code</a></p>
		</body>
		<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
		<script src="https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6"></script>
		<script type="module">
			const data = %s.map(d => ({...d, timestamp: new Date(d.timestamp * 1000)}));

			const plot = Plot.plot({
				marginTop: 20,
				marginRight: 40,
				marginBottom: 30,
				marginLeft: 40,
				width: 1500,
				x: {label: "time"},
        y: {domain: [0, 1], grid: true},
				marks: [
					Plot.frame(),
					Plot.axisY({label: "emotion"}),
					Plot.axisY({label: null, anchor: "right"}),
					Plot.line(data, {x: "timestamp", y: "dot", stroke: "red", markerStart: 'circle-stroke'}),
					Plot.crosshairX(data, {x: "timestamp", y: "dot"})
				]
			});
			const div = document.querySelector("#myplot");
			div.append(plot);
		</script>
	`, dotValue, dotEmoji, dotEmojiText, encodedImg[:10], string(encodedJsonValues)))
}

// CosineSimilarity calculates the cosine similarity between two vectors `a` and `b`
func CosineSimilarity2(a, b tensor.Tensor) (float64, error) {
	// Ensure that `a` and `b` are both 1-dimensional
	if a.Dims() != 1 || b.Dims() != 1 {
		return 0, fmt.Errorf("cosine similarity requires 1-dimensional tensors")
	}

	// Ensure both vectors have the same length
	if a.Size() != b.Size() {
		return 0, fmt.Errorf("vectors must have the same size")
	}

	// Calculate dot product
	dot, err := tensor.Inner(a, b)
	if err != nil {
		return 0, err
	}

	// Calculate norms of a and b
	normA := 0.0
	normB := 0.0
	for i := 0; i < a.Size(); i++ {
		va, err := a.At(i)
		if err != nil {
			return 0, err
		}
		vb, err := b.At(i)
		if err != nil {
			return 0, err
		}

		normA += va.(float64) * va.(float64)
		normB += vb.(float64) * vb.(float64)
	}
	normA = math.Sqrt(normA)
	normB = math.Sqrt(normB)

	// Avoid division by zero
	if normA == 0 || normB == 0 {
		return 0, fmt.Errorf("one of the vectors has zero magnitude. normA=%f, normB=%f", normA, normB)
	}

	// Calculate cosine similarity
	cosineSim := dot.(float64) / (normA * normB)

	return cosineSim, nil
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

// MapProportions maps three proportions (A, B, C) to a value between -1 and 1
func MapProportions(proportions []float64) float64 {
	// Ensure we have exactly 3 proportions
	if len(proportions) != 3 {
		panic("MapProportions requires exactly 3 proportions")
	}

	// Get individual proportions
	a, b, c := proportions[0], proportions[1], proportions[2]

	// Normalize the proportions
	total := a + b + c
	if total > 0 {
		a, b = a/total, b/total
		c = c / total
	}

	// Calculate weighted contributions
	// A pulls negative (-1), B is neutral (0), C pulls positive (+1)
	weightedSum := -a*1.0 + c*1.0

	// Apply sigmoid-like smoothing to ensure nice distribution
	// and guarantee output is between -1 and 1
	return math.Tanh(weightedSum * 1.5)
}
