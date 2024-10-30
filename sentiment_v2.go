package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"

	"github.com/samber/lo"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"gorgonia.org/tensor"

	_ "github.com/mattn/go-sqlite3"
)

// v2 algorithm:
// - use v1 embeddings
// - instead of generating singular primary , generate multiple (up to 10) vectors
// - lowest distance wins the label for a post

type Distance struct {
	d float64
	l string
}

const v2PrimarySize = 10

func sentimentFromText_V2(cfg Config, text string, primaryEmbeddings map[string][]tensor.Tensor) string {
	embeddingData := getUpstreamEmbedding(cfg, http.Client{}, text)
	embeddingT := tensor.New(v1meta.Shape(), tensor.WithBacking(embeddingData))

	distances := make([]Distance, 0)
	for label, embeddings := range primaryEmbeddings {
		if len(embeddings) != v2PrimarySize {
			panic("number of embeddings must be equal to primary embeddings size")
		}
		for _, embedding := range embeddings {
			distance, err := CosineSimilarity(embedding, embeddingT)
			if err != nil {
				panic(err)
			}
			distances = append(distances, Distance{d: distance, l: label})
		}
	}

	minDistance := Distance{}
	for _, distance := range distances {
		if minDistance.l == "" {
			minDistance = distance
		}
		if distance.d < minDistance.d {
			minDistance = distance
		}
	}

	if minDistance.l == "" {
		panic("no min label")
	}
	return minDistance.l
}

func getPrimaryEmbeddings_V2(state *State) map[string][]tensor.Tensor {
	primaryEmbeddings := make(map[string][]tensor.Tensor)
	rows, err := state.db.Query(`SELECT label, embedding FROM primary_sentiment_vectors WHERE sentiment_analyst = 'v2'`)
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
		primaryEmbeddingT := tensor.New(v1meta.Shape(), tensor.WithBacking(embeddingF))
		primaryEmbeddings[label] = append(primaryEmbeddings[label], primaryEmbeddingT)
	}
	return primaryEmbeddings
}

func initHolderV2(meta Embedding) []tensor.Tensor {
	tensors := make([]tensor.Tensor, 0)
	for _ = range v2PrimarySize {
		tensors = append(tensors,
			tensor.New(meta.Shape(), tensor.WithBacking(zeroEmbedding(meta.size, 0))),
		)
	}
	return tensors

}

func embedEverything_V2(state *State, cfg Config) {
	fr, err := local.NewLocalFileReader("dataset/train.parquet")
	if err != nil {
		panic(err)
	}

	pr, err := reader.NewParquetReader(fr, new(TweetEvalRow), 4)
	if err != nil {
		panic(err)
	}

	type EmbeddingHolderV2 struct {
		negative      []tensor.Tensor
		negativeCount []float64
		neutral       []tensor.Tensor
		neutralCount  []float64
		positive      []tensor.Tensor
		positiveCount []float64
	}
	meta := EMBEDDING_META["v1"]
	labelEmbeddings := EmbeddingHolderV2{
		negative:      initHolderV2(meta),
		negativeCount: make([]float64, v2PrimarySize),
		neutral:       initHolderV2(meta),
		neutralCount:  make([]float64, v2PrimarySize),
		positive:      initHolderV2(meta),
		positiveCount: make([]float64, v2PrimarySize),
	}

	rng := rand.New(rand.NewSource(666))

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
			row := state.db.QueryRow("SELECT embedding FROM original_embeddings WHERE post=? AND embedding_version is null", text)
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
				_, err = state.db.Exec("INSERT INTO original_embeddings (post, embedding, embedding_version) VALUES (?, ?, ?)", text, string(data), nil)
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
			slog.Info("processed", slog.Int64("label", label), slog.Any("embedding", embeddingT))

			var holder *[]tensor.Tensor
			var counter *[]float64
			switch sentiment {
			case "negative":
				holder = &labelEmbeddings.negative
				counter = &labelEmbeddings.negativeCount
			case "neutral":
				holder = &labelEmbeddings.neutral
				counter = &labelEmbeddings.neutralCount
			case "positive":
				holder = &labelEmbeddings.positive
				counter = &labelEmbeddings.positiveCount
			default:
				panic("unknown sentiment")
			}

			wantedIdx := rng.Intn(v2PrimarySize)
			(*holder)[wantedIdx], err = tensor.Add((*holder)[wantedIdx], embeddingT)
			if err != nil {
				panic(err)
			}
			(*counter)[wantedIdx]++
		}
	}
	pr.ReadStop()
	fr.Close()

	averages := make(map[string][]tensor.Tensor)

	computeAverage_V2(&averages, "negative", labelEmbeddings.negative, labelEmbeddings.negativeCount)
	computeAverage_V2(&averages, "neutral", labelEmbeddings.neutral, labelEmbeddings.neutralCount)
	computeAverage_V2(&averages, "positive", labelEmbeddings.positive, labelEmbeddings.positiveCount)

	for label, primaryEmbeddings := range averages {
		for _, embedding := range primaryEmbeddings {
			storePrimaryEmbedding(state, label, embedding, lo.ToPtr("v2"))
		}
	}
	slog.Info("done")
}

func computeAverage_V2(averages *map[string][]tensor.Tensor, label string, summedTensors []tensor.Tensor, counters []float64) {
	for index, summedTensor := range summedTensors {
		count := counters[index]
		if count == 0 {
			slog.Error("no embeddings added for index", slog.Int("index", index))
			panic("no embeddings added for index")
		}
		averagedTensor, err := tensor.Div(summedTensor, count)
		if err != nil {
			panic(err)
		}
		(*averages)[label] = append((*averages)[label], averagedTensor)
	}
}
