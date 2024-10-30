package main

import (
	"encoding/json"
	"math"
	"net/http"

	"github.com/samber/lo"
	"gorgonia.org/tensor"

	_ "github.com/mattn/go-sqlite3"
)

func sentimentFromText_V1(cfg Config, text string, primaryEmbeddings map[string]tensor.Tensor) string {
	embeddingData := getUpstreamEmbedding(cfg, http.Client{}, text)
	embeddingT := tensor.New(EMBEDDING_V1_SHAPE, tensor.WithBacking(embeddingData))
	maxLabel, maxDistance := "", math.Inf(1)
	for label, primaryEmbedding := range primaryEmbeddings {
		distance, err := CosineSimilarity(primaryEmbedding, embeddingT)
		if err != nil {
			panic(err)
		}
		if distance < maxDistance {
			maxLabel = label
			maxDistance = distance
		}
	}
	return maxLabel
}

func getPrimaryEmbeddings_V1(state *State) map[string]tensor.Tensor {
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
		primaryEmbeddingT := tensor.New(EMBEDDING_V1_SHAPE, tensor.WithBacking(embeddingF))
		primaryEmbeddings[label] = primaryEmbeddingT
	}
	return primaryEmbeddings
}
