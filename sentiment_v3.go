package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	_ "github.com/mattn/go-sqlite3"
)

// v3 algorithm:
// - the cardiffnlp model i wanted to use in the first place lmao

func sentimentFromText_V3(cfg Config, text string) string {
	client := http.Client{}
	requestBody := make(map[string]any)
	requestBody["text"] = text
	requestBytes, err := json.Marshal(requestBody)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", cfg.embeddingUrl+"/api/v1/sentiment", bytes.NewReader(requestBytes))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
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
	resOutput := resJson["output"].(map[string]any)
	resPredictions := resOutput["predictions"].([]any)
	resLabel := resPredictions[0].(map[string]any)
	return resLabel["label"].(string)
}
