package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	_ "github.com/mattn/go-sqlite3"
)

// v3 algorithm:
// - the cardiffnlp model i wanted to use in the first place lmao

type StatusCodeError struct {
	StatusCode int
}

func (p StatusCodeError) Error() string {
	return fmt.Sprintf("status code error: %d", p.StatusCode)
}

func sentimentFromText_V3(cfg Config, text string) (string, error) {
	client := http.Client{}
	requestBody := make(map[string]any)
	requestBody["text"] = text
	requestBytes, err := json.Marshal(requestBody)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", cfg.embeddingUrl+"/api/v1/sentiment", bytes.NewReader(requestBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	responseBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		slog.Error("HTTP error", slog.Int("status", res.StatusCode), slog.String("response", string(responseBytes)))
		return "", StatusCodeError{StatusCode: res.StatusCode}
	}

	var resJson map[string]any
	err = json.Unmarshal(responseBytes, &resJson)
	if err != nil {
		return "", fmt.Errorf("failed to parse response JSON (%v): %v", string(responseBytes), err)
	}
	resOutput := resJson["output"].(map[string]any)
	resPredictions := resOutput["predictions"].([]any)
	resLabel := resPredictions[0].(map[string]any)
	return resLabel["label"].(string), nil
}
