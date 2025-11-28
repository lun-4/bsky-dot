import os
from transformers import AutoModelForSequenceClassification
from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax
from flask import Flask, request

app = Flask(__name__)

auth_token = os.getenv("AUTH_TOKEN")


# Preprocess text (username and link placeholders)
def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = "@user" if t.startswith("@") and len(t) > 1 else t
        t = "http" if t.startswith("http") else t
        new_text.append(t)
    return " ".join(new_text)


MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
# tokenizer = AutoTokenizer.from_pretrained(MODEL)
# config = AutoConfig.from_pretrained(MODEL)
# model = AutoModelForSequenceClassification.from_pretrained(MODEL)

from transformers import pipeline

sentiment_task = pipeline("sentiment-analysis", model=MODEL, tokenizer=MODEL)


@app.get("/v1/models")
def models():
    return {
        "object": "list",
        "data": [
            {
                "id": "cardiffnlp/twitter-roberta-base-sentiment-latest",
            }
        ],
    }


def _old(j):
    text = j["text"]
    text = preprocess(text)
    encoded_input = tokenizer(text, return_tensors="pt")
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    ranking = np.argsort(scores)
    ranking = ranking[::-1]
    max_label, max_score = None, 0

    for i in range(scores.shape[0]):
        l = config.id2label[ranking[i]]
        s = scores[ranking[i]]
        if max_label is None:
            max_label = l
            max_score = s
        if s > max_score:
            max_label = l
            max_score = s
        print(f"{i+1}) {l} {np.round(float(s), 4)}")
    return max_label, max_score


@app.post("/api/v1/sentiment")
def compute_sentiment():
    if auth_token:
        try:
            auth_value = request.headers["Authorization"]
        except KeyError:
            return "unauthorized (missing token)", 401

        if not auth_value.startswith("Bearer "):
            return "unauthorized (not bearer token)", 401

        auth_given_token = auth_value.lstrip("Bearer ")
        if auth_token and auth_given_token != auth_token:
            print(auth_given_token, "!=", auth_token)
            return "unauthorized (invalid token)", 401

    j = request.get_json()

    # Support both single text and batched texts
    if "texts" in j:
        # Batch mode
        texts = j["texts"]
        is_batch = True
    elif "text" in j:
        # Single text mode (backwards compatible)
        texts = [j["text"]]
        is_batch = False
    else:
        return "missing 'text' or 'texts' field", 400

    print(f"Processing {len(texts)} text(s)")

    # Process all texts in one batch
    output = sentiment_task(texts)
    print(output)

    # Process results - output is a list of results
    results = []
    for result in output:
        # Each result is either a dict or a list of dicts
        if isinstance(result, list):
            # If result is a list, find max score
            max_label, max_score = None, 0
            for it in result:
                l, s = it["label"], it["score"]
                if max_label is None or s > max_score:
                    max_label = l
                    max_score = s
            results.append({"label": max_label, "score": max_score})
        else:
            # Single result dict
            results.append({"label": result["label"], "score": result["score"]})

    if is_batch:
        # Return array of predictions for batch mode
        return {"output": {"predictions": results}}
    else:
        # Return single prediction for backwards compatibility
        return {"output": {"predictions": [results[0]]}}
