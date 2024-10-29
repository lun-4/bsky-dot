#!/bin/sh
set -eux
mkdir -p dataset
curl -L 'https://huggingface.co/datasets/cardiffnlp/tweet_eval/resolve/main/sentiment/train-00000-of-00001.parquet' --output dataset/train.parquet

