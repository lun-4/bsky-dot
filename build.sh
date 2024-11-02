#!/bin/sh
docker build -t bskydot-sentiment:0.0.1 -f Dockerfile.sentiments .
docker build -t bskydot:0.0.1 .

