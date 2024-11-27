# bsky-dot
the bluesky dot
https://dot.bsky.ln4.net

## how it work

- run firehose
- run sentiment analysis model, currently https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest
- store data for "dot" analysis
  - current algorithm is dot_v5.go, tldr is
  - if there's more sentiment (either positive or negative!), the dot value increases
  - if there's less sentiment (e.g higher neutral proportion, model gives positive/negative/neutral), dot value decreases
  - works via proportion so if there's more posts or less posts, it doesn't matter
  - every hour it resets its concept of "average" emotion so that it doesn't collapse to 1 or 0, it'll always try to push itself to 0.5, making for a funnier graph
  - its possible the current dot algorithm isnt even good!! it's a lot of trial-and-error for these things

## how

- one server runs firehose, processes data, etc
- multiple servers run sentiment analysis (either one with a gpu, or multiple with cpu)
  - atm you need enough compute to run firehose at line rate (as of Nov 2024, ~80 posts a second). i want to see if i can reduce compute needs by sampling from firehose
  - in example, a.com, b.com, c.com run the sentiment worker
  - b.com runs gpu so it can do a lot more (in this case, 100 sentiments at the same time)
  - im currently running some 10 VPSes on hetzner/scaleway plus my RTX4060, but i want to downscale things to remove the cost (and so my gpu is free for other projects, good thing i dont have other projects that need it at the moment :D)
  - in the future i want to bring it down to some 5 CPU nodes, and sample firehose down to 30~40posts/second. that should make it good enough i can just drop 20USD/mo in the project (the maximum amount i'm fine with spending on this sort of high-effort shitpost)

### running main service

```sh
export UPSTREAM_TYPE=bluesky
export DATABASE_PATH=awoo.db
export UPSTREAM_TYPE=bluesky
# golang lol etc
export ASSUME_NO_MOVING_GC_UNSAFE_RISK_IT_WITH=go1.23
export AUTH_TOKEN=AAAAAAAAAAAAAAAAAAAAA
export LLAMACPP_EMBEDDING_URL=https://a.com,https://b.com;100,https://c.com

go run . run
```

### sentiment worker (cpu)

```sh
python3 -m venv env

# if CPU:
env/bin/pip install torch==2.5.1+cpu torchvision==0.20.1+cpu torchaudio==2.5.1+cpu --index-url https://download.pytorch.org/whl/cpu

# if GPU:
env/bin/pip install torch==2.5.1 torchvision==0.20.1 torchaudio==2.5.1

env/bin/pip install -Ur testrunner-requirements.txt
export AUTH_TOKEN=AAAAAAAAAAAAAAAAAAAAA
env/bin/flask --app testrunner run --host 0.0.0.0 --port 4000
```
