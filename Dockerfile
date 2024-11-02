FROM golang:1.23-bookworm
RUN apt update
RUN apt install ca-certificates
ENV CGO_ENABLED=1
ADD go.mod /dotsrc/go.mod
ADD go.sum /dotsrc/go.sum

WORKDIR /dotsrc
RUN go mod download -x

ADD . /dotsrc
RUN go build -o /dotsrc/bskydot

FROM debian:bookworm
COPY --from=0 /dotsrc/bskydot /bskydot
CMD ["/bskydot"]
