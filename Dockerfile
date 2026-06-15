FROM golang:1.24 AS builder

WORKDIR /root

ADD cmd ./cmd
ADD pkg ./pkg
ADD go.mod go.sum .

RUN go build ./cmd/scalpel-run
RUN go build ./cmd/scalpel-exp
RUN go build -o scalpel-exp_noserver -tags=noserver -ldflags="-w -s" ./cmd/scalpel-exp

FROM golang:1.24
COPY --from=builder /root/scalpel-run /root/scalpel-exp /root/scalpel-exp_noserver /usr/local/bin
