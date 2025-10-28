FROM golang:1.24 AS builder

WORKDIR /root

ADD cmd ./cmd
ADD pkg ./pkg
ADD go.mod go.sum .

RUN go build ./cmd/netmeas
RUN go build ./cmd/stltrace
RUN go build -o stltrace_noserver -tags=noserver -ldflags="-w -s" ./cmd/stltrace

FROM golang:1.24
COPY --from=builder /root/netmeas /root/stltrace /root/stltrace_noserver /usr/local/bin
