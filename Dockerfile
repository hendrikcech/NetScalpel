FROM golang:1.24 AS builder

WORKDIR /root

ADD cmd ./cmd
ADD pkg ./pkg
ADD go.mod go.sum .

RUN go build ./cmd/netmeas
RUN go build ./cmd/stltrace

FROM golang:1.24
COPY --from=builder /root/netmeas /root/stltrace /usr/local/bin
