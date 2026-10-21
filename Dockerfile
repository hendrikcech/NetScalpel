# The binaries are pure Go: no cgo, no CA bundle (the QUIC/TLS certificates are
# self-signed and verified off) and no shell tools. So build them statically and
# ship them on an empty base instead of a full Go image.

FROM golang:1.26 AS builder

WORKDIR /src

# Dependencies first: this layer survives source-only changes, which makes
# rebuilds after a commit only pay for compiling the code itself.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg
COPY internal ./internal

# -w -s drops the DWARF sections and the symbol table, -trimpath keeps the
# build reproducible and independent of the source location.
ENV CGO_ENABLED=0
ARG VERSION=2.0.0-dev
ARG RELEASE=
ARG REVISION=
ARG VCS_TIME=
ARG DIRTY=
RUN flags="-w -s -X github.com/hendrikcech/netscalpel/internal/buildinfo.Version=${VERSION} -X github.com/hendrikcech/netscalpel/internal/buildinfo.Release=${RELEASE} -X github.com/hendrikcech/netscalpel/internal/buildinfo.Revision=${REVISION} -X github.com/hendrikcech/netscalpel/internal/buildinfo.Time=${VCS_TIME} -X github.com/hendrikcech/netscalpel/internal/buildinfo.Dirty=${DIRTY}"; \
    go build -trimpath -ldflags="$flags" -o /out/ ./cmd/scalpel-run ./cmd/scalpel-exp

# No ENTRYPOINT or CMD, so the binaries stay directly runnable:
#   docker run --rm --cap-add=net_raw --cap-add=net_admin \
#     <image> scalpel-run --ip 10.0.0.2 udp-burst --num 1000 --pad 1400
# There is no shell in the image, so use --entrypoint to run another binary.
FROM scratch

COPY --from=builder /out/ /usr/local/bin/
