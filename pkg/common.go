package pkg

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"log/slog"
	"math/rand"
	randv2 "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mikioh/tcpinfo"
	slogchannel "github.com/samber/slog-channel"
	slogmulti "github.com/samber/slog-multi"
)

func RegisterGob() {
	gob.Register(MsgRcvd{})
	gob.Register(MsgSent{})
	gob.Register([]MsgRcvd{})
	gob.Register([]MsgSent{})
	gob.Register(BurstParams{})
	gob.Register(RateParams{})
	gob.Register(RateParamsW{})
	gob.Register(PeriodicParams{})
	gob.Register(TcpdumpParams{})
	gob.Register(QUICParams{})
	gob.Register(TCPSenderParams{})
	gob.Register(TCPMetric{})
	gob.Register([]TCPMetric{})
	gob.Register(tcpinfo.WindowScale(0))
	gob.Register(tcpinfo.SACKPermitted(false))
	gob.Register(tcpinfo.Timestamps(false))
	gob.Register(ICMPParams{})
}

// UDP packet
type Msg struct {
	Seq  uint64
	PadN uint
}

// Stores sent and received messages
type MsgSent struct {
	Seq    uint64
	TsSent time.Time
	Len    uint
}
type MsgRcvd struct {
	Seq    uint64
	TsRcvd time.Time
	Len    uint
}

func (m *Msg) Encode(buf []byte) (int, error) {
	if len(buf) < int(8+m.PadN) {
		return 0, fmt.Errorf("Provided buffer of %v bytes too small for seq and %v padding bytes", len(buf), m.PadN)
	}
	binary.BigEndian.PutUint64(buf[0:], m.Seq)
	fillRandom(buf[8 : 8+m.PadN])
	return int(8 + m.PadN), nil
}

// fillRandom fills b with non-cryptographic random padding (so links cannot
// compress it away). math/rand/v2's global source is goroutine-safe and
// cheap enough for the send hot path, unlike crypto/rand.
func fillRandom(b []byte) {
	for len(b) >= 8 {
		binary.LittleEndian.PutUint64(b, randv2.Uint64())
		b = b[8:]
	}
	if len(b) > 0 {
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], randv2.Uint64())
		copy(b, tail[:])
	}
}

func (m *Msg) Decode(buf []byte) {
	m.Seq = binary.BigEndian.Uint64(buf[0:])
}

func listenUDP(ctx context.Context) (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, fmt.Errorf("net.ResolveUDPAddr failed: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("net.ListenUDP failed: %w", err)
	}

	setSocketBuffers(ctx, conn)

	return conn, nil
}

func listenTCP(ctx context.Context) (*net.TCPListener, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return nil, fmt.Errorf("net.ResolveTCPAddr failed: %w", err)
	}
	// TODO: set socket buffers?
	return net.ListenTCP("tcp", addr)
}

func listenICMP(ctx context.Context) (*net.IPConn, error) {
	addr, err := net.ResolveIPAddr("ip4", "0.0.0.0")
	if err != nil {
		return nil, fmt.Errorf("net.ResolveIPAddr failed: %w", err)
	}
	return net.ListenIP("ip4:icmp", addr)
}

// RandDir creates a unique temporary directory whose name starts with suffix
// and returns its path. The directory holds per-test results and is removed,
// together with its files, once the results are retrieved
// (CommandClient.Gather locally, RequestRunCommandResult on the server).
// It is made world-writable because commands may create files in it with a
// different identity than this process (tcpdump started via sudo can drop
// root privileges before opening its savefile).
func RandDir(suffix string) (string, error) {
	path, err := os.MkdirTemp("", suffix+"_")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(path, os.ModePerm); err != nil {
		os.RemoveAll(path)
		return "", err
	}
	return path, nil
}

func waitUntil(ctx context.Context, startAt time.Time) error {
	if !startAt.IsZero() {
		now := time.Now()
		if now.After(startAt) {
			return fmt.Errorf("StartAt %v already passed by: %v", startAt, now)
		}
		sleepFor := time.Until(startAt)
		// log.Printf("Sleep for %.2f s before starting sender", sleepFor.Seconds())
		select {
		case <-time.After(sleepFor):
		case <-ctx.Done():
		}
	}
	return ctx.Err()
}

func CompressFile(path string, w io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("Failed to open file %v: %w", path, err)
	}
	defer f.Close()
	fr := bufio.NewReader(f)

	enc, err := zstd.NewWriter(w)
	if err != nil {
		return fmt.Errorf("Failed to create compression writer for %v: %w", path, err)
	}

	// Feed the encoder with input
	if _, err := io.Copy(enc, fr); err != nil {
		_ = enc.Close()
		return fmt.Errorf("Failed to compress file %v: %w", path, err)
	}

	// Flush the last data from the encoder and close it
	if err := enc.Close(); err != nil {
		return fmt.Errorf("Failed to close the encoder %v: %w", path, err)
	}

	// Important: don't forget to call Flush on w

	return nil
}

type Direction int

const (
	UL Direction = iota
	DL
)

func (d Direction) String() string {
	switch d {
	case UL:
		return "UL"
	case DL:
		return "DL"
	default:
		panic("Unknown Direction enum type")
	}
}

func (d Direction) StringLower() string {
	return strings.ToLower(d.String())
}

func ParseDirection(direction string) (Direction, error) {
	switch strings.ToLower(direction) {
	case "ul":
		return UL, nil
	case "dl":
		return DL, nil
	default:
		return 999, fmt.Errorf("Unknown Direction value '%s'", direction)
	}
}

// slog attr key for request id
type SlogIDKey struct{}

type SlogContextHandler struct {
	slog.Handler
	Keys []any
}

func (h SlogContextHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(h.observe(ctx)...)
	return h.Handler.Handle(ctx, r)
}

func (h SlogContextHandler) observe(ctx context.Context) (as []slog.Attr) {
	for _, k := range h.Keys {
		a, ok := ctx.Value(k).(slog.Attr)
		if !ok {
			continue
		}
		a.Value = a.Value.Resolve()
		as = append(as, a)
	}
	return
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GenID(content string) string {
	b := make([]rune, 12)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return fmt.Sprintf("%s_%s_%s", time.Now().Format("20060102T150405"), content, string(b))
}

func SetupSlogBasic(level slog.Level) {
	// enc := slog.NewJSONHandler(os.Stdout, nil)
	enc := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	log := slog.New(SlogContextHandler{enc, []any{
		SlogIDKey{},
	}})
	slog.SetDefault(log)
}

func SetupSlogMulti(stdoutLevel slog.Level, createChan bool, fs ...*os.File) *chan *slog.Record {
	var handlers []slog.Handler

	for i := range fs {
		if fs[i] == nil {
			continue
		}
		fileHandler := SlogContextHandler{slog.NewTextHandler(fs[i], &slog.HandlerOptions{
			Level:       slog.LevelDebug,
			AddSource:   true,
			ReplaceAttr: slogShortenSource,
		}), []any{
			SlogIDKey{},
		}}
		handlers = append(handlers, fileHandler)
	}

	stdHandler := SlogContextHandler{slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     stdoutLevel,
	}), []any{
		SlogIDKey{},
	}}
	handlers = append(handlers, stdHandler)

	var ch *chan *slog.Record
	if createChan {
		tmp := make(chan *slog.Record, 1000)
		ch = &tmp
		chHandler := slogchannel.Option{
			Channel:     *ch,
			Blocking:    false,
			Level:       slog.LevelDebug,
			AddSource:   true,
			ReplaceAttr: slogShortenSource,
		}.NewChannelHandler()

		chHandler = SlogContextHandler{chHandler, []any{
			SlogIDKey{},
		}}

		handlers = append(handlers, chHandler)
	}

	logger := slog.New(
		slogmulti.Fanout(
			handlers...,
		),
	)

	slog.SetDefault(logger)

	return ch
}

// Only keep the filename and not the full path
func slogShortenSource(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.SourceKey {
		source, _ := a.Value.Any().(*slog.Source)
		if source != nil {
			source.File = filepath.Base(source.File)
		}
	}
	return a
}
