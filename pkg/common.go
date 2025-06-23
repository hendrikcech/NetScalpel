package pkg

import (
	"bufio"
	"context"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func RegisterGob() {
	gob.Register(MsgRcvd{})
	gob.Register(MsgSent{})
	gob.Register(BurstParams{})
	gob.Register(RateParams{})
	gob.Register(RateParamsW{})
	gob.Register(PeriodicParams{})
	gob.Register(TcpdumpParams{})
}

func RandPath(suffix string) string {
	randBytes := make([]byte, 4)
	rand.Read(randBytes)
	return filepath.Join(os.TempDir(), hex.EncodeToString(randBytes)+"_"+suffix)
}

func RandDir(suffix string) (string, error) {
	path := RandPath(suffix)
	err := os.MkdirAll(path, os.ModePerm)
	return path, err
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
		return fmt.Errorf("Failed to open file %v: %v", path, err)
	}
	defer f.Close()
	fr := bufio.NewReader(f)

	enc, err := zstd.NewWriter(w)
	if err != nil {
		return fmt.Errorf("Failed to create compression writer for %v: %v", path, err)
	}

	// Feed the encoder with input
	if _, err := io.Copy(enc, fr); err != nil {
		_ = enc.Close()
		return fmt.Errorf("Failed to compress file %v: %v", path, err)
	}

	// Flush the last data from the encoder and close it
	if err := enc.Close(); err != nil {
		return fmt.Errorf("Failed to close the encoder %v: %v", path, err)
	}

	// Important: don't forget to call Flush on w

	return nil
}

// Decompress file
// f, err := os.Create(path)
// if err != nil {
// 	return fmt.Errorf("Failed os.Create(%v): %v", path, err.Error())
// }
// defer f.Close()
// fW := bufio.NewWriter(f)
// defer fW.Flush()

// encR := bytes.NewReader(bufEnc)
// dec, err := zstd.NewReader(encR)
// if err != nil {
// 	return fmt.Errorf("Failed creating decoder for %v: %v", path, err.Error())
// }
// defer dec.Close()

// nWritten, err := io.Copy(fW, dec)
// if err != nil {
// 	return fmt.Errorf("Failed writing returned file to %v: %v", path, err.Error())
// }

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
	switch direction {
	case "ul", "UL":
		return UL, nil
	case "dl", "DL":
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
