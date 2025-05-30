package pkg

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

func RegisterGob() {
	gob.Register(MsgRcvd{})
	gob.Register(MsgSent{})
	gob.Register(BurstParams{})
	gob.Register(RateParams{})
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

func waitUntil(startAt time.Time) error {
	if !startAt.IsZero() {
		now := time.Now()
		if now.After(startAt) {
			return fmt.Errorf("StartAt %v already passed by: %v", startAt, now)
		}
		sleepFor := startAt.Sub(now)
		// log.Printf("Sleep for %.2f s before starting sender", sleepFor.Seconds())
		time.Sleep(sleepFor)
	}
	return nil
}

func CompressFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file %v: %v", path, err)
	}
	defer f.Close()
	fr := bufio.NewReader(f)

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	enc, err := zstd.NewWriter(w)
	if err != nil {
		return nil, fmt.Errorf("Failed to create compression writer for %v: %v", path, err)
	}

	// Feed the encoder with input
	if _, err := io.Copy(enc, fr); err != nil {
		_ = enc.Close()
		return nil, fmt.Errorf("Failed to compress file %v: %v", path, err)
	}

	// Flush the last data from the encoder and close it
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("Failed to close the encoder %v: %v", path, err)
	}

	// Important!
	w.Flush()

	return buf.Bytes(), nil
}
