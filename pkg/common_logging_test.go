package pkg

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func preserveDefaultLogger(t *testing.T) {
	t.Helper()
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
}

func TestSetupSlogBasicToUsesWriterAndContextID(t *testing.T) {
	preserveDefaultLogger(t)
	var output bytes.Buffer
	SetupSlogBasicTo(slog.LevelInfo, &output)
	ctx := context.WithValue(context.Background(), SlogIDKey{}, slog.String("id", "basic-test"))
	slog.DebugContext(ctx, "filtered message")
	slog.InfoContext(ctx, "visible message")

	got := output.String()
	if strings.Contains(got, "filtered message") || !strings.Contains(got, "visible message") {
		t.Fatalf("basic logger output = %q, expected only the info message", got)
	}
	if !strings.Contains(got, `id=basic-test`) {
		t.Fatalf("basic logger omitted contextual request ID: %q", got)
	}
}

func TestSetupSlogMultiToPreservesFileAndChannelOutputs(t *testing.T) {
	preserveDefaultLogger(t)
	var console bytes.Buffer
	file, err := os.CreateTemp(t.TempDir(), "slog-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	channel := SetupSlogMultiTo(slog.LevelInfo, true, &console, file)
	if channel == nil {
		t.Fatal("createChan=true returned a nil channel")
	}
	ctx := context.WithValue(context.Background(), SlogIDKey{}, slog.String("id", "multi-test"))
	slog.DebugContext(ctx, "debug details")
	slog.InfoContext(ctx, "console summary")

	consoleOutput := console.String()
	if strings.Contains(consoleOutput, "debug details") || !strings.Contains(consoleOutput, "console summary") {
		t.Fatalf("console output = %q, expected info only", consoleOutput)
	}
	if !strings.Contains(consoleOutput, `id=multi-test`) {
		t.Fatalf("console output omitted contextual request ID: %q", consoleOutput)
	}

	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	fileOutput := string(fileBytes)
	if !strings.Contains(fileOutput, "debug details") || !strings.Contains(fileOutput, "console summary") || !strings.Contains(fileOutput, `id=multi-test`) {
		t.Fatalf("file output did not preserve debug/context behavior: %q", fileOutput)
	}

	for range 2 {
		select {
		case record := <-*channel:
			if record == nil {
				t.Fatal("channel received a nil record")
			}
			foundID := false
			record.Attrs(func(attr slog.Attr) bool {
				if attr.Key == "id" && attr.Value.String() == "multi-test" {
					foundID = true
				}
				return true
			})
			if !foundID {
				t.Fatalf("channel record omitted contextual request ID: %+v", record)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for channel log record")
		}
	}
}

func TestSetupSlogMultiToWithoutChannelReturnsNil(t *testing.T) {
	preserveDefaultLogger(t)
	var console bytes.Buffer
	if channel := SetupSlogMultiTo(slog.LevelInfo, false, &console); channel != nil {
		t.Fatal("createChan=false returned a channel")
	}
}
