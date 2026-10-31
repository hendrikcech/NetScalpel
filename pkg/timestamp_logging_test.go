package pkg

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestTimestampImplementationDowngradeLogging(t *testing.T) {
	previous := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	if _, err := selectTimestampSource(context.Background(), TimestampHardware, true); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"requested_source=hardware", "selected_source=software"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("log missing %q: %s", want, output.String())
		}
	}
	output.Reset()
	for _, source := range []TimestampSource{TimestampSoftware, TimestampHardware, TimestampApp} {
		if _, err := selectTimestampSource(context.Background(), source, false); err != nil {
			t.Fatal(err)
		}
	}
	if output.Len() != 0 {
		t.Fatalf("supported sources logged as downgrades: %s", output.String())
	}
}
