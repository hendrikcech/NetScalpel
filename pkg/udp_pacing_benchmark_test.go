package pkg

// Measurements, not timing assertions. See benchmarks/README.md. Never run in
// parallel or under -race: scheduler noise is part of the measured result.
import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"
)

type pacingBoundary struct {
	At      time.Time
	Seq     uint64
	Segment int
}

type pacingLog struct {
	slog.Handler
	sender          *RateSender
	indexed, legacy []pacingBoundary
	kernel          bool
	invalid         string
}

func (h *pacingLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *pacingLog) Handle(_ context.Context, r slog.Record) error {
	switch r.Message {
	case "RateSender segment start":
		segment := -1
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "segment" && a.Value.Kind() == slog.KindInt64 {
				segment = int(a.Value.Int64())
			}
			return true
		})
		if segment < 0 {
			h.invalid = "indexed start record lacks integer segment"
		}
		h.indexed = append(h.indexed, pacingBoundary{r.Time, h.sender.seq, segment})
	case "Dynamic interval calculation":
		h.legacy = append(h.legacy, pacingBoundary{r.Time, h.sender.seq, -1})
	case "selectMsgsSent: returning with txTsReader infos":
		h.kernel = true
	}
	return nil
}

// Legacy logs carry no index. Their prefix interpretation is opt-in ONLY for
// the pre-fix sender whose loop never skips an interior segment. New senders
// must use indexed records; missing/skipped segments are represented explicitly.
func (h *pacingLog) boundaries(params RateParamsW, legacy bool) ([]pacingBoundary, error) {
	if h.invalid != "" {
		return nil, fmt.Errorf("%s", h.invalid)
	}
	bs := h.indexed
	if len(bs) == 0 {
		if !legacy {
			return nil, fmt.Errorf("missing indexed segment observer (baseline only: NETSCALPEL_PACING_LEGACY_PREFIX=1)")
		}
		bs = append([]pacingBoundary(nil), h.legacy...)
		active := 0
		for i, p := range params {
			if p.Pps != 0 && active < len(bs) {
				bs[active].Segment = i
				active++
			}
		}
		if active != len(bs) {
			return nil, fmt.Errorf("too many legacy segment records")
		}
	}
	last := -1
	for i, b := range bs {
		if b.Segment <= last || b.Segment >= len(params) {
			return nil, fmt.Errorf("invalid segment index %d", b.Segment)
		}
		if i > 0 && (b.Seq < bs[i-1].Seq || b.At.Before(bs[i-1].At)) {
			return nil, fmt.Errorf("nonmonotonic segment records")
		}
		last = b.Segment
	}
	if len(bs) == 0 {
		return nil, fmt.Errorf("no observed segments")
	}
	return bs, nil
}

type pacingCase struct {
	name   string
	params RateParamsW
	capped bool
}

func pacingCases() []pacingCase {
	// Keep the original study's UDP accounting: 1400 padding + 8 sequence
	// bytes. CLI rates divide by 1400 instead, and are not interchangeable.
	segment := func(mbps uint, d time.Duration) RateParams {
		return RateParams{Pps: mbps * 1_000_000 / 8 / 1408, Duration: d, PayloadSize: 1400}
	}
	var d2, short RateParamsW
	for range 50 {
		d2 = append(d2, segment(500, 8*time.Millisecond), segment(0, 12*time.Millisecond))
	}
	d2 = append(d2, segment(500, 600*time.Millisecond))
	for range 12 {
		short = append(short, RateParams{Pps: 7, Duration: 100 * time.Millisecond, PayloadSize: 1400})
	}
	return []pacingCase{
		{"single500", RateParamsW{segment(500, 1200*time.Millisecond)}, true},
		{"single100", RateParamsW{segment(100, 1200*time.Millisecond)}, true},
		{"equal500", RateParamsW{segment(500, 600*time.Millisecond), segment(500, 600*time.Millisecond)}, true},
		{"equal100", RateParamsW{segment(100, 600*time.Millisecond), segment(100, 600*time.Millisecond)}, true},
		{"up100to500", RateParamsW{segment(100, 600*time.Millisecond), segment(500, 600*time.Millisecond)}, true},
		{"down500to100", RateParamsW{segment(500, 600*time.Millisecond), segment(100, 600*time.Millisecond)}, true},
		{"d2_capped", d2, true}, {"d2_uncapped", d2, false},
		{"single7pps", RateParamsW{{Pps: 7, Duration: 1200 * time.Millisecond, PayloadSize: 1400}}, true},
		{"split7pps", RateParamsW{{Pps: 7, Duration: 600 * time.Millisecond, PayloadSize: 1400}, {Pps: 7, Duration: 600 * time.Millisecond, PayloadSize: 1400}}, true},
		{"short7pps", short, true},
	}
}

type pacingPhase struct {
	Segment         int      `json:"segment"`
	Observed        bool     `json:"observed"`
	NominalMS       float64  `json:"nominal_ms"`
	DurationMS      float64  `json:"duration_ms"`
	PPS             uint     `json:"pps"`
	ExpectedPackets uint     `json:"expected_packets"`
	StartMS         float64  `json:"start_ms"`
	DriftMS         float64  `json:"drift_ms"`
	FirstMS         float64  `json:"first_ms"`
	LastMS          float64  `json:"last_ms"`
	SpanMS          float64  `json:"span_ms"`
	Packets         int      `json:"packets"`
	BoundaryGapUS   *float64 `json:"boundary_gap_us"`
	Mbps            float64  `json:"mbps"`
	AchievedPPS     float64  `json:"achieved_pps"`
	BudgetMS        float64  `json:"budget_ms"`
}

type pacingResult struct {
	Schema         int            `json:"schema"`
	Case           string         `json:"case"`
	Kernel         bool           `json:"kernel"`
	Packets        int            `json:"packets"`
	Phases         []pacingPhase  `json:"phases"`
	BaselineGapsUS []float64      `json:"baseline_gaps_us"`
	GapMedianUS    float64        `json:"gap_median_us"`
	GapP99US       float64        `json:"gap_p99_us"`
	GapStddevUS    float64        `json:"gap_stddev_us"`
	Metadata       map[string]any `json:"metadata"`
	CSV            string         `json:"csv"`
}

func pacingMetadata(tb testing.TB) map[string]any {
	tb.Helper()
	git := func(args ...string) string {
		b, err := exec.Command("git", args...).Output()
		if err != nil {
			tb.Fatalf("git provenance: %v", err)
		}
		return string(b)
	}
	source := os.Getenv("NETSCALPEL_PACING_SOURCE")
	if source == "" {
		source = "udp.go"
	} // go test executes in pkg/
	data, err := os.ReadFile(source)
	if err != nil {
		tb.Fatal(err)
	}
	benchmark, err := os.ReadFile("udp_pacing_benchmark_test.go")
	if err != nil {
		tb.Fatal(err)
	}
	kernel, err := exec.Command("uname", "-srvm").Output()
	if err != nil {
		tb.Fatal(err)
	}
	return map[string]any{
		"created_utc": time.Now().UTC().Format(time.RFC3339Nano), "go_version": runtime.Version(),
		"goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "cpus": runtime.NumCPU(),
		"git_revision": git("rev-parse", "HEAD"), "git_status": git("status", "--porcelain"),
		"source_path": source, "source_sha256": fmt.Sprintf("%x", sha256.Sum256(data)),
		"benchmark_sha256": fmt.Sprintf("%x", sha256.Sum256(benchmark)),
		"kernel_version":   string(kernel), "argv": os.Args, "label": os.Getenv("NETSCALPEL_PACING_LABEL"),
		"go_environment": map[string]string{"GOFLAGS": os.Getenv("GOFLAGS"), "GOGC": os.Getenv("GOGC"), "GOMEMLIMIT": os.Getenv("GOMEMLIMIT")},
		"legacy_prefix":  os.Getenv("NETSCALPEL_PACING_LEGACY_PREFIX") == "1", "payload_bytes": 1408,
		"timestamp_source": "complete kernel software TX", "suite_version": 1,
	}
}

func BenchmarkRateSender(b *testing.B) {
	filter := os.Getenv("NETSCALPEL_PACING_CASE")
	cases := pacingCases()
	if filter != "" {
		found := false
		for _, c := range cases {
			found = found || c.name == filter
		}
		if !found {
			b.Fatalf("unknown NETSCALPEL_PACING_CASE %q", filter)
		}
	}
	for _, c := range cases {
		if filter != "" && filter != c.name {
			continue
		}
		b.Run(c.name, func(b *testing.B) {
			var drift, span, packets, gap, jitter, rate float64
			var observed, gaps int
			for range b.N {
				b.StopTimer()
				metadata := pacingMetadata(b)
				b.StartTimer()
				r := pacingRun(b, c)
				b.StopTimer()
				r.Metadata = metadata
				r.Metadata["capped"] = c.capped
				r.Metadata["start_unix_ns"] = r.start.UnixNano()
				if dir := os.Getenv("NETSCALPEL_PACING_STUDY"); dir != "" {
					pacingSave(b, dir, r.pacingResult, r.msgs, r.start, r.segments)
				}
				last := r.Phases[len(r.Phases)-1]
				if last.Observed {
					observed++
					drift += last.DriftMS
					span += last.SpanMS
					rate += last.Mbps
				}
				packets += float64(r.Packets)
				jitter += r.GapStddevUS
				if last.BoundaryGapUS != nil {
					gaps++
					gap += *last.BoundaryGapUS
				}
			}
			n := float64(b.N)
			b.ReportMetric(float64(observed)/n, "final-observed/op")
			if observed > 0 {
				b.ReportMetric(drift/float64(observed), "final-drift-ms")
				b.ReportMetric(span/float64(observed), "final-span-ms")
				b.ReportMetric(rate/float64(observed), "final-Mbps")
			}
			b.ReportMetric(packets/n, "packets/op")
			if gaps > 0 {
				b.ReportMetric(gap/float64(gaps), "final-gap-us")
			}
			b.ReportMetric(jitter/n, "gap-stddev-us")
		})
	}
}

type pacingMeasurement struct {
	*pacingResult
	msgs     []MsgSent
	start    time.Time
	segments []int
}

func pacingRun(tb testing.TB, c pacingCase) pacingMeasurement {
	tb.Helper()
	recv, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		tb.Fatal(err)
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		buf := make([]byte, 1500)
		for {
			if _, _, err := recv.ReadFromUDP(buf); err != nil {
				return
			}
		}
	}()
	defer func() { recv.Close(); <-drained }()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		tb.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadBuffer(30_000_000); err != nil {
		tb.Fatal(err)
	}
	sender := &RateSender{Params: c.params}
	h := &pacingLog{Handler: slog.NewTextHandler(io.Discard, nil), sender: sender}
	old := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(old)
	limit := c.params.GetDuration()
	if !c.capped {
		limit += time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	start := time.Now()
	res, err := sender.Run(ctx, conn, recv.LocalAddr())
	if err != nil {
		tb.Fatal(err)
	}
	msgs := res.([]MsgSent)
	if err := pacingValidateTX(h.kernel, msgs, sender.seq); err != nil {
		tb.Fatal(err)
	}
	bs, err := h.boundaries(c.params, os.Getenv("NETSCALPEL_PACING_LEGACY_PREFIX") == "1")
	if err != nil {
		tb.Fatal(err)
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	r := pacingResult{Schema: 1, Case: c.name, Kernel: true, Packets: len(msgs)}
	segments := make([]int, len(msgs))
	for i := range segments {
		segments[i] = -1
	}
	var nominal time.Duration
	for i, p := range c.params {
		phase := pacingPhase{Segment: i, NominalMS: ms(nominal), DurationMS: ms(p.Duration), PPS: p.Pps, ExpectedPackets: p.NumPackets()}
		for j, boundary := range bs {
			if boundary.Segment != i {
				continue
			}
			endSeq := uint64(len(msgs))
			if j+1 < len(bs) {
				endSeq = bs[j+1].Seq
			}
			if boundary.Seq > endSeq || endSeq > uint64(len(msgs)) {
				tb.Fatal("segment sequence outside complete TX records")
			}
			phase.Observed = true
			phase.StartMS = ms(boundary.At.Sub(start))
			phase.DriftMS = ms(boundary.At.Sub(start) - nominal)
			phase.Packets = int(endSeq - boundary.Seq)
			for seq := boundary.Seq; seq < endSeq; seq++ {
				segments[seq] = i
			}
			// Record the same budget for observed empty phases, too. An
			// unobserved phase keeps its zero fields and Observed=false.
			end := start.Add(limit)
			if j+1 < len(bs) {
				end = bs[j+1].At
			}
			budget := pacingPhaseBudget(p.Duration, boundary.At, end)
			phase.BudgetMS = ms(budget)
			if endSeq > boundary.Seq {
				first, last := msgs[boundary.Seq].TsSent, msgs[endSeq-1].TsSent
				phase.FirstMS, phase.LastMS, phase.SpanMS = ms(first.Sub(start)), ms(last.Sub(start)), ms(last.Sub(first))
				if budget <= 0 {
					tb.Fatal("packets observed outside segment budget")
				}
				phase.AchievedPPS = float64(phase.Packets) / budget.Seconds()
				phase.Mbps = phase.AchievedPPS * 1408 * 8 / 1e6
				if boundary.Seq > 0 {
					gap := float64(first.Sub(msgs[boundary.Seq-1].TsSent)) / 1e3
					phase.BoundaryGapUS = &gap
				}
			}
		}
		r.Phases = append(r.Phases, phase)
		nominal += p.Duration
	}
	for _, segment := range segments {
		if segment < 0 {
			tb.Fatal("TX packet not mapped to an observed segment")
		}
	}
	var gaps []float64
	for i := 1; i < len(msgs); i++ {
		gaps = append(gaps, float64(msgs[i].TsSent.Sub(msgs[i-1].TsSent))/1e3)
	}
	r.GapMedianUS, r.GapP99US, r.GapStddevUS = pacingGapStats(gaps)
	if c.name == "single500" || c.name == "single100" {
		for boundary := 100 * time.Millisecond; boundary <= 1100*time.Millisecond; boundary += 20 * time.Millisecond {
			at := start.Add(boundary)
			for i := 1; i < len(msgs); i++ {
				if !msgs[i].TsSent.Before(at) && msgs[i-1].TsSent.Before(at) {
					r.BaselineGapsUS = append(r.BaselineGapsUS, float64(msgs[i].TsSent.Sub(msgs[i-1].TsSent))/1e3)
					break
				}
			}
		}
	}
	return pacingMeasurement{pacingResult: &r, msgs: msgs, start: start, segments: segments}
}

func pacingPhaseBudget(duration time.Duration, start, end time.Time) time.Duration {
	return max(0, min(duration, end.Sub(start)))
}

func pacingGapStats(gaps []float64) (median, p99, stddev float64) {
	if len(gaps) == 0 {
		return
	}
	x := append([]float64(nil), gaps...)
	sort.Float64s(x)
	median = x[len(x)/2]
	if len(x)%2 == 0 {
		median = (x[len(x)/2-1] + median) / 2
	}
	p99 = x[int(math.Ceil(float64(len(x))*.99))-1]
	var mean float64
	for _, g := range x {
		mean += g
	}
	mean /= float64(len(x))
	for _, g := range x {
		stddev += (g - mean) * (g - mean)
	}
	stddev = math.Sqrt(stddev / float64(len(x)))
	return
}

func pacingValidateTX(kernel bool, msgs []MsgSent, sent uint64) error {
	if !kernel {
		return fmt.Errorf("incomplete/unavailable kernel TX timestamps; refusing application fallback")
	}
	if uint64(len(msgs)) != sent {
		return fmt.Errorf("kernel TX count %d != sender sequence %d", len(msgs), sent)
	}
	for i, m := range msgs {
		if m.Seq != uint64(i) || m.TsSent.IsZero() || m.TsSent.UnixNano() <= 0 || m.Len != 1408 {
			return fmt.Errorf("invalid TX record %d: %+v", i, m)
		}
		if i > 0 && m.TsSent.Before(msgs[i-1].TsSent) {
			return fmt.Errorf("nonmonotonic kernel TX timestamp at %d", i)
		}
	}
	return nil
}

func pacingSave(tb testing.TB, dir string, r *pacingResult, msgs []MsgSent, start time.Time, segments []int) {
	tb.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		tb.Fatal(err)
	}
	f, err := os.CreateTemp(dir, r.Case+"-*.csv")
	if err != nil {
		tb.Fatal(err)
	}
	r.CSV = filepath.Base(f.Name())
	w := csv.NewWriter(f)
	w.Write([]string{"seq", "kernel_tx_unix_ns", "elapsed_ns", "segment", "udp_bytes"})
	for i, m := range msgs {
		w.Write([]string{strconv.FormatUint(m.Seq, 10), strconv.FormatInt(m.TsSent.UnixNano(), 10), strconv.FormatInt(m.TsSent.Sub(start).Nanoseconds(), 10), strconv.Itoa(segments[i]), strconv.FormatUint(uint64(m.Len), 10)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		tb.Fatal(err)
	}
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(f.Name()+".json", data, 0644); err != nil {
		tb.Fatal(err)
	}
}

func TestPacingObserver(t *testing.T) {
	params := RateParamsW{{Pps: 7}, {Pps: 7}, {Pps: 7}}
	h := pacingLog{indexed: []pacingBoundary{{Segment: 0}, {Segment: 2}}}
	bs, err := h.boundaries(params, false)
	if err != nil || len(bs) != 2 || bs[1].Segment != 2 {
		t.Fatalf("skipped segment mapping: %v %v", bs, err)
	}
	h.indexed = nil
	h.legacy = []pacingBoundary{{}, {}}
	if _, err := h.boundaries(params, false); err == nil {
		t.Fatal("silent legacy fallback")
	}
	bs, err = h.boundaries(params, true)
	if err != nil || bs[1].Segment != 1 {
		t.Fatalf("explicit baseline prefix: %v %v", bs, err)
	}
	h.indexed = []pacingBoundary{{Segment: 2}, {Segment: 1}}
	if _, err := h.boundaries(params, false); err == nil {
		t.Fatal("accepted reversed segments")
	}
}

func TestPacingGapStats(t *testing.T) {
	median, p99, sd := pacingGapStats([]float64{3, 1, 2, 4})
	if median != 2.5 || p99 != 4 || math.Abs(sd-math.Sqrt(1.25)) > 1e-9 {
		t.Fatalf("%v %v %v", median, p99, sd)
	}
	if m, p, s := pacingGapStats(nil); m != 0 || p != 0 || s != 0 {
		t.Fatal("empty gap stats")
	}
}

func TestPacingPhaseBudget(t *testing.T) {
	start := time.Now()
	for _, c := range []struct{ remaining, want time.Duration }{
		{time.Second, 100 * time.Millisecond}, {50 * time.Millisecond, 50 * time.Millisecond}, {-time.Millisecond, 0},
	} {
		if got := pacingPhaseBudget(100*time.Millisecond, start, start.Add(c.remaining)); got != c.want {
			t.Fatalf("budget with remaining %v: got %v, want %v", c.remaining, got, c.want)
		}
	}
}

func TestPacingValidateTX(t *testing.T) {
	now := time.Now()
	msgs := []MsgSent{{Seq: 0, TsSent: now, Len: 1408}, {Seq: 1, TsSent: now.Add(time.Microsecond), Len: 1408}}
	if err := pacingValidateTX(true, msgs, 2); err != nil {
		t.Fatal(err)
	}
	if err := pacingValidateTX(false, msgs, 2); err == nil {
		t.Fatal("accepted application fallback")
	}
	if err := pacingValidateTX(true, msgs, 3); err == nil {
		t.Fatal("accepted incomplete timestamps")
	}
	msgs[1].Seq = 3
	if err := pacingValidateTX(true, msgs, 2); err == nil {
		t.Fatal("accepted sequence hole")
	}
	msgs[1].Seq = 1
	msgs[1].TsSent = now.Add(-time.Second)
	if err := pacingValidateTX(true, msgs, 2); err == nil {
		t.Fatal("accepted reversed timestamps")
	}
	msgs[1].TsSent = time.Time{}
	if err := pacingValidateTX(true, msgs, 2); err == nil {
		t.Fatal("accepted missing timestamp")
	}
}

func TestPacingMetadata(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source.go")
	data := []byte("benchmark provenance sentinel")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NETSCALPEL_PACING_SOURCE", source)
	t.Setenv("NETSCALPEL_PACING_LABEL", "unit-test")
	m := pacingMetadata(t)
	if m["suite_version"] != 1 || m["go_version"] != runtime.Version() || m["label"] != "unit-test" {
		t.Fatalf("invalid version metadata: %+v", m)
	}
	if m["source_sha256"] != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatal("source hash mismatch")
	}
	if len(m["benchmark_sha256"].(string)) != 64 || len(m["git_revision"].(string)) < 40 {
		t.Fatal("missing build provenance")
	}
	if _, ok := m["git_status"]; !ok {
		t.Fatal("missing dirty status")
	}
}

func TestPacingSave(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	r := pacingResult{Schema: 1, Case: "single7pps", Kernel: true, Packets: 1, Metadata: map[string]any{"label": "fixture"}}
	pacingSave(t, dir, &r, []MsgSent{{Seq: 0, TsSent: start, Len: 1408}}, start, []int{0})
	data, err := os.ReadFile(filepath.Join(dir, r.CSV+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved pacingResult
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.CSV != r.CSV || saved.Metadata["label"] != "fixture" {
		t.Fatal("lost artifact provenance")
	}
	f, err := os.Open(filepath.Join(dir, r.CSV))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1][0] != "0" || rows[1][3] != "0" || rows[1][4] != "1408" {
		t.Fatalf("invalid raw records: %v", rows)
	}
}
