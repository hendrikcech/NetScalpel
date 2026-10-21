package experiment

import (
	"context"
	"encoding/json"
	"errors"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/pkg"
)

type lifecycleClient struct {
	started           chan struct{}
	runErr, gatherErr error
}

func (c *lifecycleClient) Run(context.Context, *rpc.Client) error    { close(c.started); return c.runErr }
func (c *lifecycleClient) Gather(context.Context, *rpc.Client) error { return c.gatherErr }
func (c *lifecycleClient) Abort(context.Context, *rpc.Client) error  { return nil }
func (c *lifecycleClient) Summary() string                           { return "retained summary" }

func readManifest(t *testing.T, dir string) Manifest {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "info.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestManifestDeferredLifecycleAndAllErrors(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "errors"}[fail], func(t *testing.T) {
			e := NewExecutor(context.Background(), "", nil)
			e.DeferStart = true
			e.Manifest = &Manifest{SchemaVersion: 1, InvocationID: "invocation", Status: "scheduled"}
			for range 2 {
				c := &lifecycleClient{started: make(chan struct{})}
				if fail {
					c.runErr = errors.New("run error")
					c.gatherErr = errors.New("gather error")
				}
				e.RunClient(c)
				select {
				case <-c.started:
					t.Fatal("launched during planning")
				default:
				}
			}
			dir := t.TempDir()
			if err := e.WriteInfo(dir); err != nil {
				t.Fatal(err)
			}
			m := readManifest(t, dir)
			if len(m.Measurements) != 2 || m.Measurements[0].ExecutionStart != nil || m.Measurements[0].Summary != "" {
				t.Fatalf("initial manifest: %+v", m)
			}
			e.Start()
			runErr := e.G.Wait()
			gatherErr := e.GatherResults()
			if (runErr != nil) != fail || (gatherErr != nil) != fail {
				t.Fatal("wrong errors")
			}
			e.Finalize()
			if err := e.WriteInfo(dir); err != nil {
				t.Fatal(err)
			}
			m = readManifest(t, dir)
			want := "complete"
			if fail {
				want = "failed"
			}
			if m.Status != want {
				t.Fatalf("status %s", m.Status)
			}
			for _, r := range m.Measurements {
				if r.ExecutionStart == nil || r.ExecutionEnd == nil || r.Summary != "retained summary" {
					t.Fatalf("bad lifecycle: %+v", r)
				}
				if fail && (len(r.Errors) != 2 || r.Errors[0].Phase != "run" || r.Errors[1].Phase != "gather" || r.Errors[0].Time.IsZero()) {
					t.Fatalf("lost errors: %+v", r)
				}
			}
			if _, err := os.Stat(filepath.Join(dir, "info.txt")); !os.IsNotExist(err) {
				t.Fatal("legacy info.txt exists")
			}
		})
	}
}

func TestManifestSenderAndCommandShape(t *testing.T) {
	e := NewExecutor(context.Background(), "", nil)
	e.DryRun = true
	start := time.Now().UTC()
	dir := t.TempDir()
	out := filepath.Join(dir, "result.csv")
	c := &pkg.SenderClient{Out: out, Direction: pkg.DL, StartAt: start, Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{Duration: time.Second, Interval: time.Millisecond}}}
	e.RunClient(c)
	cmd := &pkg.CommandClient{Local: false, StartAt: start, LocalDir: dir, Params: pkg.TcpdumpParams{Name_: "capture", Timeout_: 2 * time.Second}}
	e.RunClient(cmd)
	e.Manifest = &Manifest{SchemaVersion: 1, ScheduledStart: start, Parameters: ParamMap{"direction": "DL"}, Status: "scheduled"}
	if err := e.WriteInfo(dir); err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, dir)
	if m.SchemaVersion != 1 || len(m.Measurements) != 2 || m.Measurements[0].ID != c.ID || c.ID == "" || m.Measurements[1].ID != cmd.ID || m.Measurements[1].Endpoint != "server" || m.Measurements[0].Direction != "DL" {
		t.Fatalf("bad shape %+v", m)
	}
	if m.Measurements[0].TimestampingEnabled != nil || m.Measurements[0].PacingEnabled != nil {
		t.Fatal("invented runtime capabilities")
	}
	if !m.ScheduledEnd.Equal(start.Add(2 * time.Second)) {
		t.Fatal("wrong end")
	}
	if err := os.WriteFile(out, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	e.records[0].Status = "failed"
	if err := e.WriteInfo(dir); err != nil {
		t.Fatal(err)
	}
	m = readManifest(t, dir)
	if strings.Join(m.Measurements[0].Artifacts, ",") != "result.csv" || m.Measurements[0].GatherStatus != "not_gathered" {
		t.Fatal("partial artifact falsely complete")
	}
}

func TestCancelledManifest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := NewExecutor(ctx, "", nil)
	e.DeferStart = true
	e.Manifest = &Manifest{SchemaVersion: 1}
	e.RunClient(&lifecycleClient{started: make(chan struct{})})
	cancel()
	e.Finalize()
	dir := t.TempDir()
	if err := e.WriteInfo(dir); err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, dir)
	if m.Status != "cancelled" || m.Measurements[0].Status != "cancelled" || m.ExecutionEnd == nil || m.Measurements[0].GatherStatus != "not_gathered" {
		t.Fatalf("%+v", m)
	}
}
