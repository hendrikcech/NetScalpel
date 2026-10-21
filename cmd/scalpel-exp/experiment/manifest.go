package experiment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hendrikcech/netscalpel/internal/hostinfo"
	"github.com/hendrikcech/netscalpel/pkg"
)

type ExecutionError struct {
	Time     time.Time `json:"time"`
	Endpoint string    `json:"endpoint"`
	Phase    string    `json:"phase"`
	Code     string    `json:"code"`
	Message  string    `json:"message"`
}

type Measurement struct {
	ID            string        `json:"id"`
	Kind          string        `json:"kind"`
	Direction     string        `json:"direction,omitempty"`
	Endpoint      string        `json:"endpoint"`
	Peer          string        `json:"peer,omitempty"`
	Configuration Configuration `json:"configuration"`
	// Nil means not instrumented/unknown, not disabled. Requested configuration
	// is separate from the runtime state of sockets on either endpoint.
	TimestampingEnabled *bool            `json:"timestamping_enabled"`
	PacingEnabled       *bool            `json:"pacing_enabled"`
	ScheduledStart      time.Time        `json:"scheduled_start"`
	ScheduledEnd        time.Time        `json:"scheduled_end"`
	ExecutionStart      *time.Time       `json:"execution_start"`
	ExecutionEnd        *time.Time       `json:"execution_end"`
	Status              string           `json:"status"`
	GatherStatus        string           `json:"gather_status"`
	Artifacts           []string         `json:"artifacts"`
	ExpectedArtifacts   []string         `json:"expected_artifacts"`
	Summary             string           `json:"summary"`
	Errors              []ExecutionError `json:"errors"`
}

type Endpoint struct {
	Role     string             `json:"role"`
	Metadata *pkg.Metadata      `json:"metadata"`
	After    *hostinfo.Snapshot `json:"after"`
}

type Manifest struct {
	SchemaVersion  int              `json:"schema_version"`
	ExperimentID   string           `json:"experiment_id"`
	InvocationID   string           `json:"invocation_id"`
	Round          uint             `json:"round"`
	Procedure      string           `json:"procedure"`
	Parameters     ParamMap         `json:"parameters"`
	ReferenceTime  time.Time        `json:"reference_time"`
	ScheduledStart time.Time        `json:"scheduled_start"`
	ScheduledEnd   time.Time        `json:"scheduled_end"`
	ExecutionStart *time.Time       `json:"execution_start"`
	ExecutionEnd   *time.Time       `json:"execution_end"`
	Status         string           `json:"status"`
	Client         Endpoint         `json:"client"`
	Server         Endpoint         `json:"server"`
	Measurements   []*Measurement   `json:"measurements"`
	Warnings       []string         `json:"warnings"`
	Errors         []ExecutionError `json:"errors"`
}

func errorRecord(endpoint, phase string, err error) ExecutionError {
	return ExecutionError{Time: time.Now().UTC(), Endpoint: endpoint, Phase: phase, Code: phase + "_failed", Message: err.Error()}
}

func describe(client pkg.Client) *Measurement {
	m := &Measurement{Kind: fmt.Sprintf("%T", client), Endpoint: "client", Status: "scheduled", GatherStatus: "not_gathered", Artifacts: []string{}, Errors: []ExecutionError{}}
	switch c := client.(type) {
	case *pkg.SenderClient:
		if c.ID == "" {
			c.ID = pkg.GenID(c.Sender.SenderMode().String())
		}
		m.ID = c.ID
		m.Kind = c.Sender.SenderMode().String()
		m.Direction = c.Direction.String()
		m.Peer = c.IP
		m.Configuration = senderConfiguration(c.Sender.GetParams())
		m.ScheduledStart = c.StartAt
		m.ScheduledEnd = c.StartAt.Add(c.Sender.GetParams().GetDuration())
	case *pkg.CommandClient:
		if c.ID == "" {
			c.ID = pkg.GenID(c.Params.Name())
		}
		m.ID = c.ID
		m.Kind = "command"
		if !c.Local {
			m.Endpoint = "server"
		}
		m.Configuration = commandConfiguration(c.Params)
		m.ScheduledStart = c.StartAt
		m.ScheduledEnd = c.StartAt.Add(c.Params.Timeout())
	}
	return m
}

// WriteInfo atomically replaces the manifest. Call only before launching or
// after Wait/Gather, when summaries and artifacts are no longer being mutated.
func (e *Executor) WriteInfo(path string) error {
	if e.Manifest == nil {
		e.Manifest = &Manifest{SchemaVersion: 1, Status: "unknown"}
	}
	m := e.Manifest
	m.Measurements = e.records
	if len(e.records) > 0 {
		m.ScheduledStart = time.Time{}
		m.ScheduledEnd = time.Time{}
	}
	for i, c := range e.Clients {
		r := e.records[i]
		if m.ScheduledStart.IsZero() || r.ScheduledStart.Before(m.ScheduledStart) {
			m.ScheduledStart = r.ScheduledStart
		}
		if r.ExecutionEnd != nil {
			r.Summary = c.Summary()
		}
		if r.ScheduledEnd.After(m.ScheduledEnd) {
			m.ScheduledEnd = r.ScheduledEnd
		}
		var paths []string
		r.ExpectedArtifacts = []string{}
		switch c := c.(type) {
		case *pkg.SenderClient:
			paths = []string{c.Out}
			if c.Out != "" {
				if rel, err := filepath.Rel(path, c.Out); err == nil {
					r.ExpectedArtifacts = append(r.ExpectedArtifacts, filepath.ToSlash(rel))
				}
			}
		case *pkg.CommandClient:
			paths = c.Artifacts
			if _, ok := c.Params.(pkg.TcpdumpParams); ok {
				r.ExpectedArtifacts = append(r.ExpectedArtifacts, c.Params.Name()+".pcap.zst")
			}
		}
		r.Artifacts = []string{}
		for _, p := range paths {
			if _, err := os.Stat(p); err != nil {
				continue
			}
			rel, err := filepath.Rel(path, p)
			if err != nil {
				continue
			}
			r.Artifacts = append(r.Artifacts, filepath.ToSlash(rel))
		}
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(path, ".info-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(path, "info.json"))
}

func (e *Executor) AddError(endpoint, phase string, err error) {
	if e.Manifest != nil {
		e.Manifest.Errors = append(e.Manifest.Errors, errorRecord(endpoint, phase, err))
	}
}

func (e *Executor) Finalize() {
	if e.Manifest == nil {
		return
	}
	m := e.Manifest
	now := time.Now().UTC()
	m.ExecutionEnd = &now
	m.Status = "complete"
	if len(m.Errors) > 0 {
		m.Status = "failed"
	}
	for _, r := range e.records {
		if r.Status != "complete" {
			m.Status = "failed"
		}
	}
	if e.ctx.Err() != nil {
		m.Status = "cancelled"
		for _, r := range e.records {
			if r.Status == "scheduled" || r.Status == "ran" || r.Status == "running" {
				r.Status = "cancelled"
			}
		}
	}
}
