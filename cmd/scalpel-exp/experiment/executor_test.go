package experiment

import (
	"context"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/pkg"
)

func testSender(start time.Time) *pkg.SenderClient {
	return &pkg.SenderClient{
		StartAt: start,
		Sender:  &pkg.PeriodicSender{Params: pkg.PeriodicParams{Duration: time.Second}},
	}
}

func testCapture(start time.Time) *pkg.CommandClient {
	return &pkg.CommandClient{
		Params:  pkg.TcpdumpParams{Name_: "tcpdump_local", Timeout_: time.Second},
		StartAt: start,
	}
}

func TestRebasePreservesSchedule(t *testing.T) {
	anchor := time.Now().Add(time.Minute)
	start := time.Now().Add(2 * time.Second)
	e := NewExecutor(context.Background(), "", nil)
	e.DeferStart = true
	sender := testSender(anchor.Add(-1500 * time.Millisecond))
	capture := testCapture(anchor.Add(-2500 * time.Millisecond))
	later := testSender(anchor.Add(5 * time.Second))
	e.RunClient(sender)
	e.RunClient(capture)
	e.RunClient(later)
	if err := e.Rebase(start); err != nil {
		t.Fatal(err)
	}
	if !capture.StartTime().Equal(start) || !sender.StartTime().Equal(start.Add(time.Second)) ||
		!later.StartTime().Equal(start.Add(7500*time.Millisecond)) {
		t.Fatalf("relative timing changed: capture=%v sender=%v later=%v",
			capture.StartTime(), sender.StartTime(), later.StartTime())
	}
	if !e.records[1].ScheduledStart.Equal(start) {
		t.Fatalf("manifest start not shifted: %v", e.records[1].ScheduledStart)
	}
}

func TestRebaseRejectsInvalidScheduleWithoutMutation(t *testing.T) {
	for _, name := range []string{"started", "empty", "zero target", "zero event"} {
		t.Run(name, func(t *testing.T) {
			start := time.Now().Add(time.Second)
			original := start.Add(time.Minute)
			client := testSender(original)
			e := NewExecutor(context.Background(), "", nil)
			e.DeferStart = true
			e.RunClient(client)
			switch name {
			case "started":
				e.Started = true
			case "empty":
				e.Clients = nil
			case "zero target":
				start = time.Time{}
			case "zero event":
				e.RunClient(testCapture(time.Time{}))
			}
			if err := e.Rebase(start); err == nil {
				t.Fatal("expected invalid schedule to fail")
			}
			if !client.StartTime().Equal(original) {
				t.Fatal("failed rebase mutated the schedule")
			}
		})
	}
}
