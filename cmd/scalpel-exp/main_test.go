package main

import (
	"testing"
	"time"
)

func TestNextRi(t *testing.T) {
	t0, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	t1, _ := time.Parse(time.RFC3339, "2006-01-02T15:59:05Z")
	t2, _ := time.Parse(time.RFC3339, "2006-01-02T15:59:57Z")
	t3, _ := time.Parse(time.RFC3339, "2006-01-02T15:50:59Z")
	t4, _ := time.Parse(time.RFC3339, "2006-01-02T15:50:26Z")
	// Exactly on a boundary second: the next RI, not the current one.
	t5, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:12Z")
	t6, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:57Z")
	// The :57+15 slot rolls over into second :12 of the next minute.
	t7, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:58Z")
	tss := []time.Time{t0, t1, t2, t3, t4, t5, t6, t7}

	r0, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:12Z")
	r1, _ := time.Parse(time.RFC3339, "2006-01-02T15:59:12Z")
	r2, _ := time.Parse(time.RFC3339, "2006-01-02T16:00:12Z")
	r3, _ := time.Parse(time.RFC3339, "2006-01-02T15:51:12Z")
	r4, _ := time.Parse(time.RFC3339, "2006-01-02T15:50:27Z")
	r5, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:27Z")
	r6, _ := time.Parse(time.RFC3339, "2006-01-02T15:05:12Z")
	r7, _ := time.Parse(time.RFC3339, "2006-01-02T15:05:12Z")
	ris := []time.Time{r0, r1, r2, r3, r4, r5, r6, r7}

	for i := range tss {
		ri := nextRi(tss[i])
		if ri != ris[i] {
			t.Errorf("Wrong RI calculated: %v -> %v but should be %v", tss[i], ri, ris[i])
		}
	}
}

func TestNextRiNonUTC(t *testing.T) {
	loc := time.FixedZone("UTC+2", 2*60*60)
	ts := time.Date(2026, 1, 2, 15, 4, 20, 0, loc)
	want := time.Date(2026, 1, 2, 15, 4, 27, 0, loc)
	if ri := nextRi(ts); !ri.Equal(want) || ri.Location() != loc {
		t.Errorf("nextRi(%v) = %v, want %v in %v", ts, ri, want, loc)
	}
}
