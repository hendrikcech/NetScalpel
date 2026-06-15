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
	tss := []time.Time{t0, t1, t2, t3, t4}

	r0, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:12Z")
	r1, _ := time.Parse(time.RFC3339, "2006-01-02T15:59:12Z")
	r2, _ := time.Parse(time.RFC3339, "2006-01-02T16:00:12Z")
	r3, _ := time.Parse(time.RFC3339, "2006-01-02T15:51:12Z")
	r4, _ := time.Parse(time.RFC3339, "2006-01-02T15:50:27Z")
	ris := []time.Time{r0, r1, r2, r3, r4}

	for i := range tss {
		ri := nextRi(tss[i])
		if ri != ris[i] {
			t.Errorf("Wrong RI calculated: %v -> %v but should be %v", tss[i], ri, ris[i])
		}
	}
}
