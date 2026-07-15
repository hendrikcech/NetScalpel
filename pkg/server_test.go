package pkg

import (
	"context"
	"testing"
	"time"
)

// The server runs indefinitely with --rounds 0; per-test state must not
// accumulate. finishTest already removes testCancel, and retrieving a result
// must remove the resultC entry.
func TestResultMapCleanedAfterRetrieval(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	s := NewServer(srvCtx, nil)

	args := RequestServerArgs{
		ID:         "cleanup-res",
		Timeout:    100 * time.Millisecond,
		ServerMode: ReceiveUDP,
		Params:     RateParamsW{{Duration: 100 * time.Millisecond}},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}

	var res RequestServerResultReply
	if err := s.RequestServerResult(RequestServerResultArgs{ID: "cleanup-res"}, &res); err != nil {
		t.Fatalf("RequestServerResult failed: %v", err)
	}

	s.resultLock.Lock()
	nResults := len(s.resultC)
	nCancels := len(s.testCancel)
	s.resultLock.Unlock()
	if nResults != 0 {
		t.Errorf("expected resultC to be empty after retrieval, has %v entries", nResults)
	}
	if nCancels != 0 {
		t.Errorf("expected testCancel to be empty after retrieval, has %v entries", nCancels)
	}
}
