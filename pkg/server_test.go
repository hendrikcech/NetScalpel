package pkg

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Requesting the result of the same test twice must return an error on the
// second call. It used to fall through to panic("result.Res == nil") because
// handleChanResult misread the channel receive's ok value, killing the whole
// server on a duplicate RequestServerResult RPC.
func TestDoubleResultRetrievalReturnsError(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	s := NewServer(srvCtx, nil)

	args := RequestServerArgs{
		ID:         "double-res",
		Timeout:    100 * time.Millisecond,
		ServerMode: ReceiveUDP,
		Params:     RateParamsW{{Duration: 100 * time.Millisecond}},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}

	var res RequestServerResultReply
	if err := s.RequestServerResult(RequestServerResultArgs{ID: "double-res"}, &res); err != nil {
		t.Fatalf("first RequestServerResult failed: %v", err)
	}

	var res2 RequestServerResultReply
	err := s.RequestServerResult(RequestServerResultArgs{ID: "double-res"}, &res2)
	if err == nil {
		t.Fatal("expected an error from the second RequestServerResult")
	}
	if !strings.Contains(err.Error(), "already retrieved") {
		t.Errorf("expected an 'already retrieved' error, got: %v", err)
	}
}
