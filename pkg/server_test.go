package pkg

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestPrepareUDPReceiverSetupFailuresDiscardRejectedRequestAndCloseSocket(t *testing.T) {
	for _, source := range []TimestampSource{TimestampHardware, TimestampSoftware} {
		t.Run(source.String(), func(t *testing.T) {
			conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}

			setupErr := errors.New("injected timestamp socket setup failure")
			config := TimestampConfig{RX: source}
			s := NewServer(context.Background(), nil, config)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			id := "rx-setup-failure"
			s.resultLock.Lock()
			s.resultC[id] = make(chan *Result, 1)
			s.testCancel[id] = cancelRequest
			s.resultLock.Unlock()

			receiver, err := s.prepareUDPReceiverForRequest(requestCtx, id, conn, config, func(receiver *UDPReceiver, conn *net.UDPConn, config TimestampConfig) error {
				return receiver.prepareWith(conn, config, func(*net.UDPConn, TimestampSource) error {
					return setupErr
				})
			})
			if !errors.Is(err, setupErr) || receiver == nil {
				t.Fatalf("prepareUDPReceiverWith() = %v, %v; want receiver and setup error", receiver, err)
			}
			report := receiver.TimestampReport()
			if report.Source != source || report.Requested != source || receiver.Config.RX != source {
				t.Fatalf("setup failure changed requested/selected source: config=%+v report=%+v", receiver.Config, report)
			}
			if deadlineErr := conn.SetReadDeadline(time.Now()); !errors.Is(deadlineErr, net.ErrClosed) {
				t.Fatalf("UDP socket remained open after preparation failure: %v", deadlineErr)
			}
			s.resultLock.RLock()
			resultCount, cancelCount := len(s.resultC), len(s.testCancel)
			s.resultLock.RUnlock()
			if resultCount != 0 || cancelCount != 0 || requestCtx.Err() != context.Canceled {
				t.Fatalf("rejected setup retained server state: results=%d cancellations=%d context=%v", resultCount, cancelCount, requestCtx.Err())
			}
		})
	}
}

func TestRequestServerDuplicateIDPreservesExistingResult(t *testing.T) {
	s := NewServer(context.Background(), nil)
	const id = "existing-measurement"
	expected := &Result{Err: errors.New("existing result")}
	existingResult := make(chan *Result, 1)
	existingResult <- expected
	close(existingResult)
	existingCtx, existingCancel := context.WithCancel(context.Background())
	defer existingCancel()
	s.resultLock.Lock()
	s.resultC[id] = existingResult
	s.testCancel[id] = func() { existingCancel() }
	s.resultLock.Unlock()

	args := RequestServerArgs{ID: id, ServerMode: ReceiveUDP}
	if err := s.RequestServer(args, &RequestServerReply{}); err == nil {
		t.Fatal("duplicate RequestServer ID was accepted")
	}
	s.resultLock.RLock()
	gotResult := s.resultC[id]
	gotCancel, hasCancel := s.testCancel[id]
	s.resultLock.RUnlock()
	if gotResult != existingResult || !hasCancel || gotCancel == nil || existingCtx.Err() != nil {
		t.Fatalf("duplicate rejection changed existing request state: result=%v cancel=%v context=%v", gotResult, hasCancel, existingCtx.Err())
	}
	if got := <-existingResult; got != expected {
		t.Fatalf("duplicate rejection replaced existing result: got %v, want %v", got, expected)
	}
}

// Per-test state must not accumulate over the server's lifetime. finishTest
// already removes testCancel, and retrieving a result must remove the resultC
// entry.
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
