package pkg

import (
	"context"
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	logger := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	})
	slog.SetDefault(slog.New(logger))
	// slog.SetLogLoggerLevel(slog.LevelDebug)
}

var ip string = "127.0.0.1"

var port atomic.Uint32

func approxEqual[T int | uint](exp T, act T, margin T) error {
	if act < exp-margin || act > exp+margin {
		return fmt.Errorf("Value %v != expected %v (+/- %v)", act, exp, margin)
	}
	return nil
}

func serverPort() uint {
	port.CompareAndSwap(0, 15000)
	return uint(port.Add(1))
}

func dialRpcClient(ip string, port uint) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		return nil, fmt.Errorf("rpc.Dial failed: %v\n", err.Error())
	}
	return client, nil
}

func TestBurstUL(t *testing.T) {
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &BurstSender{Params: BurstParams{
			Timeout: time.Duration(100) * time.Millisecond,
			Num:     15,
			Pad:     0,
		}},
	}

	testSender(t, &client)
}

func TestBurstDL(t *testing.T) {
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: DL,

		Sender: &BurstSender{Params: BurstParams{
			Timeout: time.Duration(100) * time.Millisecond,
			Num:     15,
			Pad:     0,
		}},
	}

	testSender(t, &client)
}

func TestPeriodicUL(t *testing.T) {
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &PeriodicSender{Params: PeriodicParams{
			Interval: 1 * time.Millisecond,
			Duration: 100 * time.Millisecond,
			Pad:      200,
		}},
	}

	testSender(t, &client)
}

func TestPeriodicDL(t *testing.T) {
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &PeriodicSender{Params: PeriodicParams{
			Interval: 1 * time.Millisecond,
			Duration: 100 * time.Millisecond,
			Pad:      200,
		}},
	}

	testSender(t, &client)
}

func TestRateUL(t *testing.T) {
	pps := uint(100)
	durationS := uint(1)
	packets := pps * durationS

	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &RateSender{Params: []RateParams{RateParams{
			Pps:         pps,
			Interval:    time.Duration(10) * time.Millisecond,
			Duration:    time.Duration(durationS) * time.Second,
			PayloadSize: 1200,
		}}},
	}

	testSender(t, &client)

	if len(client.UDPMsgsSent) < int(packets)-2 || len(client.UDPMsgsSent) > int(packets)+2 {
		t.Errorf("Expected %v packets but %v were sent", packets, len(client.UDPMsgsSent))
	}
}

func TestRateDL(t *testing.T) {
	pps := uint(100)
	durationS := uint(1)
	packets := pps * durationS

	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: DL,

		Sender: &RateSender{Params: []RateParams{RateParams{
			Pps:         pps,
			Interval:    time.Duration(10) * time.Millisecond,
			Duration:    time.Duration(durationS) * time.Second,
			PayloadSize: 1200,
		}}},
	}

	testSender(t, &client)

	if len(client.UDPMsgsSent) < int(packets)-2 || len(client.UDPMsgsSent) > int(packets)+2 {
		t.Errorf("Expected %v packets but %v were sent", packets, len(client.UDPMsgsSent))
	}
}

func TestRateZeroPps(t *testing.T) {
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &RateSender{Params: []RateParams{RateParams{
			Pps:         0,
			Interval:    time.Duration(100) * time.Millisecond,
			Duration:    time.Duration(1) * time.Second,
			PayloadSize: 1200,
		}}},
	}

	testSender(t, &client)

	if len(client.UDPMsgsSent) != 0 {
		t.Errorf("Expected 0 packets but %v were sent", len(client.UDPMsgsSent))
	}
}

func TestRateMultiple(t *testing.T) {
	params := RateParams{
		Pps:         100,
		Interval:    time.Duration(100) * time.Millisecond,
		Duration:    time.Duration(1) * time.Second,
		PayloadSize: 1200,
	}

	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &RateSender{Params: []RateParams{params, params}},
	}

	testSender(t, &client)

	if err := approxEqual(200, uint(len(client.UDPMsgsSent)), 10); err != nil {
		t.Errorf("msgs sent: %v", err.Error())
	}
}

func TestRateMultipleWithZero(t *testing.T) {
	params := RateParams{
		Pps:         100,
		Interval:    time.Duration(100) * time.Millisecond,
		Duration:    time.Duration(1) * time.Second,
		PayloadSize: 1200,
	}
	paramsZero := RateParams{
		Pps:         0,
		Interval:    time.Duration(100) * time.Millisecond,
		Duration:    time.Duration(1) * time.Second,
		PayloadSize: 1200,
	}

	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &RateSender{Params: []RateParams{params, paramsZero, params}},
	}

	testSender(t, &client)

	if err := approxEqual(200, uint(len(client.UDPMsgsSent)), 10); err != nil {
		t.Errorf("msgs sent: %v", err.Error())
	}
}

func TestQUICUL(t *testing.T) {
	bytes := uint(15000)
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &QUICSender{Params: QUICParams{
			Duration_: time.Duration(2) * time.Second,
			Bytes:     bytes,
		}},
	}

	testSender(t, &client)

	bytesSum := uint(0)
	for i := range client.UDPMsgsSent {
		bytesSum += client.UDPMsgsSent[i].Len
	}

	if err := approxEqual(bytes, bytesSum, 1000); err != nil {
		t.Errorf("bytes sent: %v", err.Error())
	}

	// t.Fail()
}

func TestQUICDL(t *testing.T) {
	bytes := uint(15000)
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: DL,

		Sender: &QUICSender{Params: QUICParams{
			Duration_: time.Duration(2) * time.Second,
			Bytes:     bytes,
		}},
	}

	testSender(t, &client)

	bytesSum := uint(0)
	for i := range client.UDPMsgsRcvd {
		bytesSum += client.UDPMsgsRcvd[i].Len
	}

	if err := approxEqual(bytes, bytesSum, 1000); err != nil {
		t.Errorf("bytes sent: %v", err.Error())
	}

	// t.Fail()
}

func TestTCPUL(t *testing.T) {
	bytes := uint(15000)
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &TCPSender{Params: TCPSenderParams{
			Duration_: time.Duration(2) * time.Second,
			Bytes:     bytes,
		}},
	}
	testSender(t, &client)
}

func TestTCPDL(t *testing.T) {
	bytes := uint(15000)
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: DL,

		Sender: &TCPSender{Params: TCPSenderParams{
			Duration_: time.Duration(1) * time.Second,
			Bytes:     bytes,
		}},
	}
	testSender(t, &client)
}

func TestTCPULBBR(t *testing.T) {
	bytes := uint(15000)
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &TCPSender{Params: TCPSenderParams{
			Duration_: time.Duration(2) * time.Second,
			Bytes:     bytes,
			CCA:       BBR,
		}},
	}
	testSender(t, &client)
}

func TestTCPDLBBR(t *testing.T) {
	bytes := uint(15000)
	client := SenderClient{
		IP:        ip,
		Out:       "",
		Direction: UL,

		Sender: &TCPSender{Params: TCPSenderParams{
			Duration_: time.Duration(2) * time.Second,
			Bytes:     bytes,
			CCA:       BBR,
		}},
	}
	testSender(t, &client)
}

// ---

func testSender(t *testing.T, c *SenderClient) {
	t.Parallel()

	RegisterGob()

	port := serverPort()

	ctxS := context.Background()
	server := RunServer(ctxS, c.IP, port, nil)

	ctxC := context.Background()
	rpcClient, err := dialRpcClient(c.IP, port)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}
	start := time.Now()
	if err := c.Run(ctxC, rpcClient); err != nil {
		t.Fatalf("client.Run failed: %v", err.Error())
	}
	if err := c.Gather(ctxC, rpcClient); err != nil {
		t.Fatalf("client.Gather failed: %v", err.Error())
	}
	_ = time.Since(start)

	rpcClient.Close()

	server.Stop()

	switch c.Sender.SenderMode().SocketType() {
	case UDP:
		if len(c.UDPMsgsSent) != len(c.UDPMsgsRcvd) {
			t.Errorf("Not all messages were received: %v != %v", len(c.UDPMsgsSent), len(c.UDPMsgsRcvd))
		}

		for i, msg := range c.UDPMsgsSent {
			if msg.Seq != uint64(i) {
				t.Errorf("expected seq %v, got %v in UDPMsgsSent", i, msg.Seq)
			}
		}

		for i, msg := range c.UDPMsgsRcvd {
			if msg.Seq != uint64(i) {
				t.Errorf("expected seq %v, got %v in UDPMsgsRcvd", i, msg.Seq)
			}
		}

		// TODO: reenable
		// for i := range c.UDPMsgsSent {
		// 	if c.UDPMsgsSent[i].Len != c.UDPMsgsRcvd[i].Len {
		// 		t.Errorf("length of sent and received packet differs: %v != %v", c.UDPMsgsSent[i].Len, c.UDPMsgsRcvd[i].Len)
		// 	}
		// }
	case TCP:
		if c.TCPMetricsSndr == nil {
			t.Errorf("c.TCPMetricsSndr is nil")
		}
		if len(c.TCPMetricsSndr) == 0 {
			t.Errorf("c.TCPMetricsSndr is empty")
		}
		if c.TCPMetricsRcvr == nil {
			t.Errorf("c.TCPMetricsRcvr is nil")
		}
		if len(c.TCPMetricsRcvr) == 0 {
			t.Errorf("c.TCPMetricsRcvr is empty")
		}
	default:
		panic("Unknown SocketType")
	}
}

// ----

func TestRunCommandTcpdumpLocal(t *testing.T) {
	testRunCommandTcpdump(t, true)
}

func TestRunCommandTcpdumpRemote(t *testing.T) {
	testRunCommandTcpdump(t, false)
}

func testRunCommandTcpdump(t *testing.T, local bool) {
	t.Parallel()

	RegisterGob()

	ctxS := context.Background()
	port := serverPort()
	server := RunServer(ctxS, ip, port, nil)
	defer server.Stop()

	ctxC := context.Background()
	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}
	defer rpcClient.Close()

	resultDir, err := RandDir(fmt.Sprintf("test_tcpdump_%v", local))
	if err != nil {
		t.Fatalf("%v", err.Error())
	}
	fmt.Printf("RunCommand writing to %v\n", resultDir)

	client := CommandClient{
		Params: TcpdumpParams{
			Name_:    "tcpdump",
			Timeout_: 2 * time.Second,
		},
		Local:    local,
		StartAt:  time.Time{},
		LocalDir: resultDir,
	}

	if err := client.Run(ctxC, rpcClient); err != nil {
		t.Fatalf("RunCommand(Tcpdump) failed: %v", err)
	}
}
