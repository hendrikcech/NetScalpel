package pkg

import (
	"fmt"
	"net/rpc"
	"sync/atomic"
	"testing"
	"time"
)

var ip string = "127.0.0.1"

var port atomic.Uint32

func approxEqual[T int | uint](exp T, act T, margin T) error {
	if act < exp - margin || act > exp + margin {
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
		Ip:        ip,
		Port:      serverPort(),
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
		Ip:        ip,
		Port:      serverPort(),
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
		Ip:        ip,
		Port:      serverPort(),
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
		Ip:        ip,
		Port:      serverPort(),
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
		Ip:        ip,
		Port:      serverPort(),
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

	if len(client.MsgsSent) < int(packets)-2 || len(client.MsgsSent) > int(packets)+2 {
		t.Errorf("Expected %v packets but %v were sent", packets, len(client.MsgsSent))
	}
}

func TestRateDL(t *testing.T) {
	pps := uint(100)
	durationS := uint(1)
	packets := pps * durationS

	client := SenderClient{
		Ip:        ip,
		Port:      serverPort(),
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

	if len(client.MsgsSent) < int(packets)-2 || len(client.MsgsSent) > int(packets)+2 {
		t.Errorf("Expected %v packets but %v were sent", packets, len(client.MsgsSent))
	}
}

func TestRateZeroPps(t *testing.T) {
	client := SenderClient{
		Ip:        ip,
		Port:      serverPort(),
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

	if len(client.MsgsSent) != 0 {
		t.Errorf("Expected 0 packets but %v were sent", len(client.MsgsSent))
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
		Ip:        ip,
		Port:      serverPort(),
		Out:       "",
		Direction: UL,

		Sender: &RateSender{Params: []RateParams{params, params}},
	}

	testSender(t, &client)

	if err := approxEqual(200, uint(len(client.MsgsSent)), 4); err != nil {
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
		Ip:        ip,
		Port:      serverPort(),
		Out:       "",
		Direction: UL,

		Sender: &RateSender{Params: []RateParams{params, paramsZero, params}},
	}

	testSender(t, &client)

	if err := approxEqual(200, uint(len(client.MsgsSent)), 4); err != nil {
		t.Errorf("msgs sent: %v", err.Error())
	}
}

func testSender(t *testing.T, c *SenderClient) {
	RegisterGob()

	server := RunServer(c.Ip, c.Port)

	rpcClient, err := dialRpcClient(c.Ip, c.Port)
	if err != nil {
		t.Fatalf("%v", err.Error())
	}
	start := time.Now()
	if err := c.Run(rpcClient); err != nil {
		t.Fatalf("client.Run failed: %v", err.Error())
	}
	if err := c.Gather(rpcClient); err != nil {
		t.Fatalf("client.Gather failed: %v", err.Error())
	}
	_ = time.Since(start)

	rpcClient.Close()

	server.Stop()

	if len(c.MsgsSent) != len(c.MsgsRcvd) {
		t.Errorf("Not all messages were received: %v != %v", len(c.MsgsSent), len(c.MsgsRcvd))
	}

	for i, msg := range c.MsgsSent {
		if msg.Seq != uint64(i) {
			t.Errorf("expected seq %v, got %v in MsgsSent", i, msg.Seq)
		}
	}

	for i, msg := range c.MsgsRcvd {
		if msg.Seq != uint64(i) {
			t.Errorf("expected seq %v, got %v in MsgsRcvd", i, msg.Seq)
		}
	}

	// TODO: reenable
	// for i := range c.MsgsSent {
	// 	if c.MsgsSent[i].Len != c.MsgsRcvd[i].Len {
	// 		t.Errorf("length of sent and received packet differs: %v != %v", c.MsgsSent[i].Len, c.MsgsRcvd[i].Len)
	// 	}
	// }
}

// ----

func TestRunCommandTcpdumpLocal(t *testing.T) {
	testRunCommandTcpdump(t, true)
}

func TestRunCommandTcpdumpRemote(t *testing.T) {
	testRunCommandTcpdump(t, false)
}

func testRunCommandTcpdump(t *testing.T, local bool) {
	RegisterGob()

	port := serverPort()
	server := RunServer(ip, port)
	defer server.Stop()

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
			Timeout_: 5 * time.Second,
		},
		Local:    local,
		StartAt:  time.Time{},
		LocalDir: resultDir,
	}

	if err := client.Run(rpcClient); err != nil {
		t.Fatalf("RunCommand(Tcpdump) failed: %v", err)
	}
}
