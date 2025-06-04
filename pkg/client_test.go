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

func TestBurst(t *testing.T) {
	client := SenderClient{
		Ip:      ip,
		Port:    serverPort(),
		Out:     "",
		Reverse: false,

		Sender: &BurstSender{Params: BurstParams{
			Timeout: time.Duration(100) * time.Millisecond,
			Num:     15,
			Pad:     0,
		}},
	}

	testSender(t, &client)
	testBurst(t, &client)
}

func TestBurstReverse(t *testing.T) {
	client := SenderClient{
		Ip:      ip,
		Port:    serverPort(),
		Out:     "",
		Reverse: true,

		Sender: &BurstSender{Params: BurstParams{
			Timeout: time.Duration(100) * time.Millisecond,
			Num:     15,
			Pad:     0,
		}},
	}

	testSender(t, &client)
	testBurst(t, &client)
}

func TestRateUL(t *testing.T) {
	pps := uint(100)
	durationS := uint(1)
	packets := pps * durationS

	client := SenderClient{
		Ip:      ip,
		Port:    serverPort(),
		Out:     "",
		Reverse: false,

		Sender: &RateSender{Params: []RateParams{RateParams{
			Pps:         pps,
			Interval:    time.Duration(10) * time.Millisecond,
			Duration:    time.Duration(durationS) * time.Second,
			PayloadSize: 1200,
		}}},
	}

	testSender(t, &client)

	if len(client.MsgsSent) != int(packets) {
		t.Errorf("Expected %v packets but %v were sent", packets, len(client.MsgsSent))
	}
}

func TestRateReverse(t *testing.T) {
	pps := uint(100)
	durationS := uint(1)
	packets := pps * durationS

	client := SenderClient{
		Ip:      ip,
		Port:    serverPort(),
		Out:     "",
		Reverse: true,

		Sender: &RateSender{Params: []RateParams{RateParams{
			Pps:         pps,
			Interval:    time.Duration(10) * time.Millisecond,
			Duration:    time.Duration(durationS) * time.Second,
			PayloadSize: 1200,
		}}},
	}

	testSender(t, &client)

	if len(client.MsgsSent) != int(packets) {
		t.Errorf("Expected %v packets but %v were sent", packets, len(client.MsgsSent))
	}
}

func TestRateZeroPps(t *testing.T) {
	client := SenderClient{
		Ip:      ip,
		Port:    serverPort(),
		Out:     "",
		Reverse: false,

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

func testSender(t *testing.T, c *SenderClient) {
	RegisterGob()

	server := RunServer(c.Ip, c.Port)

	rpcClient, err := dialRpcClient(c.Ip, c.Port)
	if err != nil {
		t.Errorf("%v", err.Error())
	}
	c.Run(rpcClient)
	c.Gather(rpcClient)
	rpcClient.Close()

	server.Stop()

	if len(c.MsgsSent) != len(c.MsgsRcvd) {
		t.Errorf("Not all messages were received: %v != %v", len(c.MsgsSent), len(c.MsgsRcvd))
	}

	for i, msg := range c.MsgsSent {
		if msg.Seq != uint64(i) {
			t.Errorf("expected seq %v, got %v in msgsSent", i, msg.Seq)
		}
	}

	for i, msg := range c.MsgsRcvd {
		if msg.Seq != uint64(i) {
			t.Errorf("expected seq %v, got %v in msgsRcvd", i, msg.Seq)
		}
	}

	for i := range c.MsgsSent {
		if c.MsgsSent[i].Len != c.MsgsRcvd[i].Len {
			t.Errorf("length of sent and received packet differs: %v != %v", c.MsgsSent[i].Len, c.MsgsRcvd[i].Len)
		}
	}
}

func testBurst(t *testing.T, client *SenderClient) {
	for i, msg := range client.MsgsSent {
		if msg.Seq != uint64(i) {
			t.Errorf("expected seq %v, got %v in msgsSent", i, msg.Seq)
		}
	}

	for i, msg := range client.MsgsRcvd {
		if msg.Seq != uint64(i) {
			t.Errorf("expected seq %v, got %v in msgsRcvd", i, msg.Seq)
		}
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
	RegisterGob()

	port := serverPort()
	server := RunServer(ip, port)
	defer server.Stop()

	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Errorf("%v", err.Error())
	}
	defer rpcClient.Close()

	resultDir, err := RandDir(fmt.Sprintf("test_tcpdump_%v", local))
	if err != nil {
		t.Errorf("%v", err.Error())
	}
	fmt.Printf("RunCommand writing to %v", resultDir)

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
		t.Errorf("RunCommand(Tcpdump) failed: %v", err)
	}
}
