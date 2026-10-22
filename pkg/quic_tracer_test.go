package pkg

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

func TestQUICSenderWaitsForReceiverFIN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener, err := quic.ListenAddr("127.0.0.1:0", generateTLSConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	done := make(chan error, 1)
	sender := QUICSender{Params: QUICParams{Bytes: 15000}}
	go func() { done <- sender.send(ctx, udp, listener.Addr(), NewQUICTracer()) }()
	conn, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	stream.SetReadDeadline(deadline)
	data, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != int(sender.Params.Bytes) {
		t.Fatalf("received %d bytes, want %d", len(data), sender.Params.Bytes)
	}
	select {
	case err := <-done:
		t.Fatalf("sender closed before receiver FIN: %v", err)
	default:
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("sender did not finish after receiver FIN")
	}
}

func TestQUICPacketTracer(t *testing.T) {
	for _, sending := range []bool{true, false} {
		name := "receive"
		if sending {
			name = "send"
		}
		t.Run(name, func(t *testing.T) {
			tracer := NewQUICTracer()
			factory := tracer.NewReceiveTracer
			if sending {
				factory = tracer.NewSendTracer
			}
			trace := factory(context.Background(), sending, quic.ConnectionID{})
			if !trace.SupportsSchemas(qlog.EventSchema) || trace.SupportsSchemas("unsupported") {
				t.Fatal("incorrect schema support")
			}
			producer := trace.AddProducer()
			before := time.Now()
			for _, packetType := range []qlog.PacketType{qlog.PacketTypeInitial, qlog.PacketTypeHandshake, qlog.PacketType0RTT, qlog.PacketType1RTT} {
				header := qlog.PacketHeader{PacketType: packetType, PacketNumber: 42}
				raw := qlog.RawInfo{Length: 1234}
				// Only the selected direction's 1-RTT event may be collected.
				producer.RecordEvent(qlog.PacketSent{Header: header, Raw: raw})
				producer.RecordEvent(qlog.PacketReceived{Header: header, Raw: raw})
			}
			after := time.Now()
			var seq uint64
			var length uint
			var timestamp time.Time
			if sending {
				if len(tracer.sentC) != 1 || len(tracer.rcvdC) != 0 {
					t.Fatalf("unexpected packet counts: sent=%d received=%d", len(tracer.sentC), len(tracer.rcvdC))
				}
				msg := <-tracer.sentC
				seq, length, timestamp = msg.Seq, msg.Len, msg.TsSent
			} else {
				if len(tracer.rcvdC) != 1 || len(tracer.sentC) != 0 {
					t.Fatalf("unexpected packet counts: sent=%d received=%d", len(tracer.sentC), len(tracer.rcvdC))
				}
				msg := <-tracer.rcvdC
				seq, length, timestamp = msg.Seq, msg.Len, msg.TsRcvd
			}
			if seq != 42 || length != 1234 || timestamp.Before(before) || timestamp.After(after) {
				t.Fatalf("incorrect packet metadata: seq=%d len=%d time=%s", seq, length, timestamp)
			}
			if err := producer.Close(); err != nil {
				t.Fatal(err)
			}
			// Closing one producer must not disable another or close our channels.
			late := trace.AddProducer()
			close(tracer.quit)
			if sending {
				late.RecordEvent(qlog.PacketSent{Header: qlog.PacketHeader{PacketType: qlog.PacketType1RTT}})
			} else {
				late.RecordEvent(qlog.PacketReceived{Header: qlog.PacketHeader{PacketType: qlog.PacketType1RTT}})
			}
			if err := late.Close(); err != nil {
				t.Fatal(err)
			}
			// Once the buffer is full, shutdown must unblock late callbacks.
			for len(tracer.sentC) < cap(tracer.sentC) {
				tracer.sentC <- MsgSent{}
			}
			for len(tracer.rcvdC) < cap(tracer.rcvdC) {
				tracer.rcvdC <- MsgRcvd{}
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				producer := trace.AddProducer()
				defer producer.Close()
				producer.RecordEvent(qlog.PacketSent{Header: qlog.PacketHeader{PacketType: qlog.PacketType1RTT}})
				producer.RecordEvent(qlog.PacketReceived{Header: qlog.PacketHeader{PacketType: qlog.PacketType1RTT}})
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("late callback blocked after shutdown")
			}
		})
	}
}
