package pkg

import (
	"errors"
	"net"
	"testing"
)

func TestUDPReceiverPreparationUsesConfiguredRXSource(t *testing.T) {
	for _, source := range []TimestampSource{TimestampSoftware, TimestampHardware} {
		t.Run(source.String(), func(t *testing.T) {
			setupErr := errors.New("injected setup failure")
			receiver := &UDPReceiver{}
			var enabled TimestampSource
			err := receiver.prepareWith(nil, TimestampConfig{RX: source}, func(_ *net.UDPConn, selected TimestampSource) error {
				enabled = selected
				return setupErr
			})
			if !errors.Is(err, setupErr) || enabled != source {
				t.Fatalf("prepare selected %s, enabled %s, error %v", source, enabled, err)
			}
			if receiver.Config.RX != source || receiver.TimestampReport().Source != source {
				t.Fatalf("setup failure changed selected source: config=%+v report=%+v", receiver.Config, receiver.TimestampReport())
			}
		})
	}

	receiver := &UDPReceiver{}
	called := false
	if err := receiver.prepareWith(nil, TimestampConfig{RX: TimestampApp}, func(*net.UDPConn, TimestampSource) error {
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if called || receiver.TimestampReport().Source != TimestampApp {
		t.Fatalf("application RX preparation called kernel setup or selected another source: %+v", receiver.TimestampReport())
	}
}
