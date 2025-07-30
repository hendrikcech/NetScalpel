package pkg

import (
	"testing"
)

func TestNetModesComplete(t *testing.T) {
	assertPanic(t, func() {
		oob := len(NetModes)
		m := Mode(oob)
		_ = m.String()
	})
}

func TestModeString(t *testing.T) {
	for _, m := range NetModes {
		_ = m.String()
	}
}

func TestModeSocketType(t *testing.T) {
	for _, m := range NetModes {
		m.SocketType()
	}
}

func assertPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	f()
}
