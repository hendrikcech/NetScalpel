package pkg

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestMsgPadding(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	for _, size := range []uint{0, 1, 7, 8, 1492, 65535} {
		buf := bytes.Repeat([]byte{0xaa}, int(size)+16)
		m := Msg{Seq: 42, PadN: size}
		n, err := m.Encode(buf)
		if err != nil || n != int(size)+8 || binary.BigEndian.Uint64(buf) != 42 {
			t.Fatalf("size %d: n=%d err=%v", size, n, err)
		}
		if !bytes.Equal(buf[n:], bytes.Repeat([]byte{0xaa}, 8)) {
			t.Fatal("Encode modified bytes beyond the packet")
		}
		if size >= 1492 && len(enc.EncodeAll(buf[8:n], nil)) < int(size)*95/100 {
			t.Fatalf("padding of size %d is compressible", size)
		}
	}
}

func BenchmarkMsgEncode(b *testing.B) {
	buf := make([]byte, 1500)
	m := Msg{PadN: 1492}
	b.ReportAllocs()
	b.SetBytes(1500)
	for b.Loop() {
		m.Seq++
		if _, err := m.Encode(buf); err != nil {
			b.Fatal(err)
		}
	}
}
