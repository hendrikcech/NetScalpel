package pkg

import (
	"os"
	"testing"
)

func TestRandDir(t *testing.T) {
	a, err := RandDir("randdir_test")
	if err != nil {
		t.Fatalf("RandDir: %v", err)
	}
	defer os.RemoveAll(a)

	b, err := RandDir("randdir_test")
	if err != nil {
		t.Fatalf("RandDir: %v", err)
	}
	defer os.RemoveAll(b)

	if a == b {
		t.Errorf("Two RandDir calls returned the same directory: %v", a)
	}

	// Commands may write into the directory with a different identity than
	// this process (tcpdump under sudo), so it must be world-writable.
	info, err := os.Stat(a)
	if err != nil {
		t.Fatalf("os.Stat(%v): %v", a, err)
	}
	if !info.IsDir() {
		t.Errorf("%v is not a directory", a)
	}
	if perm := info.Mode().Perm(); perm != os.ModePerm {
		t.Errorf("Directory permissions are %v, want %v", perm, os.ModePerm)
	}
}
