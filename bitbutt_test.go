package bitbutt

import (
	"os"
	"testing"
)

func TestOpenWriteClose(t *testing.T) {
	os.RemoveAll("./test.db")
	bb, err := Open("./test.db")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	v, err := bb.Get([]byte("foo"))
	if err == nil {
		t.Fatalf("Get: expected error, got %v instead.", string(v))
	}

	if err := bb.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	v, err = bb.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("Get: got error: %v", err)
	}
	if string(v) != "bar" {
		t.Fatalf("Get: expected \"bar\", got %q instead.", v)
	}

	bb.Close()

	bb, err = Open("./test.db", SyncOnPut())
	if err != nil {
		t.Fatalf("Second Open failed: %v", err)
	}

	v, err = bb.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("Get: got error: %v", err)
	}
	if string(v) != "bar" {
		t.Fatalf("Get: expected \"bar\", got %q instead.", v)
	}

	if err := bb.Put([]byte("foo"), []byte("quux")); err != nil {
		t.Fatalf("Second Put failed: %v", err)
	}

	v, err = bb.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("Get: got error: %v", err)
	}
	if string(v) != "quux" {
		t.Fatalf("Get: expected \"quux\", got %q instead.", v)
	}

	if err := bb.Delete([]byte("foo")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	v, err = bb.Get([]byte("foo"))
	if err == nil {
		t.Fatalf("Get: expected error, got %q instead.", string(v))
	}

	bb.Close()

	os.RemoveAll("./test.db")
}
