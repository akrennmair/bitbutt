package bitbutt

import "testing"

func TestOpenWriteClose(t *testing.T) {
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
		t.Fatalf("Get: expected \"foo\", got %q instead.", v)
	}

	bb.Close()

	//os.RemoveAll("./test.db")
}
