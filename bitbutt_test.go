package bitbutt

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func TestMerge(t *testing.T) {
	const iterations = 10555
	os.RemoveAll("./merge.db")
	bb, err := Open("./merge.db", SizeThreshold(1000))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < iterations; i++ {
		err := bb.Put([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprintf("%09d", i)))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	//t.Logf("keyDir before merge: %s", spew.Sdump(bb.keyDir))

	bb.Merge()

	//t.Logf("keyDir after merge: %s", spew.Sdump(bb.keyDir))

	for i := 0; i < iterations; i++ {
		expectedData := []byte(fmt.Sprintf("%09d", i))
		key := []byte(fmt.Sprintf("%09d", i))
		actualData, err := bb.Get(key)
		if err != nil {
			t.Fatalf("Get %s failed: %v", string(key), err)
		}
		if !bytes.Equal(expectedData, actualData) {
			t.Fatalf("%d. got different data: %q != %q", i, string(expectedData), string(actualData))
		}
	}

	for i := 0; i < iterations; i += 2 {
		key := []byte(fmt.Sprintf("%09d", i))
		if err := bb.Delete(key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	bb.Merge()

	for i := 0; i < iterations; i++ {
		key := []byte(fmt.Sprintf("%09d", i))
		if i%2 == 0 {
			data, err := bb.Get(key)
			if err == nil {
				t.Fatalf("%d. expected non-existent key, got %q:%q instead.", i, string(key), string(data))
			}
		} else {
			data, err := bb.Get(key)
			if err != nil {
				t.Fatalf("%d. Get %q failed: %v", i, string(key), err)
			}
			if string(data) != string(key) {
				t.Fatalf("%d. Get %q returned wrong data: %q", i, string(key), string(data))
			}
		}
	}

	for i := 0; i < iterations; i++ {
		err := bb.Put([]byte(fmt.Sprintf("%09d", iterations+i)), []byte(fmt.Sprintf("%09d", iterations+i)))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	bb.Merge()

	bb.Close()

	os.RemoveAll("./merge.db")
}

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

func BenchmarkWrite1KBRecord(b *testing.B) {
	var value [1024]byte

	b.StopTimer()
	os.RemoveAll("./bench_write.db")
	bb, err := Open("./bench_write.db")
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := []byte(fmt.Sprintf("%d", i))
		b.StartTimer()
		if err := bb.Put(key, value[:]); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.StopTimer()
	os.RemoveAll("./bench_write.db")
	b.StartTimer()
}

func BenchmarkContinuousRead1KBRecord(b *testing.B) {
	var value [1024]byte

	b.StopTimer()
	os.RemoveAll("./bench_read.db")
	bb, err := Open("./bench_read.db")
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%d", i))
		if err := bb.Put(key, value[:]); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := []byte(fmt.Sprintf("%d", i))
		b.StartTimer()
		if _, err := bb.Get(key); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}

	b.StopTimer()
	os.RemoveAll("./bench_read.db")
	b.StartTimer()
}

func BenchmarkRandomRead1KBRecord(b *testing.B) {
	var value [1024]byte

	b.StopTimer()
	os.RemoveAll("./bench_read2.db")
	bb, err := Open("./bench_read2.db")
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%d", i))
		if err := bb.Put(key, value[:]); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		j := rand.Intn(b.N)
		key := []byte(fmt.Sprintf("%d", j))
		b.StartTimer()
		if _, err := bb.Get(key); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}

	b.StopTimer()
	os.RemoveAll("./bench_read2.db")
	b.StartTimer()
}
