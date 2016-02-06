// +build stress

package bitbutt

import (
	"bytes"
	cryptorand "crypto/rand"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func genRandomKeys() (keys [][]byte) {
	for i := 0; i < 10000; i++ {
		l := 5 + rand.Intn(50)
		key := make([]byte, l)
		if _, err := cryptorand.Read(key); err != nil {
			panic(err)
		}
		keys = append(keys, key)
	}
	return
}

func TestStress(t *testing.T) {
	os.RemoveAll("./stress.db")
	defer os.RemoveAll("./stress.db")

	bb, err := Open("./stress.db", SizeThreshold(10*KiB))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	keys := genRandomKeys()

	putChan := time.NewTicker(1 * time.Millisecond).C
	getChan := time.NewTicker(100 * time.Microsecond).C
	delChan := time.NewTicker(10 * time.Millisecond).C
	mergeChan := time.NewTicker(5 * time.Second).C

	quitChan := make(chan struct{})

	var wg sync.WaitGroup

	stressFunc := func() {
		defer wg.Done()

		for {
			i := rand.Intn(len(keys))
			select {
			case <-putChan:
				bb.Put(keys[i], append(keys[i], keys[i]...))
			case <-getChan:
				value, err := bb.Get(keys[i])
				if err == nil {
					expectedValue := append(keys[i], keys[i]...)
					if !bytes.Equal(value, expectedValue) {
						t.Errorf("Get failed: %q != %q", string(value), string(expectedValue))
					}
				}
			case <-delChan:
				bb.Delete(keys[i])
			case <-mergeChan:
				bb.Merge()
			case <-quitChan:
				return
			}
		}
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go stressFunc()
	}

	time.Sleep(1 * time.Minute)

	close(quitChan)

	wg.Wait()
	bb.Close()
}
