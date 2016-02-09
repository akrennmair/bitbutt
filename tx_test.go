package bitbutt

import (
	"os"
	"testing"
)

func TestTransaction(t *testing.T) {
	os.RemoveAll("./trans1.db")

	bb, err := Open("./trans1.db", SizeThreshold(50))
	if err != nil {
		t.Fatal(err)
	}

	bb.Put([]byte("key1"), []byte("value1"))
	bb.Put([]byte("key2"), []byte("value2"))

	tx := bb.Begin()
	tx.Put([]byte("key3"), []byte("value3"))
	tx.Put([]byte("key2"), []byte("value4"))

	if v, err := tx.Get([]byte("key1")); err != nil || string(v) != "value1" {
		t.Fatalf("Expected key1=value1, got key1=%s instead.", string(v))
	}

	if v, err := tx.Get([]byte("key2")); err != nil || string(v) != "value4" {
		t.Fatalf("Expected key2=value4, got key2=%s instead (err = %v)", string(v), err)
	}

	if v, err := tx.Get([]byte("key3")); err != nil || string(v) != "value3" {
		t.Fatalf("Expected key3=value3, got key3=%s instead (err = %v)", string(v), err)
	}

	if v, err := tx.Get([]byte("invalid_key")); err == nil {
		t.Fatalf("Expected error, got %s instead.", string(v))
	}

	if err := tx.Delete([]byte("key1")); err != nil {
		t.Fatalf("Delete key1 failed: %v", err)
	}

	if v, err := tx.Get([]byte("key1")); err == nil {
		t.Fatalf("Expected error, got %s instead.", string(v))
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if v, err := bb.Get([]byte("key1")); err == nil {
		t.Fatalf("Expected error, got %s instead.", string(v))
	}

	if v, err := bb.Get([]byte("key2")); err != nil || string(v) != "value4" {
		t.Fatalf("Expected key2=value4, got key2=%s instead (err = %v)", string(v), err)
	}

	if v, err := bb.Get([]byte("key3")); err != nil || string(v) != "value3" {
		t.Fatalf("Expected key3=value3, got key3=%s instead (err = %v)", string(v), err)
	}

	if v, err := tx.Get([]byte("key2")); err == nil {
		t.Fatalf("Expected error from transaction, got %v instead.", string(v))
	}

	os.RemoveAll("./trans1.db")
}

func TestRollback(t *testing.T) {
	os.RemoveAll("./trans2.db")

	bb, err := Open("./trans2.db", SizeThreshold(50))
	if err != nil {
		t.Fatal(err)
	}

	bb.Put([]byte("key1"), []byte("value1"))

	tx := bb.Begin()

	tx.Put([]byte("key1"), []byte("value2"))
	tx.Put([]byte("key2"), []byte("value3"))

	tx.Rollback()

	if err := tx.Put([]byte("key3"), []byte("value4")); err == nil {
		t.Fatalf("Expected error, got none.")
	}

	if v, err := bb.Get([]byte("key1")); err != nil || string(v) != "value1" {
		t.Fatalf("Expected key1=value1, got key1=%s instead (err = %v).", string(v), err)
	}

	if v, err := bb.Get([]byte("key2")); err == nil {
		t.Fatalf("Expected error, got key2=%s instead.", string(v))
	}

	os.RemoveAll("./trans2.db")
}

func TestUpdateConflict(t *testing.T) {
	os.RemoveAll("./trans3.db")

	bb, err := Open("./trans3.db", SizeThreshold(50))
	if err != nil {
		t.Fatal(err)
	}

	bb.Put([]byte("key1"), []byte("value1"))

	tx := bb.Begin()

	// we update the same key in both bb and tx, this means there's a conflict.
	bb.Put([]byte("key1"), []byte("value2"))
	tx.Put([]byte("key1"), []byte("value3"))

	if err := tx.Commit(); err == nil {
		t.Fatalf("Expected update conflict error, got none.")
	}

	os.RemoveAll("./trans3.db")
}
