package canaldb

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func before(t *testing.T) *leveldb.DB {
	err := os.RemoveAll("./testdb/")
	if err != nil {
		t.Error(err)
	}

	leveldb, err := leveldb.OpenFile("./testdb", nil)
	if err != nil {
		t.Error(err)
	}

	return leveldb
}

func TestPutAndGetCurrent(t *testing.T) {
	leveldb := before(t)
	defer leveldb.Close()

	var err error

	db := NewCanalDB(leveldb)
	err = db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Millisecond)
	err = db.Put("test-namespace", "1")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Millisecond)
	err = db.Put("test-namespace", "-1")
	if err != nil {
		t.Error(err)
	}

	kv := db.GetCurrent("test-namespace")
	if !bytes.Equal(kv.Value, []byte("-1")) {
		t.Error("Failed putting")
	}
}

func TestPutWithDuplicatedValue(t *testing.T) {
	leveldb := before(t)
	defer leveldb.Close()

	var err error

	db := NewCanalDB(leveldb)
	err = db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Millisecond)
	err = db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}

	iter := db.searchEntriesWithPrefix("test-namespace")
	cnt := 0
	for iter.Next() {
		cnt++
	}
	if cnt != 1 {
		t.Error("Failed to prohibit duplicated value")
	}
}

func TestGetRange(t *testing.T) {
	leveldb := before(t)
	defer leveldb.Close()

	db := NewCanalDB(leveldb)

	for i := 1; i <= 5; i++ {
		time.Sleep(1 * time.Millisecond)
		db.Put("test-namespace", fmt.Sprintf("%d", i))
	}

	var kvs []KV

	// ASC
	kvs = db.GetRange("test-namespace", int64(0), getEpochMillis(), -1, false)
	if len(kvs) != 5 {
		t.Error()
	}
	if !bytes.Equal(kvs[0].Value, []byte("1")) {
		t.Error()
	}
	if !bytes.Equal(kvs[4].Value, []byte("5")) {
		t.Error()
	}

	// DESC
	kvs = db.GetRange("test-namespace", int64(0), getEpochMillis(), -1, true)
	if len(kvs) != 5 {
		t.Error()
	}
	if !bytes.Equal(kvs[0].Value, []byte("5")) {
		t.Error()
	}
	if !bytes.Equal(kvs[4].Value, []byte("1")) {
		t.Error()
	}

	// LIMIT
	kvs = db.GetRange("test-namespace", int64(0), getEpochMillis(), 3, false)
	if len(kvs) != 3 {
		t.Error()
	}
	if !bytes.Equal(kvs[0].Value, []byte("1")) {
		t.Error()
	}
	if !bytes.Equal(kvs[2].Value, []byte("3")) {
		t.Error()
	}
}
