package canaldb

import (
	"bytes"
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
