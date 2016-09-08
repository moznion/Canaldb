package canaldb

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
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
	_, err = db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Millisecond)
	_, err = db.Put("test-namespace", "1")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Millisecond)
	_, err = db.Put("test-namespace", "-1")
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
	expected, err := db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Millisecond)
	_, err = db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}

	iter := db.searchEntriesWithPrefix("test-namespace")
	defer iter.Release()

	cnt := 0
	for iter.Next() {
		cnt++
		if !bytes.Equal(iter.Value(), expected.Value) {
			t.Error()
		}
		if !bytes.Equal(iter.Key(), expected.Key) {
			t.Error()
		}
	}
	if cnt != 1 {
		t.Error("Failed to prohibit duplicated value")
	}
}

func TestGetRange(t *testing.T) {
	leveldb := before(t)
	defer leveldb.Close()

	db := NewCanalDB(leveldb)

	putKVs := make([]KV, 0, 5)
	for i := 1; i <= 5; i++ {
		time.Sleep(1 * time.Millisecond)
		kv, _ := db.Put("test-namespace", fmt.Sprintf("%d", i))
		putKVs = append(putKVs, *kv)
	}
	oldestKV := putKVs[0]
	latestKV := putKVs[4]

	var kvs []KV

	// ASC
	kvs = db.GetRange("test-namespace", int64(0), getEpochMillis(), -1, false)
	if len(kvs) != 5 {
		t.Error()
	}
	if !bytes.Equal(kvs[0].Key, oldestKV.Key) || !bytes.Equal(kvs[0].Value, oldestKV.Value) {
		t.Error()
	}
	if !bytes.Equal(kvs[4].Key, latestKV.Key) || !bytes.Equal(kvs[4].Value, latestKV.Value) {
		t.Error()
	}

	// DESC
	kvs = db.GetRange("test-namespace", int64(0), getEpochMillis(), -1, true)
	if len(kvs) != 5 {
		t.Error()
	}
	if !bytes.Equal(kvs[0].Key, latestKV.Key) || !bytes.Equal(kvs[0].Value, latestKV.Value) {
		t.Error()
	}
	if !bytes.Equal(kvs[4].Key, oldestKV.Key) || !bytes.Equal(kvs[4].Value, oldestKV.Value) {
		t.Error()
	}

	// LIMIT
	kvs = db.GetRange("test-namespace", int64(0), getEpochMillis(), 3, false)
	if len(kvs) != 3 {
		t.Error()
	}
	if !bytes.Equal(kvs[0].Key, oldestKV.Key) || !bytes.Equal(kvs[0].Value, oldestKV.Value) {
		t.Error()
	}
	if !bytes.Equal(kvs[2].Key, putKVs[2].Key) || !bytes.Equal(kvs[2].Value, putKVs[2].Value) {
		t.Error()
	}
}

func TestGetNamespaces(t *testing.T) {
	leveldb := before(t)
	defer leveldb.Close()

	db := NewCanalDB(leveldb)

	db.Put("test-namespace", "0")
	db.Put("test-namespace", "1")
	db.Put("test-namespace2", "")
	db.Put("test-namespace3", "")
	db.Put("many_many_underscores", "")

	nss := db.GetNamespaces()
	if len(nss) != 4 {
		t.Error()
	}

	if !bytes.Equal(nss[0], []byte("many_many_underscores")) {
		t.Error()
	}
	if !bytes.Equal(nss[1], []byte("test-namespace")) {
		t.Error()
	}
	if !bytes.Equal(nss[2], []byte("test-namespace2")) {
		t.Error()
	}
	if !bytes.Equal(nss[3], []byte("test-namespace3")) {
		fmt.Println(string(nss[2]))
		t.Error()
	}
}

func TestTrim(t *testing.T) {
	leveldb := before(t)
	defer leveldb.Close()

	db := NewCanalDB(leveldb)

	_, err := db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}

	time.Sleep(1 * time.Millisecond)
	_, err = db.Put("test-namespace", "1")
	if err != nil {
		t.Error(err)
	}

	time.Sleep(1 * time.Millisecond)
	targetKV, err := db.Put("test-namespace", "-1")
	if err != nil {
		t.Error(err)
	}
	targetKey := targetKV.Key
	timestamp, _ := strconv.ParseInt(string(bytes.Split(targetKey, []byte("_"))[1]), 10, 64)

	kvs := db.GetRange("test-namespace", int64(0), timestamp, -1, false)
	if len(kvs) != 3 {
		t.Error()
	}

	err = db.Trim("test-namespace", timestamp)
	if err != nil {
		t.Error(err)
	}

	kvs = db.GetRange("test-namespace", int64(0), timestamp, -1, false)
	if len(kvs) != 1 {
		t.Error()
	}

	kv := kvs[0]
	if !bytes.Equal(kv.Key, targetKey) || !bytes.Equal(kv.Value, targetKV.Value) {
		t.Error()
	}
}
