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
	if err := os.RemoveAll("./testdb/"); err != nil {
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

	entry := db.GetCurrent("test-namespace")
	if !bytes.Equal(entry.Value, []byte("-1")) {
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
			t.Error("")
		}
		if !bytes.Equal(iter.Key(), expected.Key) {
			t.Error("")
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

	putEntries := make([]Entry, 0, 5)
	for i := 1; i <= 5; i++ {
		time.Sleep(1 * time.Millisecond)
		entry, _ := db.Put("test-namespace", fmt.Sprintf("%d", i))
		putEntries = append(putEntries, *entry)
	}
	oldestEntry := putEntries[0]
	latestEntry := putEntries[4]

	var entries []Entry

	// ASC
	entries = db.GetRange("test-namespace", int64(0), getEpochMillis(), -1, false)
	if len(entries) != 5 {
		t.Error("")
	}
	if !bytes.Equal(entries[0].Key, oldestEntry.Key) || !bytes.Equal(entries[0].Value, oldestEntry.Value) {
		t.Error("")
	}
	if !bytes.Equal(entries[4].Key, latestEntry.Key) || !bytes.Equal(entries[4].Value, latestEntry.Value) {
		t.Error("")
	}

	// DESC
	entries = db.GetRange("test-namespace", int64(0), getEpochMillis(), -1, true)
	if len(entries) != 5 {
		t.Error("")
	}
	if !bytes.Equal(entries[0].Key, latestEntry.Key) || !bytes.Equal(entries[0].Value, latestEntry.Value) {
		t.Error("")
	}
	if !bytes.Equal(entries[4].Key, oldestEntry.Key) || !bytes.Equal(entries[4].Value, oldestEntry.Value) {
		t.Error("")
	}

	// LIMIT
	entries = db.GetRange("test-namespace", int64(0), getEpochMillis(), 3, false)
	if len(entries) != 3 {
		t.Error("")
	}
	if !bytes.Equal(entries[0].Key, oldestEntry.Key) || !bytes.Equal(entries[0].Value, oldestEntry.Value) {
		t.Error("")
	}
	if !bytes.Equal(entries[2].Key, putEntries[2].Key) || !bytes.Equal(entries[2].Value, putEntries[2].Value) {
		t.Error("")
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
	db.Put("many|many|pipes", "")

	nss, _ := db.GetNamespaces()
	if len(nss) != 4 {
		t.Error("")
	}

	if !bytes.Equal(nss[0], []byte("many|many|pipes")) {
		t.Error("")
	}
	if !bytes.Equal(nss[1], []byte("test-namespace")) {
		t.Error("")
	}
	if !bytes.Equal(nss[2], []byte("test-namespace2")) {
		t.Error("")
	}
	if !bytes.Equal(nss[3], []byte("test-namespace3")) {
		fmt.Println(string(nss[2]))
		t.Error("")
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
	targetEntry, err := db.Put("test-namespace", "-1")
	if err != nil {
		t.Error(err)
	}
	targetKey := targetEntry.Key
	timestamp, _ := strconv.ParseInt(string(bytes.Split(targetKey, []byte("|"))[1]), 10, 64)

	entries := db.GetRange("test-namespace", int64(0), timestamp, -1, false)
	if len(entries) != 3 {
		t.Error("")
	}

	err = db.Trim("test-namespace", timestamp)
	if err != nil {
		t.Error(err)
	}

	entries = db.GetRange("test-namespace", int64(0), timestamp, -1, false)
	if len(entries) != 1 {
		t.Error("")
	}

	entry := entries[0]
	if !bytes.Equal(entry.Key, targetKey) || !bytes.Equal(entry.Value, targetEntry.Value) {
		t.Error("")
	}
}

func TestTrimAll(t *testing.T) {
	leveldb := before(t)
	defer leveldb.Close()

	db := NewCanalDB(leveldb)

	_, err := db.Put("test-namespace", "0")
	if err != nil {
		t.Error(err)
	}
	_, err = db.Put("test-namespace2", "0")
	if err != nil {
		t.Error(err)
	}

	time.Sleep(1 * time.Millisecond)
	_, err = db.Put("test-namespace", "1")
	if err != nil {
		t.Error(err)
	}
	_, err = db.Put("test-namespace2", "1")
	if err != nil {
		t.Error(err)
	}

	time.Sleep(1 * time.Millisecond)
	_, err = db.Put("test-namespace", "2")
	if err != nil {
		t.Error(err)
	}
	targetEntry, err := db.Put("test-namespace2", "2")
	if err != nil {
		t.Error(err)
	}

	targetKey := targetEntry.Key
	timestamp, _ := strconv.ParseInt(string(bytes.Split(targetKey, []byte("|"))[1]), 10, 64)

	time.Sleep(1 * time.Millisecond)
	_, err = db.Put("test-namespace", "3")
	if err != nil {
		t.Error(err)
	}
	_, err = db.Put("test-namespace2", "3")
	if err != nil {
		t.Error(err)
	}

	err = db.TrimAll(timestamp)
	if err != nil {
		t.Error(err)
	}

	entries := db.GetRange("test-namespace", int64(0), timestamp, -1, false)
	if len(entries) != 1 {
		t.Error("Failed to trim all")
	}

	entries = db.GetRange("test-namespace2", int64(0), timestamp, -1, false)
	if len(entries) != 1 {
		t.Error("Failed to trim all")
	}

	entry := db.GetCurrent("test-namespace")
	if !bytes.Equal(entry.Value, []byte("3")) {
		t.Error("")
	}

	entry = db.GetCurrent("test-namespace2")
	if !bytes.Equal(entry.Value, []byte("3")) {
		t.Error("")
	}
}
