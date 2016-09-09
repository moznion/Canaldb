package canaldb

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

func makePrefix(namespace string) []byte {
	return []byte(namespace + "|")
}

func makeKey(namespace string, timestamp int64) []byte {
	return []byte(fmt.Sprintf("%s|%d", namespace, timestamp))
}

func makeCurrentKey(namespace string) []byte {
	return makeKey(namespace, getEpochMillis())
}

func makeOriginKey(namespace string) []byte {
	return makeKey(namespace, 0)
}

func getEpochMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func splitKey(k []byte) ([]byte, int64) {
	sep := []byte("|")

	split := bytes.Split(k, sep)
	// return split[0], int64(binary.LittleEndian.Uint64(split[1]))
	ts, _ := strconv.ParseInt(string(split[len(split)-1]), 10, 64)
	return bytes.Join(split[:len(split)-1], sep), ts
}
