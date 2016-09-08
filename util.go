package canaldb

import (
	"fmt"
	"time"
)

func makePrefix(identifier string) []byte {
	return []byte(identifier + "_")
}

func makeKey(identifier string, timestamp int64) []byte {
	return []byte(fmt.Sprintf("%s_%d", identifier, timestamp))
}

func makeCurrentKey(identifier string) []byte {
	return makeKey(identifier, getEpochMillis())
}

func makeOriginKey(identifier string) []byte {
	return makeKey(identifier, 0)
}

func getEpochMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
