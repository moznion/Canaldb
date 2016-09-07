package canaldb

import (
	"fmt"
	"time"
)

func makeKey(identifier string, timestamp int64) []byte {
	return []byte(fmt.Sprintf("%s_%d", identifier, timestamp))
}

func makeCurrentKey(identifier string) []byte {
	return makeKey(identifier, getEpochMillis())
}

func makeOriginKey(identifier string) []byte {
	return makeKey("%s", 0)
}

func getEpochMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
