package canaldb

type Entry struct {
	Key       []byte
	Value     []byte
	timestamp int64
	namespace []byte
}
