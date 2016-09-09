package canaldb

type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Namespace []byte
}
