package canaldb

import (
	"bytes"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const namespaceManagementKeyPrefix string = "_canaldb|namespaces"
const nsManagementMarker string = "1"

func markNamespace(batch *leveldb.Batch, namespace string) {
	batch.Put(
		[]byte(fmt.Sprintf(namespaceManagementKeyPrefix+"|%s", namespace)),
		[]byte(nsManagementMarker),
	)
}

func fetchAllNamespaces(leveldb *leveldb.DB) ([][]byte, error) {
	iter := leveldb.NewIterator(util.BytesPrefix([]byte(namespaceManagementKeyPrefix+"|")), nil)

	var namespaces [][]byte
	for iter.Next() {
		ns := cloneBytes(bytes.SplitN(iter.Key(), []byte("|"), 3)[2])
		namespaces = append(namespaces, ns)
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return namespaces, nil
}
