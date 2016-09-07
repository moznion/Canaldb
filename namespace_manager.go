package canaldb

import (
	"bytes"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const namespaceManagementKeyPrefix string = "_canaldb_namespaces"
const nsManagementMarker string = "1"

func markNamespace(leveldb *leveldb.DB, namespace string) error {
	return leveldb.Put(
		[]byte(fmt.Sprintf(namespaceManagementKeyPrefix+"_%s", namespace)),
		[]byte(nsManagementMarker),
		nil,
	)
}

func fetchAllNamespaces(leveldb *leveldb.DB) [][]byte {
	iter := leveldb.NewIterator(&util.Range{
		Start: []byte(namespaceManagementKeyPrefix + "_"),
		Limit: []byte(namespaceManagementKeyPrefix + "`"),
	}, nil)

	var namespaces [][]byte
	for iter.Next() {
		namespaces = append(
			namespaces,
			bytes.SplitN(iter.Key(), []byte("_"), 4)[3],
		)
	}
	return namespaces
}
