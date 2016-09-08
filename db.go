package canaldb

import (
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type CanalDB struct {
	leveldb *leveldb.DB
}

func NewCanalDB(leveldb *leveldb.DB) *CanalDB {
	return &CanalDB{
		leveldb: leveldb,
	}
}

func (c *CanalDB) Put(namespace string, value string) error {
	ckv := c.GetCurrent(namespace)
	if ckv == nil || string(ckv.Value) != value {
		err := markNamespace(c.leveldb, namespace)
		if err != nil {
			return err
		}
		return c.leveldb.Put(makeCurrentKey(namespace), []byte(value), nil)
	}
	return nil
}

func (c *CanalDB) searchEntriesWithPrefix(namespace string) iterator.Iterator {
	return c.leveldb.NewIterator(util.BytesPrefix(makePrefix(namespace)), nil)
}

func (c *CanalDB) GetCurrent(namespace string) *KV {
	iter := c.searchEntriesWithPrefix(namespace)
	if iter.Last() {
		return &KV{iter.Key(), iter.Value()}
	}
	return nil
}

func (c *CanalDB) GetRange(namespace string, begin int64, end int64, num int64, desc bool) []*KV {
	var start []byte
	var limit []byte
	if desc {
		start = makeKey(namespace, end)
		limit = makeKey(namespace, begin)
	} else {
		start = makeKey(namespace, begin)
		limit = makeKey(namespace, end)
	}

	iter := c.leveldb.NewIterator(&util.Range{
		Start: start,
		Limit: limit,
	}, nil)

	isUnlimited := num < 0

	i := int64(0)
	var kvs []*KV
	if isUnlimited {
		kvs = make([]*KV, 100)
	} else {
		kvs = make([]*KV, num)
	}

	for iter.Next() {
		i++
		if !isUnlimited && i > num {
			break
		}
		kvs = append(kvs, &KV{iter.Key(), iter.Value()})
	}

	return kvs
}

func (c *CanalDB) Trim(namespace string, boundary int64) error {
	iter := c.leveldb.NewIterator(&util.Range{
		Start: []byte(makeOriginKey(namespace)),
		Limit: []byte(makeKey(namespace, boundary)),
	}, nil)

	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	for iter.Next() {
		wg.Add(1)
		go func() {
			err := c.leveldb.Delete(iter.Key(), nil)
			if err != nil {
				errChan <- err
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return <-errChan
}

func (c *CanalDB) GetNamaspaces() [][]byte {
	return fetchAllNamespaces(c.leveldb)
}

func (c *CanalDB) TrimAll(boundary int64) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	for _, namespace := range c.GetNamaspaces() {
		wg.Add(1)
		go func() {
			err := c.Trim(string(namespace), boundary)
			if err != nil {
				errChan <- err
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return <-errChan
}
