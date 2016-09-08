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

func (c *CanalDB) Put(namespace string, value string) (*KV, error) {
	ckv := c.GetCurrent(namespace)
	if ckv == nil || string(ckv.Value) != value {
		var err error
		err = markNamespace(c.leveldb, namespace)
		if err != nil {
			return nil, err
		}

		k := makeCurrentKey(namespace)
		v := []byte(value)
		err = c.leveldb.Put(k, v, nil)
		if err != nil {
			return nil, err
		}
		return &KV{k, v}, nil
	}
	return ckv, nil
}

func (c *CanalDB) searchEntriesWithPrefix(namespace string) iterator.Iterator {
	return c.leveldb.NewIterator(util.BytesPrefix(makePrefix(namespace)), nil)
}

func (c *CanalDB) GetCurrent(namespace string) *KV {
	iter := c.searchEntriesWithPrefix(namespace)
	defer iter.Release()
	if iter.Last() {
		return &KV{iter.Key(), iter.Value()}
	}
	return nil
}

func (c *CanalDB) GetRange(namespace string, begin int64, end int64, num int64, desc bool) []KV {
	isUnlimited := num < 0

	var kvs []KV
	if isUnlimited {
		kvs = make([]KV, 0)
	} else {
		kvs = make([]KV, 0, num)
	}

	end++ // to include in the rarnge

	iter := c.leveldb.NewIterator(&util.Range{
		Start: makeKey(namespace, begin),
		Limit: makeKey(namespace, end),
	}, nil)
	defer iter.Release()

	var edgeJumper func() bool
	var seeker func() bool
	if desc {
		edgeJumper = iter.Last
		seeker = iter.Prev
	} else {
		edgeJumper = iter.First
		seeker = iter.Next
	}

	i := int64(0)

	if edgeJumper() {
		i++
		if isUnlimited || i <= num {
			kvs = append(kvs, KV{cloneBytes(iter.Key()), cloneBytes(iter.Value())})
		}
	}

	for seeker() {
		i++
		if !isUnlimited && i > num {
			break
		}
		kvs = append(kvs, KV{cloneBytes(iter.Key()), cloneBytes(iter.Value())})
	}

	return kvs
}

func (c *CanalDB) Trim(namespace string, boundary int64) error {
	iter := c.leveldb.NewIterator(&util.Range{
		Start: []byte(makeOriginKey(namespace)),
		Limit: []byte(makeKey(namespace, boundary)),
	}, nil)
	defer iter.Release()

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

func (c *CanalDB) GetNamespaces() [][]byte {
	return fetchAllNamespaces(c.leveldb)
}

func (c *CanalDB) TrimAll(boundary int64) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	for _, namespace := range c.GetNamespaces() {
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
