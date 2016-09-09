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

func (c *CanalDB) Put(namespace, value string) (*KV, error) {
	ckv := c.GetCurrent(namespace)
	if ckv == nil || string(ckv.Value) != value {
		batch := new(leveldb.Batch)

		markNamespace(batch, namespace)

		k := makeCurrentKey(namespace)
		v := []byte(value)
		batch.Put(k, v)

		if err := c.leveldb.Write(batch, nil); err != nil {
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

func (c *CanalDB) GetRange(namespace string, begin, end, num int64, desc bool) []KV {
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
		Limit: []byte(makeKey(namespace, boundary+1)), // +1: to include in the range
	}, nil)

	var wg sync.WaitGroup

	batch := new(leveldb.Batch)
	for iter.Next() {
		wg.Add(1)
		go func(key []byte) {
			batch.Delete(key)
			wg.Done()
		}(cloneBytes(iter.Key()))
	}

	wg.Wait()

	iter.Last()
	lastValue := iter.Value()
	if lastValue != nil {
		batch.Put(makeKey(namespace, boundary), lastValue)
	}

	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}

	return c.leveldb.Write(batch, nil)
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
			if err := c.Trim(string(namespace), boundary); err != nil {
				errChan <- err
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return <-errChan
}
