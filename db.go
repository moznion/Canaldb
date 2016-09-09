package canaldb

import (
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

func (c *CanalDB) Put(namespace, value string) (*Entry, error) {
	currentEntry := c.GetCurrent(namespace)
	if currentEntry == nil || string(currentEntry.Value) != value {
		batch := new(leveldb.Batch)

		markNamespace(batch, namespace)

		k := makeCurrentKey(namespace)
		v := []byte(value)
		batch.Put(k, v)

		if err := c.leveldb.Write(batch, nil); err != nil {
			return nil, err
		}
		return &Entry{k, v, 0, nil}, nil // TODO
	}
	return currentEntry, nil
}

func (c *CanalDB) searchEntriesWithPrefix(namespace string) iterator.Iterator {
	return c.leveldb.NewIterator(util.BytesPrefix(makePrefix(namespace)), nil)
}

func (c *CanalDB) GetCurrent(namespace string) *Entry {
	iter := c.searchEntriesWithPrefix(namespace)
	defer iter.Release()
	if iter.Last() {
		return &Entry{iter.Key(), iter.Value(), 0, nil} // TODO
	}
	return nil
}

func (c *CanalDB) GetRange(namespace string, begin, end, num int64, desc bool) []Entry {
	isUnlimited := num < 0

	var entries []Entry
	if isUnlimited {
		entries = make([]Entry, 0)
	} else {
		entries = make([]Entry, 0, num)
	}

	iter := c.leveldb.NewIterator(&util.Range{
		Start: makeKey(namespace, begin),
		Limit: makeKey(namespace, end+1), // +1: to include in the rarnge
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
			entries = append(entries, Entry{cloneBytes(iter.Key()), cloneBytes(iter.Value()), 0, nil}) // TODO
		}
	}

	for seeker() {
		i++
		if !isUnlimited && i > num {
			break
		}
		entries = append(entries, Entry{cloneBytes(iter.Key()), cloneBytes(iter.Value()), 0, nil}) // TODO
	}

	return entries
}

func (c *CanalDB) trim(batch *leveldb.Batch, namespace string, boundary int64) error {
	iter := c.leveldb.NewIterator(&util.Range{
		Start: []byte(makeOriginKey(namespace)),
		Limit: []byte(makeKey(namespace, boundary+1)), // +1: to include in the range
	}, nil)

	for iter.Next() {
		batch.Delete(cloneBytes(iter.Key()))
	}

	iter.Last()
	lastValue := iter.Value()
	if lastValue != nil {
		batch.Put(makeKey(namespace, boundary), lastValue)
	}

	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}

	return nil
}

func (c *CanalDB) Trim(namespace string, boundary int64) error {
	batch := new(leveldb.Batch)
	if err := c.trim(batch, namespace, boundary); err != nil {
		return err
	}

	return c.leveldb.Write(batch, nil)
}

func (c *CanalDB) GetNamespaces() ([][]byte, error) {
	return fetchAllNamespaces(c.leveldb)
}

func (c *CanalDB) TrimAll(boundary int64) error {
	namespaces, err := c.GetNamespaces()
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	for _, namespace := range namespaces {
		if err := c.trim(batch, string(namespace), boundary); err != nil {
			return err
		}
	}

	return c.leveldb.Write(batch, nil)
}
