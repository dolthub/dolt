// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type rateLimitedLevelDB struct {
	*leveldb.DB
	concurrentFileIOLimit chan struct{}
}

func (db *rateLimitedLevelDB) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	db.concurrentFileIOLimit <- struct{}{}
	defer func() { <-db.concurrentFileIOLimit }()
	return db.DB.Get(key, ro)
}

func (db *rateLimitedLevelDB) Has(key []byte, ro *opt.ReadOptions) (ret bool, err error) {
	db.concurrentFileIOLimit <- struct{}{}
	defer func() { <-db.concurrentFileIOLimit }()
	return db.DB.Has(key, ro)
}

func (db *rateLimitedLevelDB) Put(key, value []byte, wo *opt.WriteOptions) error {
	db.concurrentFileIOLimit <- struct{}{}
	defer func() { <-db.concurrentFileIOLimit }()
	return db.DB.Put(key, value, wo)
}

func (db *rateLimitedLevelDB) Write(b *leveldb.Batch, wo *opt.WriteOptions) (err error) {
	db.concurrentFileIOLimit <- struct{}{}
	defer func() { <-db.concurrentFileIOLimit }()
	return db.DB.Write(b, wo)
}
