// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvbench

import (
	"path"

	"github.com/boltdb/bolt"
)

const (
	dbFileName = "bolt_store.db"
	bucketName = "bench"
)

func newBoltStore(dir string) keyValStore {
	db, err := bolt.Open(path.Join(dir, dbFileName), 0600, nil)
	if err != nil {
		panic(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		panic(err)
	}

	return boltStore{DB: db}
}

type boltStore struct {
	*bolt.DB
}

var _ keyValStore = boltStore{}

func (bs boltStore) get(key []byte) (val []byte, ok bool) {
	err := bs.DB.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(bucketName))
		v := b.Get(key)
		ok = v != nil
		if ok {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return
	})
	if err != nil {
		panic(err)
	}
	return val, ok
}

func (bs boltStore) put(key, val []byte) {
	err := bs.DB.Update(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(bucketName))
		return b.Put(key, val)
	})
	if err != nil {
		panic(err)
	}
}

func (bs boltStore) putMany(keys, vals [][]byte) {
	err := bs.DB.Update(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(bucketName))
		for i := range keys {
			err = b.Put(keys[i], vals[i])
			if err != nil {
				break
			}
		}
		return
	})
	if err != nil {
		panic(err)
	}
}

func (bs boltStore) delete(key []byte) {
	err := bs.DB.Update(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(bucketName))
		return b.Put(key, nil)
	})
	if err != nil {
		panic(err)
	}
}
