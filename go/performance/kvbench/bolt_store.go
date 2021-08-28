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
	"context"

	//"github.com/boltdb/bolt"
)

const (
	dbFileName = "bolt_store.db"
	bucketName = "bench"
)

func newBoltStore(ctx context.Context) keyValStore {
	panic("unimplemented")
	//db, err := bolt.Open(dbFileName, 0600, nil)
	//if err != nil {
	//	panic(err)
	//}
	//
	//err = db.Update(func(tx *bolt.Tx) error {
	//	_, err := tx.CreateBucketIfNotExists(bucketName)
	//	return err
	//})
	//if err != nil {
	//	panic(err)
	//}
	//
	//return boltStore{DB: db}
}

type boltStore struct {
	//*bolt.DB
}

var _ keyValStore = boltStore{}


func (bs boltStore) get(key []byte) (val []byte, ok bool) {
	panic("unimplemented")
	//_ = bs.DB.View(func(tx *bolt.Tx) (err error) {
	//	b := tx.Bucket(bucketName)
	//	val = b.Get(key)
	//	ok = val != nil
	//	return
	//})
	//return val, ok
}

func (bs boltStore) put(key, val []byte) {
	panic("unimplemented")
}

func (bs boltStore) delete(key []byte) {
	panic("unimplemented")
}

func (bs boltStore) load(key, val []byte) {
	panic("unimplemented")
}
