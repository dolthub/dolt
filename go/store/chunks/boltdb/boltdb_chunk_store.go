// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package boltdb

import (
	"context"

	"github.com/boltdb/bolt"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const dbFile = "bolt_chunk_store.db"

var (
	rootKey  = []byte("root")
	csBucket = []byte("cs")
)

func NewBoltDBChunkStore(ctx context.Context, dir string) (cs BoltDBChunkStore, err error) {
	var db *bolt.DB
	db, err = bolt.Open(dbFile, 0600, nil)
	if err != nil {
		return cs, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(csBucket)
		if err != nil {
			return err
		}

		if b.Get(rootKey) == nil {
			// write a new root if it doesn't exist
			err = b.Put(rootKey, byteStr(hash.Hash{}))
		}
		return err
	})
	if err != nil {
		return cs, err
	}

	cs = BoltDBChunkStore{DB: db}
	return cs, nil
}

type BoltDBChunkStore struct {
	*bolt.DB
}

var _ chunks.ChunkStore = BoltDBChunkStore{}
var _ chunks.ChunkStoreGarbageCollector = BoltDBChunkStore{}

//var _ chunks.LoggingChunkStore = BoltDBChunkStore{}

// Get the Chunk for the value of the hash in the store. If the hash is
// absent from the store EmptyChunk is returned.
func (b BoltDBChunkStore) Get(ctx context.Context, h hash.Hash) (c chunks.Chunk, err error) {
	err = b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(csBucket)
		val := b.Get(byteStr(h))
		if val == nil {
			c = chunks.EmptyChunk
		}
		c = chunks.NewChunk(val)
		return nil
	})
	return
}

// GetMany gets the Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (b BoltDBChunkStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(*chunks.Chunk)) error {
	return b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(csBucket)
		for h := range hashes {
			val := b.Get(byteStr(h))
			// nil value return signals absence
			if val == nil {
				continue
			}
			c := chunks.NewChunk(val)
			found(&c)
		}
		return nil
	})
}

// Has Returns true iff the value at the address |h| is contained in the store
func (b BoltDBChunkStore) Has(ctx context.Context, h hash.Hash) (ok bool, err error) {
	err = b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(csBucket)
		val := b.Get(byteStr(h))
		// nil value return signals absence
		ok = val != nil
		return nil
	})
	return
}

// HasMany Returns a new HashSet containing any members of |hashes| that are absent from the store.
func (b BoltDBChunkStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	absent = hash.NewHashSet()
	err = b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(csBucket)
		for h := range hashes {
			val := b.Get(byteStr(h))
			// nil value return signals absence
			if val == nil {
				absent.Insert(h)
			}
		}
		return nil
	})
	return
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (b BoltDBChunkStore) Put(ctx context.Context, c chunks.Chunk) error {
	return b.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(csBucket)
		return b.Put(byteStr(c.Hash()), c.Data())
	})
}

// Version Returns the NomsVersion with which this ChunkSource is compatible.
func (b BoltDBChunkStore) Version() string {
	// todo: fix
	return "__LD_1__" // pretend to be NBS
}

// Rebase brings this chunks.ChunkStore into sync with the persistent storage's
// current root.
func (b BoltDBChunkStore) Rebase(ctx context.Context) error {
	return nil
}

// Root returns the root of the database as of the time the chunks.ChunkStore
// was opened or the most recent call to Rebase.
func (b BoltDBChunkStore) Root(ctx context.Context) (root hash.Hash, err error) {
	err = b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(csBucket)
		root = hash.New(b.Get(rootKey))
		return nil
	})
	return
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (b BoltDBChunkStore) Commit(ctx context.Context, current, last hash.Hash) (ok bool, err error) {
	err = b.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(csBucket)
		root := hash.New(b.Get(rootKey))
		if !root.Equal(last) {
			ok = false
			return nil
		}
		return b.Put(rootKey, byteStr(current))
	})
	return
}

// Stats may return some kind of struct that reports statistics about the
// chunks.ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (b BoltDBChunkStore) Stats() interface{} {
	return nil
}

// StatsSummary may return a string containing summarized statistics for
// this chunks.ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (b BoltDBChunkStore) StatsSummary() string {
	return ""
}

// Close tears down any resources in use by the implementation. After
// Close(), the chunks.ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other chunks.ChunkStore method; behavior is
// undefined and probably crashy.
func (b BoltDBChunkStore) Close() error {
	return b.DB.Close()
}

//func (b BoltDBChunkStore) SetLogger(logger chunks.DebugLogger) {}

// MarkAndSweepChunks expects |keepChunks| to receive the chunk hashes
// that should be kept in the chunk store. Once |keepChunks| is closed
// and MarkAndSweepChunks returns, the chunk store will only have the
// chunks sent on |keepChunks| and will have removed all other content
// from the chunks.ChunkStore.
func (b BoltDBChunkStore) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan []hash.Hash, dest chunks.ChunkStore) error {
	// todo
	return nil
}

func byteStr(h hash.Hash) []byte {
	bb := [20]byte(h)
	return bb[:]
}
