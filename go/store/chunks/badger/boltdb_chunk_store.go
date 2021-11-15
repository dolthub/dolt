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

package badger

import (
	"context"

	badger "github.com/dgraph-io/badger/v3"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const dbDir = "badger_chunk_store"

var (
	rootKey  = []byte("root")
	csBucket = []byte("cs")
)

var db *badger.DB

func NewBadgerChunkStore(ctx context.Context, dir string) (cs BadgerChunkStore, err error) {
	// TODO(andy): this is gross and bad
	if db == nil {
		db, err = badger.Open(badger.DefaultOptions(dbDir))
		if err != nil {
			return cs, err
		}
	}

	if err = maybeInitBoltStore(db); err != nil {
		return cs, err
	}

	cs = BadgerChunkStore{DB: db}
	return cs, nil
}

func maybeInitBoltStore(db *badger.DB) error {
	return db.Update(func(tx *badger.Txn) (err error) {
		_, err = tx.Get(rootKey)
		if isAbsent(err) {
			err = tx.Set(rootKey, byteStr(hash.Hash{}))
		}
		return err
	})
}

type BadgerChunkStore struct {
	*badger.DB
}

var _ chunks.ChunkStore = BadgerChunkStore{}
var _ chunks.ChunkStoreGarbageCollector = BadgerChunkStore{}

//var _ chunks.LoggingChunkStore = BadgerChunkStore{}

// Get the Chunk for the value of the hash in the store. If the hash is
// isAbsent from the store EmptyChunk is returned.
func (b BadgerChunkStore) Get(ctx context.Context, h hash.Hash) (c chunks.Chunk, err error) {
	err = b.DB.View(func(tx *badger.Txn) error {
		var item *badger.Item
		item, err = tx.Get(byteStr(h))
		if isAbsent(err) {
			c = chunks.EmptyChunk
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) (e error) {
			// |val| is read-only
			c = chunks.CopyNewChunk(val)
			return
		})
	})
	return
}

// GetMany gets the Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (b BadgerChunkStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	return b.DB.View(func(tx *badger.Txn) (err error) {
		var item *badger.Item
		for h := range hashes {
			item, err = tx.Get(byteStr(h))
			if isAbsent(err) {
				continue
			}
			if err != nil {
				return err
			}

			err = item.Value(func(val []byte) (e error) {
				// |val| is read-only
				c := chunks.CopyNewChunk(val)
				found(ctx, &c)
				return
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Has Returns true iff the value at the address |h| is contained in the store
func (b BadgerChunkStore) Has(ctx context.Context, h hash.Hash) (ok bool, err error) {
	err = b.DB.View(func(tx *badger.Txn) error {
		_, err = tx.Get(byteStr(h))
		if err == nil {
			ok = true
		} else if isAbsent(err) {
			ok = false
			err = nil
		}
		return err
	})
	return
}

// HasMany Returns a new HashSet containing any members of |hashes| that are isAbsent from the store.
func (b BadgerChunkStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	absent = hash.NewHashSet()
	err = b.DB.View(func(tx *badger.Txn) error {
		for h := range hashes {
			_, err = tx.Get(byteStr(h))
			if err == nil {
				continue
			}
			if isAbsent(err) {
				absent.Insert(h)
				continue
			}
			return err
		}
		return nil
	})
	return
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (b BadgerChunkStore) Put(ctx context.Context, c chunks.Chunk) error {
	return b.DB.Update(func(tx *badger.Txn) error {
		return tx.Set(byteStr(c.Hash()), c.Data())
	})
}

// Version Returns the NomsVersion with which this ChunkSource is compatible.
func (b BadgerChunkStore) Version() string {
	// todo: fix
	return "__LD_1__" // pretend to be NBS
}

// Rebase brings this chunks.ChunkStore into sync with the persistent storage's
// current root.
func (b BadgerChunkStore) Rebase(ctx context.Context) error {
	return nil
}

// Root returns the root of the database as of the time the chunks.ChunkStore
// was opened or the most recent call to Rebase.
func (b BadgerChunkStore) Root(ctx context.Context) (root hash.Hash, err error) {
	err = b.DB.Update(func(tx *badger.Txn) error {
		var item *badger.Item
		if item, err = tx.Get(rootKey); err != nil {
			return err
		}
		return item.Value(func(val []byte) (err error) {
			// |val| is read-only
			copy(root[:], val)
			return
		})
	})
	return
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (b BadgerChunkStore) Commit(ctx context.Context, current, last hash.Hash) (ok bool, err error) {
	err = b.DB.Update(func(tx *badger.Txn) error {
		var item *badger.Item
		if item, err = tx.Get(rootKey); err != nil {
			return err
		}

		// Check
		err = item.Value(func(val []byte) (err error) {
			ok = last.Equal(hash.New(val))
			return
		})
		if err != nil {
			return err
		}

		// Set
		if ok {
			err = tx.Set(rootKey, byteStr(current))
		}
		return err
	})
	return
}

// Stats may return some kind of struct that reports statistics about the
// chunks.ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (b BadgerChunkStore) Stats() interface{} {
	return nil
}

// StatsSummary may return a string containing summarized statistics for
// this chunks.ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (b BadgerChunkStore) StatsSummary() string {
	return ""
}

// Close tears down any resources in use by the implementation. After
// Close(), the chunks.ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other chunks.ChunkStore method; behavior is
// undefined and probably crashy.
func (b BadgerChunkStore) Close() error {
	return b.DB.Close()
}

//func (b BadgerChunkStore) SetLogger(logger chunks.DebugLogger) {}

// MarkAndSweepChunks expects |keepChunks| to receive the chunk hashes
// that should be kept in the chunk store. Once |keepChunks| is closed
// and MarkAndSweepChunks returns, the chunk store will only have the
// chunks sent on |keepChunks| and will have removed all other content
// from the chunks.ChunkStore.
func (b BadgerChunkStore) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan []hash.Hash, dest chunks.ChunkStore) error {
	// todo
	return nil
}

func byteStr(h hash.Hash) []byte {
	bb := [20]byte(h)
	return bb[:]
}

func isAbsent(err error) bool {
	return err == badger.ErrKeyNotFound
}
