// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"sync/atomic"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/syndtr/goleveldb/leveldb"
	ldbIterator "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const uint32Size = 4

type opCacheStore interface {
	opCache() opCache
	destroy() error
}

type opCache interface {
	MapSet(mapKey Value, mapVal Value)
	SetInsert(val Value)
	NewIterator() opCacheIterator
}

type opCacheIterator interface {
	MapOp() sequenceItem
	SetOp() sequenceItem
	Next() bool
	Release()
}

type ldbOpCacheStore struct {
	ldb          *leveldb.DB
	dbDir        string
	collectionId uint32
	vrw          ValueReadWriter
}

type ldbOpCache struct {
	vrw   ValueReadWriter
	colId uint32
	ldb   *leveldb.DB
}

type ldbOpCacheIterator struct {
	iter ldbIterator.Iterator
	vr   ValueReader
}

func newLdbOpCacheStore(vrw ValueReadWriter) *ldbOpCacheStore {
	dir, err := ioutil.TempDir("", "")
	d.Chk.NoError(err)
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Comparer:               opCacheComparer{},
		OpenFilesCacheCapacity: 24,
		NoSync:                 true,    // We don't need this data to be durable. LDB is acting as temporary storage that can be larger than main memory.
		WriteBuffer:            1 << 27, // 128MiB
	})
	d.Chk.NoError(err, "opening put cache in %s", dir)
	return &ldbOpCacheStore{ldb: db, dbDir: dir, vrw: vrw}
}

func (store *ldbOpCacheStore) destroy() error {
	d.Chk.NoError(store.ldb.Close())
	return os.RemoveAll(store.dbDir)
}

func (store *ldbOpCacheStore) opCache() opCache {
	colId := atomic.AddUint32(&store.collectionId, 1)
	return &ldbOpCache{vrw: store.vrw, colId: colId, ldb: store.ldb}
}

// Set can be called from any goroutine
func (opc *ldbOpCache) MapSet(mapKey Value, mapVal Value) {
	mapKeyArray := [initialBufferSize]byte{}
	mapValArray := [initialBufferSize]byte{}

	switch mapKey.Type().Kind() {
	default:
		ldbKey := ldbKeyFromValueHash(mapKey, opc.colId)

		// Since we've used the ref of keyValue as our ldbKey. We need to store mapKey and mapVal in the ldb value. We use the following format for that:
		//
		//     uint32 (4 bytes)             bytes                 bytes
		// +-----------------------+---------------------+----------------------+
		// | key serialization len |    serialized key   |   serialized value   |
		// +-----------------------+---------------------+----------------------+

		encodedMapKey := encToSlice(mapKey, mapKeyArray[:], opc.vrw)
		encodedMapVal := encToSlice(mapVal, mapValArray[:], opc.vrw)
		ldbValueArray := [initialBufferSize * 2]byte{}
		binary.LittleEndian.PutUint32(ldbValueArray[:], uint32(len(encodedMapKey)))
		ldbValue := ldbValueArray[0:4]
		ldbValue = append(ldbValue, encodedMapKey...)
		ldbValue = append(ldbValue, encodedMapVal...)

		// TODO: Will manually batching calls to ldb.Put() help?
		err := opc.ldb.Put(ldbKey, ldbValue, nil)
		d.Chk.NoError(err)

	case BoolKind, NumberKind, StringKind:
		ldbKey := ldbKeyFromValue(mapKey, opc.colId, opc.vrw)
		encodedMapVal := encToSlice(mapVal, mapValArray[:], opc.vrw)
		err := opc.ldb.Put(ldbKey, encodedMapVal, nil)
		d.Chk.NoError(err)
	}
}

// Note that, if 'v' are prolly trees, any in-memory child chunks will be written to vw at this time.
func encToSlice(v Value, initBuf []byte, vw ValueWriter) []byte {
	// TODO: Are there enough calls to this that it's worth re-using a nomsWriter and valueEncoder?
	w := &binaryNomsWriter{initBuf, 0}
	enc := newValueEncoder(w, vw)
	enc.writeValue(v)
	return w.data()
}

func (opc *ldbOpCache) NewIterator() opCacheIterator {
	prefix := [4]byte{}
	binary.LittleEndian.PutUint32(prefix[:], opc.colId)
	return &ldbOpCacheIterator{iter: opc.ldb.NewIterator(util.BytesPrefix(prefix[:]), nil), vr: opc.vrw}
}

func (i *ldbOpCacheIterator) Next() bool {
	return i.iter.Next()
}

func (i *ldbOpCacheIterator) MapOp() sequenceItem {
	entry := mapEntry{}
	ldbKey := i.iter.Key()
	ldbValue := i.iter.Value()
	switch NomsKind(ldbKey[uint32Size]) {
	case BoolKind, NumberKind, StringKind:
		entry.key = DecodeFromBytes(ldbKey[uint32Size:], i.vr, staticTypeCache)
		entry.value = DecodeFromBytes(ldbValue, i.vr, staticTypeCache)
	default:
		keyBytesLen := int(binary.LittleEndian.Uint32(ldbValue))
		entry.key = DecodeFromBytes(ldbValue[uint32Size:uint32Size+keyBytesLen], i.vr, staticTypeCache)
		entry.value = DecodeFromBytes(ldbValue[uint32Size+keyBytesLen:], i.vr, staticTypeCache)
	}

	return entry
}

// Insert can be called from any goroutine
func (opc *ldbOpCache) SetInsert(val Value) {
	switch val.Type().Kind() {
	default:
		ldbKey := ldbKeyFromValueHash(val, opc.colId)
		valArray := [initialBufferSize]byte{}
		encodedVal := encToSlice(val, valArray[:], opc.vrw)
		err := opc.ldb.Put(ldbKey, encodedVal, nil)
		d.Chk.NoError(err)

	case BoolKind, NumberKind, StringKind:
		ldbKey := ldbKeyFromValue(val, opc.colId, opc.vrw)
		// Since the ldbKey contains the val, there's no reason to store anything in the ldbValue
		err := opc.ldb.Put(ldbKey, nil, nil)
		d.Chk.NoError(err)
	}
}

func (i *ldbOpCacheIterator) SetOp() sequenceItem {
	ldbKey := i.iter.Key()

	switch NomsKind(ldbKey[uint32Size]) {
	case BoolKind, NumberKind, StringKind:
		return DecodeFromBytes(ldbKey[uint32Size:], i.vr, staticTypeCache)
	default:
		data := i.iter.Value()
		return DecodeFromBytes(data, i.vr, staticTypeCache)
	}
}

func (i *ldbOpCacheIterator) Release() {
	i.iter.Release()
}

// writeLdbKeyHeaderBytes writes the first 4 or 5 bytes into the ldbKey. The first 4 bytes in every
// ldbKey are the colId. This identifies all the keys for a particular opCache and allows this opStore
// to cache values for multiple collections. The optional 5th byte is the NomsKind of the value. In
// cases where we're encoding the hash of an object we need to write the nomsKind manually because the
// hash doesn't contain it. In cases were we are encoding a primitive value into the key, the first byte
// of the value is the nomsKind so there is no reason to write it again.
func writeLdbKeyHeaderBytes(ldbKey []byte, colId uint32, v Value) []byte {
	binary.LittleEndian.PutUint32(ldbKey, colId)
	length := uint32Size
	if v != nil {
		ldbKey[length] = byte(v.Type().Kind())
		length++
	}
	return ldbKey[0:length]
}

// ldbKeys for non-primitive Nom Values (e.g. blobs, structs, lists, maps, etc) use a serialization of the values hash:
// colId(4 bytes) + nomsKind(val)(1 byte) + val.Hash()(20 bytes).

func ldbKeyFromValueHash(val Value, colId uint32) []byte {
	ldbKeyArray := [uint32Size + 1 + hash.ByteLen]byte{}
	ldbKey := writeLdbKeyHeaderBytes(ldbKeyArray[:], colId, val)
	return append(ldbKey, val.Hash().DigestSlice()...)
}

// ldbKeys for primitive Noms Values (e.g. bool, number, & string) consist of a byte string that encodes:
// colId(4 bytes) + serialized value(n bytes)
// Note: the first byte of the serialized value is the NomsKind.
func ldbKeyFromValue(val Value, colId uint32, vrw ValueReadWriter) []byte {
	valArray := [initialBufferSize]byte{}
	ldbKeyArray := [initialBufferSize]byte{}
	ldbKey := writeLdbKeyHeaderBytes(ldbKeyArray[:], colId, nil)
	encodedVal := encToSlice(val, valArray[:], vrw)
	return append(ldbKey, encodedVal...)
}
