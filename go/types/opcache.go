// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// opCache stores build operations on a graph of nested Maps whose leaves can
// in turn be Set, Map, or List collections containing any Noms Value.
// OpCacheIterator returns operations in sorted order.
//
// OpCache uses a special encoding of the information supplied by the MapSet(),
// ListAppend(), or SetInsert() operation stored in the ldbKey combined with
// custom ldb Comparer object implemented in opcache_compare.go to make this
// happen.
//
// Ldb keys are encoded byte arrays that contain the following information:
//     4-bytes -- uint32 in BigEndian order which identifies this key/value
//                as belonging to a particular graph
//     1-byte  -- a NomsKind value that represents the collection type that is
//                being acted on. This will either be MapKind, SetKind, or ListKind.
//     1-byte  -- uint8 representing the number of NomsValues encoded in this key
//
// After this 6-byte header, there is a section of bytes for each value encoded
// into the key. Each value has a 1-byte prefix:
//     1-byte  -- a NomsKind value that represents the type of value that is
//                being encoded.
//     The 1-byte NomsKind value determines what follows, if this value is
//     BoolKind, NumberKind, or StringKind, the rest of the bytes are:
//         4-bytes -- uint32 length of the Value serialization
//         n-bytes -- the serialized value
//     If the NomsKind byte has any other value, it is followed by:
//         20-bytes -- digest of Value's hash
//
// Whenever the value is encoded as a hash digest in the ldbKey, it's actual value
// needs to get stored in the ldbValue. (More about this later)
//
// There are 3 operation types on opCache: MapSet(), SetInsert(), and ListAppend().
// Each one stores slightly different things in the ldbKey.
// MapSet() -- stores each graphKey and the key to the final Map
// ValueSet() -- stores each graphKey and the Value being inserted into the set
// ListAppend() -- stores each graphKey and a Number() containing an uint64 value
//    that is shared across all collections and lists which is incremented each time
//    ListAppend() is called.
//
// The ldbValue also stores different information for each mutation operation. An
// ldbValue has a 1-byte uint8 header that is the number of values that are encoded
// into it.
//    1-byte -- uint8 indicating number of values encoded into this byte array
// Then for each encoded value it contains:
//    4-byte -- uint32 indicating length of value serialization
//    n-bytes -- the serialized value
//
// The ldbValue contains the following values for each type of mutation:
// MapSet() -- stores any graphKeys that were encoded as a hash digest in
//    the ldbKey. The mapKey if it was encoded as a hash digest in the ldbKey
//    and the value being set in the map.
// SetInsert() -- stores any graphKeys that were encoded as a hash digest in
//    the ldbKey. The value being inserted into the set if it was encoded into the
//    ldbKey as a hash digest.
// ListAppend() -- stores any graphKeys that were encoded as a hash digest in the
//    ldbKey. The value being appended to the list.
//

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
	// This method can be called from multiple go routines.
	GraphMapSet(keys ValueSlice, mapKey Value, mapVal Value)

	// This method can be called from multiple go routines.
	GraphSetInsert(keys ValueSlice, val Value)

	// This method can be called from multiple go routines, however items will
	// be appended to the list based on the order that routines execute
	// this method.
	GraphListAppend(keys ValueSlice, val Value)

	NewIterator() opCacheIterator
}

type opCacheIterator interface {
	GraphOp() (ValueSlice, NomsKind, sequenceItem)
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
	vrw     ValueReadWriter
	colId   uint32
	listIdx int64
	ldb     *leveldb.DB
}

type ldbOpCacheIterator struct {
	iter ldbIterator.Iterator
	vrw  ValueReadWriter
}

func newLdbOpCacheStore(vrw ValueReadWriter) *ldbOpCacheStore {
	dir, err := ioutil.TempDir("", "")
	d.Chk.NoError(err)
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Comparer:               opCacheComparer{},
		OpenFilesCacheCapacity: 24,
		// This data does not have to be durable. LDB is acting as temporary
		// storage that can be larger than main memory.
		NoSync:      true,
		WriteBuffer: 1 << 27, // 128MiB
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

// insertLdbOp encodes allKeys into the ldb key. Bool, Number, and String values
// are encoded directly into the ldb key bytes. All other types are encoded as
// their Hash() digest. Their actual value is then stored in ldb value.
func (opc *ldbOpCache) insertLdbOp(allKeys ValueSlice, opKind NomsKind, val Value) {
	if len(allKeys) > 0x00FF {
		d.Panic("Number of keys in GraphMapSet exceeds max of 256")
	}
	ldbKeyBytes := [initialBufferSize]byte{}
	ldbValBytes := [initialBufferSize]byte{}

	ldbKey, valuesToEncode := encodeKeys(ldbKeyBytes[:0], opc.colId, opKind, allKeys)

	// val may be nil when dealing with sets, since the val is the key.
	if val != nil {
		valuesToEncode = append(valuesToEncode, val)
	}
	ldbVal := encodeValues(ldbValBytes[:0], valuesToEncode)

	err := opc.ldb.Put(ldbKey, ldbVal, nil)
	d.Chk.NoError(err)
}

func (opc *ldbOpCache) GraphMapSet(graphKeys ValueSlice, mapKey, mapVal Value) {
	allKeys := append(graphKeys, mapKey)
	opc.insertLdbOp(allKeys, MapKind, mapVal)
}

func (opc *ldbOpCache) GraphSetInsert(graphKeys ValueSlice, val Value) {
	allKeys := append(graphKeys, val)
	opc.insertLdbOp(allKeys, SetKind, val)
}

func (opc *ldbOpCache) GraphListAppend(graphKeys ValueSlice, val Value) {
	idx := atomic.AddInt64(&opc.listIdx, 1)
	allKeys := append(graphKeys, Number(idx))
	opc.insertLdbOp(allKeys, ListKind, val)
}

func (i *ldbOpCacheIterator) GraphOp() (ValueSlice, NomsKind, sequenceItem) {
	ldbKey := i.iter.Key()
	ldbVal := i.iter.Value()

	// skip over 4 bytes of colId and get opKind, and numKeys from bytes 4 & 5
	opKind := NomsKind(ldbKey[4])
	numKeys := uint8(ldbKey[5])
	ldbKey = ldbKey[6:]

	// Call decodeValue for each encoded graphKey. nil will be appended to
	// graphKeys for any keys that were encoded as hash digests.
	graphKeys := ValueSlice{}
	for pos := uint8(0); pos < numKeys; pos++ {
		var gk Value
		ldbKey, gk = decodeValue(ldbKey, false, i.vrw)
		graphKeys = append(graphKeys, gk)
	}

	// Get the number of values whose value was encoded in ldbVal
	numEncodedValues := uint8(ldbVal[0])
	ldbVal = ldbVal[1:]

	// Call decodeValue for each non-primitive key stored in ldbVal. Replace
	// the nil value in graphKeys with the new decodedValue.
	values := ValueSlice{}
	for pos := uint8(0); pos < numEncodedValues; pos++ {
		var gk Value
		ldbVal, gk = decodeValue(ldbVal, true, i.vrw)
		values = append(values, gk)
	}

	// Fold in any non-primitive key values that were stored in ldbVal
	pos := 0
	for idx, k1 := range graphKeys {
		if k1 == nil {
			graphKeys[idx] = values[pos]
			pos++
		}
	}

	// Remove the last key in graphKeys. The last key in graphKeys is the
	// mapkey for Maps, the item for Sets, and the index for Lists.
	key := graphKeys[len(graphKeys)-1]
	graphKeys = graphKeys[:len(graphKeys)-1]

	var item sequenceItem
	switch opKind {
	case MapKind:
		val := values[len(values)-1]
		item = mapEntry{key, val}
	case SetKind:
		item = key
	case ListKind:
		item = values[len(values)-1]
	}

	return graphKeys, opKind, item
}

func (opc *ldbOpCache) NewIterator() opCacheIterator {
	prefix := [4]byte{}
	binary.BigEndian.PutUint32(prefix[:], opc.colId)
	return &ldbOpCacheIterator{iter: opc.ldb.NewIterator(util.BytesPrefix(prefix[:]), nil), vrw: opc.vrw}
}

func (i *ldbOpCacheIterator) Next() bool {
	return i.iter.Next()
}

func (i *ldbOpCacheIterator) Release() {
	i.iter.Release()
}

// encodeKeys() serializes a list of keys to the byte slice |bs|.
func encodeKeys(bs []byte, colId uint32, opKind NomsKind, keys []Value) ([]byte, []Value) {
	// All ldb keys start with a 4-byte collection id that serves as a namespace
	// that keeps them separate from other collections.
	idHolder := [4]byte{}
	idHolderSlice := idHolder[:4]
	binary.BigEndian.PutUint32(idHolderSlice, colId)
	bs = append(bs, idHolderSlice...)

	// bs[4] is a NomsKind value which represents the type of leaf
	//   collection being operated on (i.e. MapKind, SetKind, or ListKind)
	// bs[5] is a single uint8 value representing the number of keys
	//   encoded in the ldb key.
	bs = append(bs, byte(opKind), byte(len(keys)))

	valuesToEncode := ValueSlice{}
	for _, gk := range keys {
		bs = encodeGraphKey(bs, gk)
		if !isKindOrderedByValue(gk.Kind()) {
			valuesToEncode = append(valuesToEncode, gk)
		}
	}
	return bs, valuesToEncode
}

func encodeValues(bs []byte, valuesToEncode []Value) []byte {
	// Encode allValues into the ldbVal byte slice.
	bs = append(bs, uint8(len(valuesToEncode)))
	for _, k := range valuesToEncode {
		bs = encodeGraphValue(bs, k)
	}
	return bs
}

func encodeGraphKey(bs []byte, v Value) []byte {
	return encodeForGraph(bs, v, false)
}

func encodeGraphValue(bs []byte, v Value) []byte {
	return encodeForGraph(bs, v, true)
}

func encodeForGraph(bs []byte, v Value, asValue bool) []byte {
	// Note: encToSlice() and append() will both grow the backing store of |bs|
	// as necessary. Always call them when writing to |bs|.
	if asValue || isKindOrderedByValue(v.Kind()) {
		// if we're encoding value, then put:
		// noms-kind(1-byte), serialization-len(4-bytes), serialization(n-bytes)
		buf := [initialBufferSize]byte{}
		uint32buf := [4]byte{}
		encodedVal := encToSlice(v, buf[:])
		binary.BigEndian.PutUint32(uint32buf[:], uint32(len(encodedVal)))
		bs = append(bs, uint8(v.Kind()))
		bs = append(bs, uint32buf[:]...)
		bs = append(bs, encodedVal...)
	} else {
		// if we're encoding hash values, we know the length, so we can leave that out
		bs = append(bs, uint8(v.Kind()))
		h := v.Hash()
		bs = append(bs, h[:]...)
	}
	return bs
}

func decodeValue(bs []byte, asValue bool, vrw ValueReadWriter) ([]byte, Value) {
	kind := NomsKind(bs[0])
	var v Value
	if asValue || isKindOrderedByValue(kind) {
		encodedLen := binary.BigEndian.Uint32(bs[1:5])
		// The bytes in bs gets reused by LDB. The data of a chunk must
		// never change since we are backing the values by this data.
		data := make([]byte, encodedLen)
		copy(data, bs[5:5+encodedLen])
		v = DecodeFromBytes(data, vrw)
		return bs[5+encodedLen:], v
	}
	return bs[1+hash.ByteLen:], nil
}

// Note that, if 'v' are prolly trees, any in-memory child chunks will be written to vw at this time.
func encToSlice(v Value, initBuf []byte) []byte {
	// TODO: Are there enough calls to this that it's worth re-using a nomsWriter?
	w := &binaryNomsWriter{initBuf, 0}
	v.writeTo(w)
	return w.data()
}
