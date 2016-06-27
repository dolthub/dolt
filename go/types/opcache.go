// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"crypto/sha1"
	"encoding/binary"
	"io/ioutil"
	"os"

	"github.com/attic-labs/noms/go/d"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func newOpCache(vrw ValueReadWriter) *opCache {
	dir, err := ioutil.TempDir("", "")
	d.Chk.NoError(err)
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression:            opt.NoCompression,
		Comparer:               opCacheComparer{},
		OpenFilesCacheCapacity: 24,
		NoSync:                 true,    // We don't need this data to be durable. LDB is acting as temporary storage that can be larger than main memory.
		WriteBuffer:            1 << 28, // 256MiB
	})
	d.Chk.NoError(err, "opening put cache in %s", dir)
	return &opCache{ops: db, dbDir: dir, vrw: vrw}
}

type opCache struct {
	ops           *leveldb.DB
	dbDir         string
	vrw           ValueReadWriter
	ldbKeyScratch [1 + sha1.Size]byte
	keyScratch    [initialBufferSize]byte
	valScratch    [initialBufferSize]byte
}

type opCacheIterator struct {
	iter iterator.Iterator
	vr   ValueReader
}

var uint32Size = binary.Size(uint32(0))

// Set can be called from any goroutine
func (p *opCache) Set(mapKey Value, mapVal Value) {
	switch mapKey.Type().Kind() {
	default:
		// This is the complicated case. For non-primitives, we want the ldb key to be the hash of mapKey, but we obviously need to get both mapKey and mapVal into ldb somehow. The simplest thing is just to do this:
		//
		//     uint32 (4 bytes)             bytes                 bytes
		// +-----------------------+---------------------+----------------------+
		// | key serialization len |    serialized key   |   serialized value   |
		// +-----------------------+---------------------+----------------------+

		// Note that, if mapKey and/or mapVal are prolly trees, any in-memory child chunks will be written to vrw at this time.
		p.ldbKeyScratch[0] = byte(mapKey.Type().Kind())
		copy(p.ldbKeyScratch[1:], mapKey.Hash().DigestSlice())
		mapKeyData := encToSlice(mapKey, p.keyScratch[:], p.vrw)
		mapValData := encToSlice(mapVal, p.valScratch[:], p.vrw)

		mapKeyByteLen := len(mapKeyData)
		data := make([]byte, uint32Size+mapKeyByteLen+len(mapValData))
		binary.LittleEndian.PutUint32(data, uint32(mapKeyByteLen))
		copy(data[uint32Size:], mapKeyData)
		copy(data[uint32Size+mapKeyByteLen:], mapValData)

		// TODO: Will manually batching these help?
		err := p.ops.Put(p.ldbKeyScratch[:], data, nil)
		d.Chk.NoError(err)

	case BoolKind, NumberKind, StringKind:
		// In this case, we can just serialize mapKey and use it as the ldb key, so we can also just serialize mapVal and dump that into the DB.
		keyData := encToSlice(mapKey, p.keyScratch[:], p.vrw)
		valData := encToSlice(mapVal, p.valScratch[:], p.vrw)
		// TODO: Will manually batching these help?
		err := p.ops.Put(keyData, valData, nil)
		d.Chk.NoError(err)
	}
}

func encToSlice(v Value, initBuf []byte, vw ValueWriter) []byte {
	// TODO: Are there enough calls to this that it's worth re-using a nomsWriter and valueEncoder?
	w := &binaryNomsWriter{initBuf, 0}
	enc := newValueEncoder(w, vw)
	enc.writeValue(v)
	return w.data()
}

func (p *opCache) NewIterator() *opCacheIterator {
	return &opCacheIterator{p.ops.NewIterator(nil, nil), p.vrw}
}

func (p *opCache) Destroy() error {
	d.Chk.NoError(p.ops.Close())
	return os.RemoveAll(p.dbDir)
}

func (i *opCacheIterator) Next() bool {
	return i.iter.Next()
}

func (i *opCacheIterator) Op() sequenceItem {
	entry := mapEntry{}
	ldbKey := i.iter.Key()
	data := i.iter.Value()
	dataOffset := 0
	switch NomsKind(ldbKey[0]) {
	case BoolKind, NumberKind, StringKind:
		entry.key = newValueDecoder(&binaryNomsReader{ldbKey, 0}, i.vr).readValue()
	default:
		keyBytesLen := int(binary.LittleEndian.Uint32(data))
		entry.key = newValueDecoder(&binaryNomsReader{data[uint32Size : uint32Size+keyBytesLen], 0}, i.vr).readValue()
		dataOffset = uint32Size + keyBytesLen
	}

	dec := newValueDecoder(&binaryNomsReader{data[dataOffset:], 0}, i.vr)
	entry.value = dec.readValue()
	return entry
}

func (i *opCacheIterator) Release() {
	i.iter.Release()
}
