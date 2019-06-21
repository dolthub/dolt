// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"encoding/binary"
	"io"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

func serializeHashes(w io.Writer, batch chunks.ReadBatch) {
	err := binary.Write(w, binary.BigEndian, uint32(len(batch))) // 4 billion hashes is probably absurd. Maybe this should be smaller?
	d.PanicIfError(err)
	for h := range batch {
		serializeHash(w, h)
	}
}

func serializeHash(w io.Writer, h hash.Hash) {
	_, err := w.Write(h[:])
	d.PanicIfError(err)
}

func deserializeHashes(reader io.Reader) hash.HashSlice {
	count := uint32(0)
	err := binary.Read(reader, binary.BigEndian, &count)
	d.PanicIfError(err)

	hashes := make(hash.HashSlice, count)
	for i := range hashes {
		hashes[i] = deserializeHash(reader)
	}
	return hashes
}

func deserializeHash(reader io.Reader) hash.Hash {
	h := hash.Hash{}
	n, err := io.ReadFull(reader, h[:])
	d.PanicIfError(err)
	d.PanicIfFalse(int(hash.ByteLen) == n)
	return h
}
