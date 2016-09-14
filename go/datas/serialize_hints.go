// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

func serializeHints(w io.Writer, hints types.Hints) {
	err := binary.Write(w, binary.BigEndian, uint32(len(hints))) // 4 billion hints is probably absurd. Maybe this should be smaller?
	d.Chk.NoError(err)
	for r := range hints {
		serializeHash(w, r)
	}
}

func serializeHashes(w io.Writer, hashes hash.HashSlice) {
	err := binary.Write(w, binary.BigEndian, uint32(len(hashes))) // 4 billion hashes is probably absurd. Maybe this should be smaller?
	d.Chk.NoError(err)
	for _, r := range hashes {
		serializeHash(w, r)
	}
}

func serializeHash(w io.Writer, h hash.Hash) {
	digest := h.Digest()
	n, err := io.Copy(w, bytes.NewReader(digest[:]))
	d.Chk.NoError(err)
	d.PanicIfFalse(int64(hash.ByteLen) == n)
}

func deserializeHints(reader io.Reader) types.Hints {
	numRefs := uint32(0)
	err := binary.Read(reader, binary.BigEndian, &numRefs)
	d.Chk.NoError(err)

	hints := make(types.Hints, numRefs)
	for i := uint32(0); i < numRefs; i++ {
		hints[deserializeHash(reader)] = struct{}{}
	}
	return hints
}

func deserializeHashes(reader io.Reader) hash.HashSlice {
	numRefs := uint32(0)
	err := binary.Read(reader, binary.BigEndian, &numRefs)
	d.Chk.NoError(err)

	hashes := make(hash.HashSlice, numRefs)
	for i := range hashes {
		hashes[i] = deserializeHash(reader)
	}
	return hashes
}

func deserializeHash(reader io.Reader) hash.Hash {
	digest := hash.Digest{}
	n, err := io.ReadFull(reader, digest[:])
	d.Chk.NoError(err)
	d.PanicIfFalse(int(hash.ByteLen) == n)
	return hash.New(digest)
}
