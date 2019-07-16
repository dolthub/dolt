// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

func serializeHashes(w io.Writer, batch chunks.ReadBatch) error {
	err := binary.Write(w, binary.BigEndian, uint32(len(batch))) // 4 billion hashes is probably absurd. Maybe this should be smaller?

	if err != nil {
		return err
	}

	for h := range batch {
		err = serializeHash(w, h)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeHash(w io.Writer, h hash.Hash) error {
	_, err := w.Write(h[:])

	return err
}

func deserializeHashes(reader io.Reader) (hash.HashSlice, error) {
	count := uint32(0)
	err := binary.Read(reader, binary.BigEndian, &count)

	if err != nil {
		return hash.HashSlice{}, err
	}

	hashes := make(hash.HashSlice, count)
	for i := range hashes {
		hashes[i], err = deserializeHash(reader)

		if err != nil {
			return hash.HashSlice{}, err
		}
	}
	return hashes, nil
}

func deserializeHash(reader io.Reader) (hash.Hash, error) {
	h := hash.Hash{}
	n, err := io.ReadFull(reader, h[:])

	if err != nil {
		return hash.Hash{}, err
	}

	if int(hash.ByteLen) != n {
		return hash.Hash{}, errors.New("failed to read all data")
	}

	return h, nil
}
