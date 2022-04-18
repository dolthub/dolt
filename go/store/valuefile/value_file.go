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

package valuefile

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// ErrCorruptNVF is the error used when the file being read is corrupt
var ErrCorruptNVF = errors.New("nvf file is corrupt")

// WritePrimitiveValueFile writes values to the filepath provided
func WritePrimitiveValueFile(ctx context.Context, filepath string, values ...types.Value) error {
	for _, v := range values {
		if !types.IsPrimitiveKind(v.Kind()) {
			return errors.New("non-primitve value found")
		}
	}

	nbf := types.Format_Default
	store, err := NewFileValueStore(nbf)

	if err != nil {
		return err
	}

	return WriteValueFile(ctx, filepath, store, values...)
}

// WriteValueFile writes the values stored in the *FileValueStore to the filepath provided
func WriteValueFile(ctx context.Context, filepath string, store *FileValueStore, values ...types.Value) (err error) {

	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	if err != nil {
		return err
	}

	return WriteToWriter(ctx, f, store, values...)
}

// WriteToWriter writes the values out to the provided writer in the value file format
func WriteToWriter(ctx context.Context, wr io.Writer, store *FileValueStore, values ...types.Value) error {
	vrw := types.NewValueStore(store)
	db := datas.NewTypesDatabase(vrw)
	ds, err := db.GetDataset(ctx, env.DefaultInitBranch)

	if err != nil {
		return err
	}

	l, err := types.NewList(ctx, vrw, values...)

	if err != nil {
		return err
	}

	ds, err = datas.CommitValue(ctx, db, ds, l)

	if err != nil {
		return err
	}

	addr, _ := ds.MaybeHeadAddr()

	err = write(wr, addr, store)

	if err != nil {
		return err
	}

	return nil
}

// write writes out:
// NomsBinFormat version string length
// NomsBinFormat version String
// Root Hash
// uint32 num chunks
//
// for each chunk:
//   hash of chunk
//   len of chunk
//
// for each chunk
//   chunk bytes
func write(wr io.Writer, h hash.Hash, store *FileValueStore) error {
	// The Write*IfNoErr functions makes the error handling code less annoying
	err := iohelp.WritePrimIfNoErr(wr, uint32(len(store.nbf.VersionString())), nil)
	err = iohelp.WriteIfNoErr(wr, []byte(store.nbf.VersionString()), err)
	err = iohelp.WriteIfNoErr(wr, h[:], err)
	err = iohelp.WritePrimIfNoErr(wr, uint32(store.numChunks()), err)

	if err != nil {
		return err
	}

	err = store.iterChunks(func(ch chunks.Chunk) error {
		h := ch.Hash()
		err = iohelp.WriteIfNoErr(wr, h[:], err)
		return iohelp.WritePrimIfNoErr(wr, uint32(len(ch.Data())), err)
	})

	err = store.iterChunks(func(ch chunks.Chunk) error {
		return iohelp.WriteIfNoErr(wr, ch.Data(), err)
	})

	return err
}

// ReadValueFile reads from the provided file and returns the values stored in the file
func ReadValueFile(ctx context.Context, filepath string) ([]types.Value, error) {
	f, err := os.Open(filepath)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	return ReadFromReader(ctx, f)
}

// ReadFromReader reads from the provided reader which should provided access to data in the value file format and returns
// the values
func ReadFromReader(ctx context.Context, rd io.Reader) ([]types.Value, error) {
	h, store, err := read(ctx, rd)

	if err != nil {
		return nil, err
	}

	vrw := types.NewValueStore(store)

	v, err := vrw.ReadValue(ctx, h)

	if err != nil {
		return nil, err
	}

	commitSt, ok := v.(types.Struct)

	if !ok {
		return nil, ErrCorruptNVF
	}

	rootVal, err := datas.GetCommittedValue(ctx, vrw, commitSt)
	if err != nil {
		return nil, err
	}

	l := rootVal.(types.List)
	values := make([]types.Value, l.Len())
	err = l.IterAll(ctx, func(v types.Value, index uint64) error {
		values[index] = v
		return nil
	})

	if err != nil {
		return nil, err
	}

	return values, nil
}

// see the write section to see the value file
func read(ctx context.Context, rd io.Reader) (hash.Hash, *FileValueStore, error) {
	// ErrPreservingReader allows me to ignore errors until I need to use the data
	errRd := iohelp.NewErrPreservingReader(rd)

	// read len of NBF version string and then read the version string and check it
	fmtLen, err := errRd.ReadUint32(binary.BigEndian)

	if err != nil {
		if err == io.EOF {
			err = fmt.Errorf("EOF read while tring to get nbf format len - %w", ErrCorruptNVF)
		}

		return hash.Hash{}, nil, err
	}

	data, err := iohelp.ReadNBytes(errRd, int(fmtLen))

	if err != nil {
		if err == io.EOF {
			err = fmt.Errorf("EOF read while tring to get nbf format string - %w", ErrCorruptNVF)
		}

		return hash.Hash{}, nil, err
	}

	var nbf *types.NomsBinFormat
	switch string(data) {
	case types.Format_7_18.VersionString():
		nbf = types.Format_7_18
	case types.Format_LD_1.VersionString():
		nbf = types.Format_LD_1
	// todo(andy): add types.Format_DOLT_1
	default:
		return hash.Hash{}, nil, fmt.Errorf("unknown noms format: %s", string(data))
	}

	store, err := NewFileValueStore(nbf)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	// read the root hash and the chunk count
	hashBytes, _ := iohelp.ReadNBytes(errRd, hash.ByteLen)
	numChunks, err := errRd.ReadUint32(binary.BigEndian)

	if err != nil {
		if err == io.EOF {
			err = fmt.Errorf("EOF read while trying to read the root hash and chunk count - %w", ErrCorruptNVF)
		}

		return hash.Hash{}, nil, err
	}

	// read the hashes and sizes
	type hashAndSize struct {
		h    hash.Hash
		size uint32
	}
	hashesAndSizes := make([]hashAndSize, numChunks)
	for i := uint32(0); i < numChunks; i++ {
		chHashBytes, _ := iohelp.ReadNBytes(errRd, hash.ByteLen)
		size, err := errRd.ReadUint32(binary.BigEndian)

		if err != nil {
			if err == io.EOF {
				err = fmt.Errorf("EOF read the root hash and chunk count - %w", ErrCorruptNVF)
			}

			return hash.Hash{}, nil, err
		}

		hashesAndSizes[i] = hashAndSize{hash.New(chHashBytes), size}
	}

	// read the data and validate it against the expected hashes
	for _, hashAndSize := range hashesAndSizes {
		h := hashAndSize.h
		size := hashAndSize.size
		chBytes, err := iohelp.ReadNBytes(errRd, int(size))

		if err != nil && err != io.EOF || err == io.EOF && uint32(len(chBytes)) != size {
			if err == io.EOF {
				err = fmt.Errorf("EOF read trying to read chunk - %w", ErrCorruptNVF)
			}

			return hash.Hash{}, nil, err
		}

		ch := chunks.NewChunk(chBytes)

		if h != ch.Hash() {
			return hash.Hash{}, nil, errors.New("data corrupted")
		}

		err = store.Put(ctx, ch)

		if err != nil {
			return hash.Hash{}, nil, err
		}
	}

	return hash.New(hashBytes), store, nil
}
