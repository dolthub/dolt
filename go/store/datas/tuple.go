// Copyright 2023 Dolthub, Inc.
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

package datas

import (
	"context"
	"errors"
	"fmt"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type Tuple struct {
	val []byte
}

func (t Tuple) Bytes() []byte {
	return t.val
}

// IsTuple determines whether the types.Value is a tuple
func IsTuple(v types.Value) (bool, error) {
	if _, ok := v.(types.Struct); ok {
		// this should not return true as stash is not supported for old format
		return false, nil
	} else if sm, ok := v.(types.SerialMessage); ok {
		return serial.GetFileID(sm) == serial.StatisticFileID, nil
	} else {
		return false, nil
	}
}

// LoadTuple attempts to dereference a database's Tuple Dataset into a typed Tuple object.
func LoadTuple(ctx context.Context, nbf *types.NomsBinFormat, ns tree.NodeStore, vr types.ValueReader, ds Dataset) (*Tuple, error) {
	if !nbf.UsesFlatbuffers() {
		return nil, errors.New("loadTuple: Tuple are not supported for old storage format")
	}

	rootHash, hasHead := ds.MaybeHeadAddr()
	if !hasHead {
		return &Tuple{}, nil
	}

	val, err := vr.ReadValue(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, ErrNoBranchStats
	}

	return parse_Tuple(ctx, []byte(val.(types.SerialMessage)), ns, vr)
}

// newStat writes an address to a Tuple map as a Tuple message
// in the provided database.
func newTuple(ctx context.Context, db *database, addr []byte) (hash.Hash, types.Ref, error) {
	data := Tuple_flatbuffer(addr)
	r, err := db.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, types.Ref{}, err
	}
	return r.TargetHash(), r, nil
}

// Tuple_flatbuffer encodes a prolly map address in a Tuple message.
func Tuple_flatbuffer(val []byte) serial.Message {
	builder := flatbuffers.NewBuilder(1024)
	valOff := builder.CreateByteVector(val[:])

	serial.TupleStart(builder)
	serial.TupleAddValue(builder, valOff)
	return serial.FinishMessage(builder, serial.CommitEnd(builder), []byte(serial.TupleFileID))
}

// parse_Tuple converts a Tuple serial massage (STAT) into a Tuple object
// embedding the stats table and address.
func parse_Tuple(ctx context.Context, bs []byte, ns tree.NodeStore, vr types.ValueReader) (*Tuple, error) {
	if serial.GetFileID(bs) != serial.TupleFileID {
		return nil, fmt.Errorf("expected Tuple file id, got: " + serial.GetFileID(bs))
	}
	tup, err := serial.TryGetRootAsTuple(bs, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}

	return &Tuple{val: tup.ValueBytes()}, nil
}
