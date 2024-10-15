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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrNoBranchStats = errors.New("stats for branch not found")

type Statistics struct {
	m    prolly.Map
	addr hash.Hash
}

func (s *Statistics) Map() prolly.Map {
	return s.m
}

func (s *Statistics) Addr() hash.Hash {
	return s.addr
}

func (s *Statistics) Count() (int, error) {
	return s.m.Count()
}

// IsStatistic determines whether the types.Value is a stash list object.
func IsStatistic(v types.Value) (bool, error) {
	if _, ok := v.(types.Struct); ok {
		// this should not return true as stash is not supported for old format
		return false, nil
	} else if sm, ok := v.(types.SerialMessage); ok {
		return serial.GetFileID(sm) == serial.StatisticFileID, nil
	} else {
		return false, nil
	}
}

// LoadStatistics attempts to dereference a database's statistics Dataset into a typed Statistics object.
func LoadStatistics(ctx context.Context, nbf *types.NomsBinFormat, ns tree.NodeStore, vr types.ValueReader, ds Dataset) (*Statistics, error) {
	if !nbf.UsesFlatbuffers() {
		return nil, errors.New("loadStatistics: statistics are not supported for old storage format")
	}

	rootHash, hasHead := ds.MaybeHeadAddr()
	if !hasHead {
		return &Statistics{m: prolly.Map{}, addr: hash.Hash{}}, nil
	}

	val, err := vr.ReadValue(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, ErrNoBranchStats
	}

	return parse_Statistics(ctx, []byte(val.(types.SerialMessage)), ns, vr)
}

// newStat writes an address to a statistics map as a statistics message
// in the provided database.
func newStat(ctx context.Context, db *database, addr hash.Hash) (hash.Hash, types.Ref, error) {
	data := Statistics_flatbuffer(addr)
	r, err := db.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, types.Ref{}, err
	}
	return r.TargetHash(), r, nil
}

// Statistics_flatbuffer encodes a prolly map address in a statistics message.
func Statistics_flatbuffer(addr hash.Hash) serial.Message {
	builder := flatbuffers.NewBuilder(1024)
	vaddroff := builder.CreateByteVector(addr[:])

	serial.StatisticStart(builder)
	serial.StatisticAddRoot(builder, vaddroff)
	return serial.FinishMessage(builder, serial.CommitEnd(builder), []byte(serial.StatisticFileID))
}

// parse_Statistics converts a statistics serial massage (STAT) into a Statistics object
// embedding the stats table and address.
func parse_Statistics(ctx context.Context, bs []byte, ns tree.NodeStore, vr types.ValueReader) (*Statistics, error) {
	if serial.GetFileID(bs) != serial.StatisticFileID {
		return nil, fmt.Errorf("expected statistics file id, got: " + serial.GetFileID(bs))
	}
	stat, err := serial.TryGetRootAsStatistic(bs, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}

	addr := hash.New(stat.RootBytes())
	value, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	m, err := shim.MapFromValue(value, schema.StatsTableDoltSchema, ns)
	if err != nil {
		return nil, err
	}
	return &Statistics{m: m, addr: m.HashOf()}, nil
}
