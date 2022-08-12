// Copyright 2022 Dolthub, Inc.
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

package writer

import (
	"context"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyKeylessWriter struct {
	name string
	mut  prolly.MutableMap

	keyBld *val.TupleBuilder
	valBld *val.TupleBuilder
	valMap val.OrdinalMapping
}

var _ indexWriter = prollyKeylessWriter{}
var _ primaryIndexErrBuilder = prollyKeylessWriter{}

func (k prollyKeylessWriter) Name() string {
	return k.name
}

func (k prollyKeylessWriter) Map(ctx context.Context) (prolly.Map, error) {
	return k.mut.Map(ctx)
}

func (k prollyKeylessWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	hashId, value, err := k.tuplesFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}

	err = k.mut.Get(ctx, hashId, func(k, v val.Tuple) (err error) {
		if k != nil {
			value = v
		}
		return
	})
	if err != nil {
		return err
	}

	// increment cardinality
	updated, _ := val.ModifyKeylessCardinality(sharePool, value, int64(1))

	return k.mut.Put(ctx, hashId, updated)
}

func (k prollyKeylessWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	hashId, _, err := k.tuplesFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}

	var value val.Tuple
	err = k.mut.Get(ctx, hashId, func(k, v val.Tuple) (err error) {
		if k != nil {
			value = v
		}
		return
	})
	if err != nil {
		return err
	}

	if value == nil {
		return nil // non-existent row
	}

	// decrement cardinality
	updated, after := val.ModifyKeylessCardinality(sharePool, value, int64(-1))
	if after > 0 {
		return k.mut.Put(ctx, hashId, updated)
	} else {
		return k.mut.Delete(ctx, hashId)
	}
}

func (k prollyKeylessWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	if err = k.Delete(ctx, oldRow); err != nil {
		return err
	}
	if err = k.Insert(ctx, newRow); err != nil {
		return err
	}
	return
}

func (k prollyKeylessWriter) Commit(ctx context.Context) error {
	return k.mut.ApplyPending(ctx)
}

func (k prollyKeylessWriter) Discard(ctx context.Context) error {
	k.mut.DiscardPending(ctx)
	return nil
}

func (k prollyKeylessWriter) HasEdits(ctx context.Context) bool {
	return k.mut.HasEdits()
}

func (k prollyKeylessWriter) IterRange(ctx context.Context, rng prolly.Range) (prolly.MapIter, error) {
	return k.IterRange(ctx, rng)
}

func (k prollyKeylessWriter) tuplesFromRow(ctx context.Context, sqlRow sql.Row) (hashId, value val.Tuple, err error) {
	// initialize cardinality to 0
	if err = index.PutField(ctx, k.mut.NodeStore(), k.valBld, 0, uint64(0)); err != nil {
		return nil, nil, err
	}

	for to := range k.valMap {
		from := k.valMap.MapOrdinal(to)
		if err = index.PutField(ctx, k.mut.NodeStore(), k.valBld, to+1, sqlRow[from]); err != nil {
			return nil, nil, err
		}
	}

	value = k.valBld.Build(sharePool)
	hashId = val.HashTupleFromValue(sharePool, value)
	return
}

func (k prollyKeylessWriter) getMut() prolly.MutableMap {
	return k.mut
}

func (k prollyKeylessWriter) errForSecondaryUniqueKeyError(ctx context.Context, err secondaryUniqueKeyError) error {
	return k.uniqueKeyError(ctx, err.keyStr, err.existingKey, false)
}

// UniqueKeyError builds a sql.UniqueKeyError. It fetches the existing row using
// |key| and passes it as the |existing| row.
func (k prollyKeylessWriter) uniqueKeyError(ctx context.Context, keyStr string, key val.Tuple, isPk bool) error {
	existing := make(sql.Row, len(k.valMap))

	_ = k.mut.Get(ctx, key, func(key, value val.Tuple) (err error) {
		vd := k.valBld.Desc
		for from := range k.valMap {
			to := k.valMap.MapOrdinal(from)
			if existing[to], err = index.GetField(ctx, vd, from, value, k.mut.NodeStore()); err != nil {
				return err
			}
		}
		return
	})

	return sql.NewUniqueKeyErr(keyStr, isPk, existing)
}

type secondaryUniqueKeyError struct {
	keyStr      string
	existingKey val.Tuple
}

func (e secondaryUniqueKeyError) Error() string {
	return ""
}

type prollyKeylessSecondaryWriter struct {
	name    string
	mut     prolly.MutableMap
	primary prollyKeylessWriter
	unique  bool

	keyBld    *val.TupleBuilder
	prefixBld *val.TupleBuilder
	hashBld   *val.TupleBuilder
	keyMap    val.OrdinalMapping
}

var _ indexWriter = prollyKeylessSecondaryWriter{}

// Name implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) Name() string {
	return writer.name
}

// Map implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) Map(ctx context.Context) (prolly.Map, error) {
	return writer.mut.Map(ctx)
}

// Insert implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) Insert(ctx context.Context, sqlRow sql.Row) error {
	for to := range writer.keyMap {
		from := writer.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, writer.mut.NodeStore(), writer.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
		if to < writer.prefixBld.Desc.Count() {
			if err := index.PutField(ctx, writer.mut.NodeStore(), writer.prefixBld, to, sqlRow[from]); err != nil {
				return err
			}
		}
	}

	hashId, _, err := writer.primary.tuplesFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}
	writer.keyBld.PutHash128(len(writer.keyBld.Desc.Types)-1, hashId.GetField(0))
	indexKey := writer.keyBld.Build(sharePool)

	if writer.unique {
		prefixKey := writer.prefixBld.Build(sharePool)
		err := writer.checkForUniqueKeyError(ctx, prefixKey)
		if err != nil {
			return err
		}
	} else {
		writer.prefixBld.Recycle()
	}

	return writer.mut.Put(ctx, indexKey, val.EmptyTuple)
}

func (writer prollyKeylessSecondaryWriter) checkForUniqueKeyError(ctx context.Context, prefixKey val.Tuple) error {
	for i := 0; i < prefixKey.Count(); i++ {
		if prefixKey.FieldIsNull(i) {
			return nil
		}
	}

	rng := prolly.PrefixRange(prefixKey, writer.prefixBld.Desc)
	itr, err := writer.mut.IterRange(ctx, rng)
	if err != nil {
		return err
	}
	k, _, err := itr.Next(ctx)
	if err != nil && err != io.EOF {
		return err
	}
	if err == nil {
		keyStr := FormatKeyForUniqKeyErr(prefixKey, writer.prefixBld.Desc)
		writer.hashBld.PutRaw(0, k.GetField(k.Count()-1))
		existingKey := writer.hashBld.Build(sharePool)
		return secondaryUniqueKeyError{keyStr: keyStr, existingKey: existingKey}
	}
	return nil
}

// Delete implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) Delete(ctx context.Context, sqlRow sql.Row) error {
	hashId, cardRow, err := writer.primary.tuplesFromRow(ctx, sqlRow)
	if err != nil {
		return err
	}
	err = writer.primary.mut.Get(ctx, hashId, func(k, v val.Tuple) (err error) {
		if k != nil {
			cardRow = v
		}
		return
	})
	if err != nil {
		return err
	}

	for to := range writer.keyMap {
		from := writer.keyMap.MapOrdinal(to)
		if err := index.PutField(ctx, writer.mut.NodeStore(), writer.keyBld, to, sqlRow[from]); err != nil {
			return err
		}
	}
	writer.keyBld.PutHash128(len(writer.keyBld.Desc.Types)-1, hashId.GetField(0))
	indexKey := writer.keyBld.Build(sharePool)

	// Indexes are always updated before the primary table, so we check if the deletion will cause the row to be removed
	// from the primary. If not, then we just return.
	card := val.ReadKeylessCardinality(cardRow)
	if card > 1 {
		return nil
	}
	return writer.mut.Delete(ctx, indexKey)
}

// Update implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) Update(ctx context.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	if err = writer.Delete(ctx, oldRow); err != nil {
		return err
	}
	if err = writer.Insert(ctx, newRow); err != nil {
		return err
	}
	return
}

// Commit implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) Commit(ctx context.Context) error {
	return writer.mut.ApplyPending(ctx)
}

// Discard implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) Discard(ctx context.Context) error {
	writer.mut.DiscardPending(ctx)
	return nil
}

// HasEdits implements the interface indexWriter.
func (writer prollyKeylessSecondaryWriter) HasEdits(ctx context.Context) bool {
	return writer.mut.HasEdits()
}

func (writer prollyKeylessSecondaryWriter) IterRange(ctx context.Context, rng prolly.Range) (prolly.MapIter, error) {
	return writer.mut.IterRange(ctx, rng)
}
