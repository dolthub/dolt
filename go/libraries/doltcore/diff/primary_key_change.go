// Copyright 2026 Dolthub, Inc.
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

package diff

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// KeyedRows is one side of a diff: the rows of a table and the schema whose
// primary key orders them.
//
// A primary key holds one row and no NULL, so rekeying leaves out the rows
// whose new key is shared or NULL. KeyedRows keeps them so that a diff can
// still report them.
type KeyedRows struct {
	Rows prolly.Map
	Sch  schema.Schema

	// unpaired holds the rows left out of Rows, as stored before rekeying.
	unpaired prolly.Map
	// unpairedCount is the number of rows in unpaired.
	unpairedCount int
	// rekeyer rekeys the rows of unpaired. It is nil when Rows was not rekeyed.
	rekeyer *durable.RowRekeyer
}

// CountUnpaired returns how many rows of |from| and of |to| are left out of
// their Rows.
func CountUnpaired(from, to KeyedRows) (int, int) {
	return from.unpairedCount, to.unpairedCount
}

// IterUnpaired calls |cb| with each row left out of Rows, rekeyed like Rows.
//
// Rekeying gives those rows the layout of Rows, so they read with Sch. A row
// whose new key is NULL keeps the NULL in its key.
func (r KeyedRows) IterUnpaired(ctx context.Context, cb func(k, v val.Tuple) error) error {
	if r.rekeyer == nil {
		return nil
	}
	return durable.IterRows(ctx, r.unpaired, func(k, v val.Tuple) error {
		nk, nv, err := r.rekeyer.Rekey(ctx, k, v)
		if err != nil {
			return err
		}
		return cb(nk, nv)
	})
}

// AlignPrimaryKeys returns |from| and |to| keyed by the primary key that
// AlignedSchema chooses, rekeying at most one side.
//
// [prolly.DiffMaps] pairs rows by key bytes, so the sides of a diff across a
// primary key change pair only once they share a key. The rekeyed side leaves
// out the rows that the key cannot hold, as KeyedRows describes.
func AlignPrimaryKeys(ctx context.Context, from, to KeyedRows) (KeyedRows, KeyedRows, error) {
	newSch, isFromRekeyed, err := AlignedSchema(from.Sch, to.Sch)
	if err != nil || newSch == nil {
		return from, to, err
	}
	if isFromRekeyed {
		from, err = rekeyBy(ctx, from, newSch, to.Sch)
	} else {
		to, err = rekeyBy(ctx, to, newSch, from.Sch)
	}
	return from, to, err
}

// AlignedSchema returns the schema that one side of a diff from |fromSch| to
// |toSch| takes to share the other side's primary key. It also reports whether
// that side is the from side, and returns a nil schema when the sides already
// store the same key.
//
// It prefers the key of |toSch|, the key the table has after the change, when
// the from side stores every column of it. Only the schemas decide, so a
// primary key change always keys rows the same way.
//
// It returns an error when neither side stores the other's key. The error
// wraps ErrPrimaryKeySetChanged unless a key column changed type.
func AlignedSchema(fromSch, toSch schema.Schema) (schema.Schema, bool, error) {
	if fromSch == nil || toSch == nil || schema.IsKeyless(fromSch) || schema.IsKeyless(toSch) {
		return nil, false, nil
	}
	if schema.ComparePrimaryKeys(fromSch, toSch) != schema.PrimaryKeysDiffer || isSameStoredKey(fromSch, toSch) {
		return nil, false, nil
	}
	if newSch, ok, err := schema.RekeyedSchema(fromSch, toSch); err != nil || ok {
		return newSch, true, err
	}
	if newSch, ok, err := schema.RekeyedSchema(toSch, fromSch); err != nil || ok {
		return newSch, false, err
	}
	return nil, false, unalignableError(fromSch, toSch)
}

// isSameStoredKey reports whether the primary keys of |fromSch| and |toSch|
// hold the same columns with the same encodings. Columns match by tag or
// name, in order.
//
// Such keys store the same bytes even across a type change such as
// VARCHAR(20) to VARCHAR(30), which [schema.ComparePrimaryKeys] reports as a
// different key.
func isSameStoredKey(fromSch, toSch schema.Schema) bool {
	fromCols, toCols := fromSch.GetPKCols().GetColumns(), toSch.GetPKCols().GetColumns()
	if len(fromCols) != len(toCols) {
		return false
	}
	for i, from := range fromCols {
		to := toCols[i]
		isSameCol := from.Tag == to.Tag || strings.EqualFold(from.Name, to.Name)
		if !isSameCol || from.TypeInfo.Encoding() != to.TypeInfo.Encoding() {
			return false
		}
	}
	return true
}

// rekeyBy returns |side| rekeyed to |newSch|, which holds the primary key of
// |other|. It returns an error when |side| stores a column of that key with
// another encoding.
func rekeyBy(ctx context.Context, side KeyedRows, newSch, other schema.Schema) (KeyedRows, error) {
	rk, ok := durable.NewRowRekeyer(side.Rows.NodeStore(), side.Sch, newSch)
	if !ok {
		return KeyedRows{}, unalignableError(side.Sch, other)
	}
	rows, unpaired, _, err := rk.RekeyMap(ctx, side.Rows)
	if err != nil {
		return KeyedRows{}, err
	}
	n, err := unpaired.Count()
	if err != nil {
		return KeyedRows{}, err
	}
	return KeyedRows{Rows: rows, Sch: newSch, unpaired: unpaired, unpairedCount: n, rekeyer: rk}, nil
}

// alignIndexes is AlignPrimaryKeys for rows held as durable indexes.
func alignIndexes(ctx context.Context, from, to durable.Index, fromSch, toSch schema.Schema) (KeyedRows, KeyedRows, error) {
	fromRows, err := durable.ProllyMapFromIndex(from)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	toRows, err := durable.ProllyMapFromIndex(to)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	return AlignPrimaryKeys(ctx, KeyedRows{Rows: fromRows, Sch: fromSch}, KeyedRows{Rows: toRows, Sch: toSch})
}

// unalignableError returns the error for a diff whose sides cannot take each
// other's primary key.
//
// It names a retyped key column when there is one, since that is the likeliest
// cause.
func unalignableError(fromSch, toSch schema.Schema) error {
	if from, to, ok := retypedKeyColumn(fromSch, toSch); ok {
		return fmt.Errorf("%w: column %s is %s on the from side and %s on the to side, so rows cannot be paired across the change",
			errPrimaryKeyTypeChanged, to.Name, from.TypeInfo.ToSqlType().String(), to.TypeInfo.ToSqlType().String())
	}
	return fmt.Errorf("%w from %s to %s, and neither side has every column of the other's primary key with the same type",
		ErrPrimaryKeySetChanged, primaryKeyString(fromSch), primaryKeyString(toSch))
}

// retypedKeyColumn returns both versions of a primary key column of |toSch|
// whose type differs in |fromSch|. Columns match by tag, or else by name.
func retypedKeyColumn(fromSch, toSch schema.Schema) (schema.Column, schema.Column, bool) {
	fromCols := fromSch.GetAllCols()
	for _, to := range toSch.GetPKCols().GetColumns() {
		from, ok := fromCols.GetByTag(to.Tag)
		if !ok {
			from, ok = fromCols.GetByNameCaseInsensitive(to.Name)
		}
		if ok && !from.TypeInfo.ToSqlType().Equals(to.TypeInfo.ToSqlType()) {
			return from, to, true
		}
	}
	return schema.Column{}, schema.Column{}, false
}

// primaryKeyString returns the primary key columns of |sch| as "(a, b)".
func primaryKeyString(sch schema.Schema) string {
	return "(" + strings.Join(sch.GetPKCols().GetColumnNames(), ", ") + ")"
}
