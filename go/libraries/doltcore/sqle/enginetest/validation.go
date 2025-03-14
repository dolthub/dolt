// Copyright 2020 Dolthub, Inc.
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

package enginetest

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func ValidateDatabase(ctx context.Context, db sql.Database) (err error) {
	switch tdb := db.(type) {
	case sqle.Database:
		return ValidateDoltDatabase(ctx, tdb)
	case mysql_db.PrivilegedDatabase:
		return ValidateDatabase(ctx, tdb.Unwrap())
	default:
		return nil
	}
}

func ValidateDoltDatabase(ctx context.Context, db sqle.Database) (err error) {
	if !types.IsFormat_DOLT(db.GetDoltDB().Format()) {
		return nil
	}
	for _, stage := range validationStages {
		if err = stage(ctx, db); err != nil {
			return err
		}
	}
	return
}

type validator func(ctx context.Context, db sqle.Database) error

var validationStages = []validator{
	validateChunkReferences,
	validateSecondaryIndexes,
}

// validateChunkReferences checks for dangling chunks.
func validateChunkReferences(ctx context.Context, db sqle.Database) error {
	validateIndex := func(ctx context.Context, idx durable.Index) error {
		m := durable.MapFromIndex(idx)
		return m.WalkNodes(ctx, func(ctx context.Context, nd tree.Node) error {
			if nd.Size() <= 0 {
				return fmt.Errorf("encountered nil tree.Node")
			}
			return nil
		})
	}

	cb := func(n doltdb.TableName, t *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		if sch == nil {
			return true, fmt.Errorf("expected non-nil schema: %v", sch)
		}

		rows, err := t.GetRowData(ctx)
		if err != nil {
			return true, err
		}
		if err = validateIndex(ctx, rows); err != nil {
			return true, err
		}

		indexes, err := t.GetIndexSet(ctx)
		if err != nil {
			return true, err
		}
		err = durable.IterAllIndexes(ctx, sch, indexes, func(_ string, idx durable.Index) error {
			return validateIndex(ctx, idx)
		})
		if err != nil {
			return true, err
		}
		return
	}

	return iterDatabaseTables(ctx, db, cb)
}

// validateSecondaryIndexes checks that secondary index contents are consistent
// with primary index contents.
func validateSecondaryIndexes(ctx context.Context, db sqle.Database) error {
	cb := func(n doltdb.TableName, t *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		rows, err := t.GetRowData(ctx)
		if err != nil {
			return false, err
		}
		primary := durable.MapFromIndex(rows)

		for _, def := range sch.Indexes().AllIndexes() {
			set, err := t.GetIndexSet(ctx)
			if err != nil {
				return true, err
			}
			idx, err := set.GetIndex(ctx, sch, nil, def.Name())
			if err != nil {
				return true, err
			}
			secondary := durable.MapFromIndex(idx)

			err = validateIndexConsistency(ctx, sch, def, primary, secondary)
			if err != nil {
				return true, err
			}
		}
		return false, nil
	}
	return iterDatabaseTables(ctx, db, cb)
}

func validateIndexConsistency(
	ctx context.Context,
	sch schema.Schema,
	def schema.Index,
	primary, secondary prolly.MapInterface,
) error {
	if schema.IsKeyless(sch) {
		return validateKeylessIndex(ctx, sch, def, primary, secondary)
	} else {
		return validatePkIndex(ctx, sch, def, primary, secondary)
	}
}

// printIndexContents prints the contents of |prollyMap| to stdout. Intended for use debugging
// index consistency issues.
func printIndexContents(ctx context.Context, prollyMap prolly.MapInterface) {
	fmt.Printf("Secondary index contents:\n")
	kd := prollyMap.KeyDesc()
	iterAll, _ := prollyMap.IterAll(ctx)
	for {
		k, _, err := iterAll.Next(ctx)
		if err == io.EOF {
			break
		}
		fmt.Printf("  - k: %v \n", kd.Format(ctx, k))
	}
}

func validateKeylessIndex(ctx context.Context, sch schema.Schema, def schema.Index, primary, secondary prolly.MapInterface) error {
	// Full-Text indexes do not make use of their internal map, so we may safely skip this check
	if def.IsFullText() {
		return nil
	}

	// Indexes on virtual columns cannot be rebuilt via the method below
	if isVirtualIndex(def, sch) {
		return nil
	}

	idxDesc, _ := secondary.Descriptors()
	builder := val.NewTupleBuilder(idxDesc, primary.NodeStore())
	mapping := ordinalMappingsForSecondaryIndex(sch, def)
	_, vd := primary.Descriptors()

	iter, err := primary.IterAll(ctx)
	if err != nil {
		return err
	}

	for {
		hashId, value, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// make secondary index key
		for i := range mapping {
			j := mapping.MapOrdinal(i)
			// first field in |value| is cardinality
			field := value.GetField(j + 1)

			if shouldDereferenceContent(j+1, vd, i, idxDesc) {
				field, err = dereferenceContent(ctx, vd, j+1, value, secondary.NodeStore())
				if err != nil {
					return err
				}
			} else if def.IsSpatial() {
				geom, err := dereferenceGeometry(ctx, vd, j+1, value, secondary.NodeStore())
				if err != nil {
					return err
				}
				geom, _, err = sqltypes.GeometryType{}.Convert(geom)
				if err != nil {
					return err
				}
				cell := tree.ZCell(geom.(sqltypes.GeometryValue))
				field = cell[:]
			}

			// Apply prefix lengths if they are configured
			if len(def.PrefixLengths()) > i {
				field = trimValueToPrefixLength(field, def.PrefixLengths()[i], vd.Types[j+1].Enc)
			}

			builder.PutRaw(i, field)
		}
		builder.PutRaw(idxDesc.Count()-1, hashId.GetField(0))
		k := builder.Build(primary.Pool())

		ok, err := secondary.Has(ctx, k)
		if err != nil {
			return err
		}
		if !ok {
			printIndexContents(ctx, secondary)
			return fmt.Errorf("index key %s not found in index %s", builder.Desc.Format(ctx, k), def.Name())
		}
	}
}

func validatePkIndex(ctx context.Context, sch schema.Schema, def schema.Index, primary, secondary prolly.MapInterface) error {
	// Full-Text indexes do not make use of their internal map, so we may safely skip this check
	if def.IsFullText() {
		return nil
	}

	// Indexes on virtual columns cannot be rebuilt via the method below
	if isVirtualIndex(def, sch) {
		return nil
	}

	// secondary indexes have empty values
	idxDesc, _ := secondary.Descriptors()
	builder := val.NewTupleBuilder(idxDesc, ns)
	mapping := ordinalMappingsForSecondaryIndex(sch, def)
	kd, vd := primary.Descriptors()

	// Before we walk through the primary index data and validate that every row in the primary index exists in the
	// secondary index, we also check that the primary index and secondary index have the same number of rows.
	// Otherwise, we won't catch if the secondary index has extra, bogus data in it.
	totalSecondaryCount, err := secondary.Count()
	if err != nil {
		return err
	}
	totalPrimaryCount, err := primary.Count()
	if err != nil {
		return err
	}
	if totalSecondaryCount != totalPrimaryCount {
		return fmt.Errorf("primary index row count (%d) does not match secondary index row count (%d)",
			totalPrimaryCount, totalSecondaryCount)
	}

	pkSize := kd.Count()
	iter, err := primary.IterAll(ctx)
	if err != nil {
		return err
	}

	for {
		key, value, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// make secondary index key
		for i := range mapping {
			j := mapping.MapOrdinal(i)
			if j < pkSize {
				builder.PutRaw(i, key.GetField(j))
			} else {
				field := value.GetField(j - pkSize)

				if shouldDereferenceContent(j-pkSize, vd, i, idxDesc) {
					field, err = dereferenceContent(ctx, vd, j-pkSize, value, secondary.NodeStore())
					if err != nil {
						return err
					}
				} else if def.IsSpatial() {
					geom, err := dereferenceGeometry(ctx, vd, j-pkSize, value, secondary.NodeStore())
					if err != nil {
						return err
					}
					geom, _, err = sqltypes.GeometryType{}.Convert(geom)
					if err != nil {
						return err
					}
					cell := tree.ZCell(geom.(sqltypes.GeometryValue))
					field = cell[:]
				}

				// Apply prefix lengths if they are configured
				if len(def.PrefixLengths()) > i {
					field = trimValueToPrefixLength(field, def.PrefixLengths()[i], vd.Types[j-pkSize].Enc)
				}

				builder.PutRaw(i, field)
			}
		}
		k := builder.Build(primary.Pool())

		ok, err := secondary.Has(ctx, k)
		if err != nil {
			return err
		}
		if !ok {
			printIndexContents(ctx, secondary)
			return fmt.Errorf("index key %v not found in index %s", builder.Desc.Format(ctx, k), def.Name())
		}
	}
}

func isVirtualIndex(def schema.Index, sch schema.Schema) bool {
	for _, colName := range def.ColumnNames() {
		col, ok := sch.GetAllCols().GetByName(colName)
		if !ok {
			panic(fmt.Sprintf("column not found: %s", colName))
		}
		if col.Virtual {
			return true
		}
	}
	return false
}

// shouldDereferenceContent returns true if address encoded content should be dereferenced when
// building a key for a secondary index. This is determined by looking at the encoding of the field
// in the main table (|tablePos| and |tableValueDescriptor|) and the encoding of the field in the index
// (|indexPos| and |indexKeyDescriptor|) and seeing if one is an address encoding and the other is not.
func shouldDereferenceContent(tablePos int, tableValueDescriptor val.TupleDesc, indexPos int, indexKeyDescriptor val.TupleDesc) bool {
	if tableValueDescriptor.Types[tablePos].Enc == val.StringAddrEnc && indexKeyDescriptor.Types[indexPos].Enc != val.StringAddrEnc {
		return true
	}

	if tableValueDescriptor.Types[tablePos].Enc == val.BytesAddrEnc && indexKeyDescriptor.Types[indexPos].Enc != val.BytesAddrEnc {
		return true
	}

	return false
}

// dereferenceContent dereferences an address encoded field (e.g. TEXT, BLOB) to load the content
// and return a []byte. |tableValueDescriptor| is the tuple descriptor for the value tuple of the main
// table, |tablePos| is the field index into the value tuple, and |tuple| is the value tuple from the
// main table.
func dereferenceContent(ctx context.Context, tableValueDescriptor val.TupleDesc, tablePos int, tuple val.Tuple, ns tree.NodeStore) ([]byte, error) {
	v, err := tree.GetField(ctx, tableValueDescriptor, tablePos, tuple, ns)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	switch x := v.(type) {
	case sql.StringWrapper:
		str, err := x.Unwrap(ctx)
		if err != nil {
			return nil, err
		}
		return []byte(str), nil
	case sql.BytesWrapper:
		return x.Unwrap(ctx)
	case string:
		return []byte(x), nil
	case []byte:
		return x, nil
	default:
		return nil, fmt.Errorf("unexpected type for address encoded content: %T", v)
	}
}

// dereferenceGeometry dereferences an address encoded geometry field to load the content
// and return a GeometryType. |tableValueDescriptor| is the tuple descriptor for the value tuple of the main
// table, |tablePos| is the field index into the value tuple, and |tuple| is the value tuple from the
// main table.
func dereferenceGeometry(ctx context.Context, tableValueDescriptor val.TupleDesc, tablePos int, tuple val.Tuple, ns tree.NodeStore) (interface{}, error) {
	v, err := tree.GetField(ctx, tableValueDescriptor, tablePos, tuple, ns)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	switch x := v.(type) {
	case string:
		return []byte(x), nil
	case []byte:
		return x, nil
	case sqltypes.Point, sqltypes.LineString, sqltypes.Polygon, sqltypes.MultiPoint, sqltypes.MultiLineString, sqltypes.MultiPolygon, sqltypes.GeometryType, sqltypes.GeomColl:
		return x, nil
	default:
		return nil, fmt.Errorf("unexpected type for address encoded content: %T", v)
	}
}

// trimValueToPrefixLength trims |value| by truncating the bytes after |prefixLength|. If |prefixLength|
// is zero or if |value| is nil, then no trimming is done and |value| is directly returned. The
// |encoding| param indicates the original encoding of |value| in the source table.
func trimValueToPrefixLength(value []byte, prefixLength uint16, encoding val.Encoding) []byte {
	if value == nil || prefixLength == 0 {
		return value
	}

	if uint16(len(value)) < prefixLength {
		prefixLength = uint16(len(value))
	}

	addTerminatingNullByte := false
	if encoding == val.BytesAddrEnc || encoding == val.StringAddrEnc {
		// If the original encoding was for a BLOB or TEXT field, then we need to add
		// a null byte at the end of the prefix to get it into StringEnc format.
		addTerminatingNullByte = true
	} else if prefixLength < uint16(len(value)) {
		// Otherwise, if we're trimming a StringEnc value, we also need to re-add the
		// null terminating byte.
		addTerminatingNullByte = true
	}

	newValue := make([]byte, prefixLength)
	copy(newValue, value[:prefixLength])
	if addTerminatingNullByte {
		newValue = append(newValue, byte(0))
	}

	return newValue
}

func ordinalMappingsForSecondaryIndex(sch schema.Schema, def schema.Index) (ord val.OrdinalMapping) {
	// assert empty values for secondary indexes
	if def.Schema().GetNonPKCols().Size() > 0 {
		panic("expected empty secondary index values")
	}

	secondary := def.Schema().GetPKCols()
	ord = make(val.OrdinalMapping, secondary.Size())

	for i := range ord {
		name := secondary.GetByIndex(i).Name
		ord[i] = -1

		pks := sch.GetPKCols().GetColumns()
		for j, col := range pks {
			if col.Name == name {
				ord[i] = j
			}
		}
		vals := sch.GetNonPKCols().GetColumns()
		for _, col := range vals {
			if col.Name == name {
				storedIdx, ok := sch.GetNonPKCols().StoredIndexByTag(col.Tag)
				if !ok {
					panic("column " + name + " not found")
				}
				ord[i] = storedIdx + len(pks)
			}
		}
		if ord[i] < 0 {
			panic("column " + name + " not found")
		}
	}
	return
}

// iterDatabaseTables is a utility to factor out common validation access patterns.
func iterDatabaseTables(
	ctx context.Context,
	db sqle.Database,
	cb func(name doltdb.TableName, t *doltdb.Table, sch schema.Schema) (bool, error),
) error {
	ddb := db.GetDoltDB()
	branches, err := ddb.GetBranches(ctx)
	if err != nil {
		return err
	}

	for _, branchRef := range branches {
		wsRef, err := ref.WorkingSetRefForHead(branchRef)
		if err != nil {
			return err
		}
		ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
		if err != nil {
			return err
		}

		r := ws.WorkingRoot()

		if err = r.IterTables(ctx, cb); err != nil {
			return err
		}
	}
	return nil
}
