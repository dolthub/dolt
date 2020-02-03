// Copyright 2019 Liquidata, Inc.
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

package doltdb

import (
	"context"
	"errors"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"regexp"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	tableStructName = "table"

	schemaRefKey       = "schema_ref"
	tableRowsKey       = "rows"
	conflictsKey       = "conflicts"
	conflictSchemasKey = "conflict_schemas"

	// TableNameRegexStr is the regular expression that valid tables must match.
	TableNameRegexStr = `^[a-zA-Z]{1}$|^[a-zA-Z]+[-_0-9a-zA-Z]*[0-9a-zA-Z]+$`

	// We reserve all tables that begin with dolt_ for system use.
	DoltNamespace = "dolt_"

	// DoltQueryCatalogTableName is the name of the query catalog table
	DoltQueryCatalogTableName = "dolt_query_catalog"
)

// The set of reserved dolt_ tables that should be considered part of user space, like any other user-created table,
// for the purposes of the dolt command line.
var userSpaceReservedTables = set.NewStrSet([]string{
	DoltQueryCatalogTableName,
})

var tableNameRegex, _ = regexp.Compile(TableNameRegexStr)

// IsValidTableName returns true if the name matches the regular expression TableNameRegexStr.
// Table names must be composed of 1 or more letters and non-initial numerals, as well as the characters _ and -
func IsValidTableName(name string) bool {
	return tableNameRegex.MatchString(name)
}

// HasDoltPrefix returns a boolean whether or not the provided string is prefixed with the DoltNamespace. Users should
// not be able to create tables in this reserved namespace.
func HasDoltPrefix(s string) bool {
	return strings.HasPrefix(s, DoltNamespace)
}

// IsSystemTable returns whether the table name given is a system table that should not be included in command line
// output (e.g. dolt status) by default.
func IsSystemTable(name string) bool {
	return HasDoltPrefix(name) && !userSpaceReservedTables.Contains(name)
}

// Table is a struct which holds row data, as well as a reference to it's schema.
type Table struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

// NewTable creates a noms Struct which stores the schema and the row data
func NewTable(ctx context.Context, vrw types.ValueReadWriter, schema types.Value, rowData types.Map) (*Table, error) {
	schemaRef, err := writeValAndGetRef(ctx, vrw, schema)

	if err != nil {
		return nil, err
	}

	rowDataRef, err := writeValAndGetRef(ctx, vrw, rowData)

	if err != nil {
		return nil, err
	}

	sd := types.StructData{
		schemaRefKey: schemaRef,
		tableRowsKey: rowDataRef,
	}

	tableStruct, err := types.NewStruct(vrw.Format(), tableStructName, sd)

	if err != nil {
		return nil, err
	}

	return &Table{vrw, tableStruct}, nil
}

func (t *Table) Format() *types.NomsBinFormat {
	return t.vrw.Format()
}

// ValueReadWriter returns the ValueReadWriter for this table.
func (t *Table) ValueReadWriter() types.ValueReadWriter {
	return t.vrw
}

func (t *Table) SetConflicts(ctx context.Context, schemas Conflict, conflictData types.Map) (*Table, error) {
	conflictsRef, err := writeValAndGetRef(ctx, t.vrw, conflictData)

	if err != nil {
		return nil, err
	}

	tpl, err := schemas.ToNomsList(t.vrw)

	if err != nil {
		return nil, err
	}

	updatedSt, err := t.tableStruct.Set(conflictSchemasKey, tpl)

	if err != nil {
		return nil, err
	}

	updatedSt, err = updatedSt.Set(conflictsKey, conflictsRef)

	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, updatedSt}, nil
}

func (t *Table) GetConflicts(ctx context.Context) (Conflict, types.Map, error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)

	if err != nil {
		return Conflict{}, types.EmptyMap, err
	}

	if !ok {
		return Conflict{}, types.EmptyMap, ErrNoConflicts
	}

	schemas, err := ConflictFromTuple(schemasVal.(types.Tuple))

	if err != nil {
		return Conflict{}, types.EmptyMap, err
	}

	conflictsVal, _, err := t.tableStruct.MaybeGet(conflictsKey)

	if err != nil {
		return Conflict{}, types.EmptyMap, err
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		v, err := confMapRef.TargetValue(ctx, t.vrw)

		if err != nil {
			return Conflict{}, types.EmptyMap, err
		}

		confMap = v.(types.Map)
	}

	return schemas, confMap, nil
}

func (t *Table) HasConflicts() (bool, error) {
	if t == nil {
		return false, nil
	}

	_, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)

	return ok, err
}

func (t *Table) NumRowsInConflict(ctx context.Context) (uint64, error) {
	if t == nil {
		return 0, nil
	}

	conflictsVal, ok, err := t.tableStruct.MaybeGet(conflictsKey)

	if err != nil {
		return 0, err
	}

	if !ok {
		return 0, nil
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		v, err := confMapRef.TargetValue(ctx, t.vrw)

		if err != nil {
			return 0, err
		}

		confMap = v.(types.Map)
	}

	return confMap.Len(), nil
}

func (t *Table) ClearConflicts() (*Table, error) {
	tSt, err := t.tableStruct.Delete(conflictSchemasKey)

	if err != nil {
		return nil, err
	}

	tSt, err = tSt.Delete(conflictsKey)

	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, tSt}, nil
}

func (t *Table) GetConflictSchemas(ctx context.Context) (base, sch, mergeSch schema.Schema, err error) {
	schemasVal, ok, err := t.tableStruct.MaybeGet(conflictSchemasKey)

	if err != nil {
		return nil, nil, nil, err
	}

	if ok {
		schemas, err := ConflictFromTuple(schemasVal.(types.Tuple))

		if err != nil {
			return nil, nil, nil, err
		}

		baseRef := schemas.Base.(types.Ref)
		valRef := schemas.Value.(types.Ref)
		mergeRef := schemas.MergeValue.(types.Ref)

		var baseSch, sch, mergeSch schema.Schema
		if baseSch, err = refToSchema(ctx, t.vrw, baseRef); err == nil {
			if sch, err = refToSchema(ctx, t.vrw, valRef); err == nil {
				mergeSch, err = refToSchema(ctx, t.vrw, mergeRef)
			}
		}

		return baseSch, sch, mergeSch, err
	}
	return nil, nil, nil, ErrNoConflicts
}

func refToSchema(ctx context.Context, vrw types.ValueReadWriter, ref types.Ref) (schema.Schema, error) {
	schemaVal, err := ref.TargetValue(ctx, vrw)

	if err != nil {
		return nil, err
	}

	schema, err := encoding.UnmarshalNomsValue(ctx, vrw.Format(), schemaVal)

	if err != nil {
		return nil, err
	}

	return schema, nil
}

// GetSchema will retrieve the schema being referenced from the table in noms and unmarshal it.
func (t *Table) GetSchema(ctx context.Context) (schema.Schema, error) {
	schemaRefVal, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return nil, err
	}

	schemaRef := schemaRefVal.(types.Ref)
	return refToSchema(ctx, t.vrw, schemaRef)
}

func (t *Table) GetSchemaRef() (types.Ref, error) {
	v, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return types.Ref{}, err
	}

	if v == nil {
		return types.Ref{}, errors.New("missing schema")
	}

	return v.(types.Ref), nil
}

// HasTheSameSchema tests the schema within 2 tables for equality
func (t *Table) HasTheSameSchema(t2 *Table) (bool, error) {
	schemaVal, _, err := t.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return false, err
	}

	schemaRef := schemaVal.(types.Ref)

	schema2Val, _, err := t2.tableStruct.MaybeGet(schemaRefKey)

	if err != nil {
		return false, err
	}

	schema2Ref := schema2Val.(types.Ref)

	return schemaRef.TargetHash() == schema2Ref.TargetHash(), nil
}

// HashOf returns the hash of the underlying table struct
func (t *Table) HashOf() (hash.Hash, error) {
	return t.tableStruct.Hash(t.vrw.Format())
}

func (t *Table) GetRowByPKVals(ctx context.Context, pkVals row.TaggedValues, sch schema.Schema) (row.Row, bool, error) {
	pkTuple := pkVals.NomsTupleForTags(t.vrw.Format(), sch.GetPKCols().Tags, true)
	pkTupleVal, err := pkTuple.Value(ctx)

	if err != nil {
		return nil, false, err
	}

	return t.GetRow(ctx, pkTupleVal.(types.Tuple), sch)
}

// GetRow uses the noms DestRef containing the row data to lookup a row by primary key.  If a valid row exists with this pk
// then the supplied TableRowFactory will be used to create a TableRow using the row data.
func (t *Table) GetRow(ctx context.Context, pk types.Tuple, sch schema.Schema) (row.Row, bool, error) {
	rowMap, err := t.GetRowData(ctx)

	if err != nil {
		return nil, false, err
	}

	fieldsVal, _, err := rowMap.MaybeGet(ctx, pk)

	if err != nil {
		return nil, false, err
	}

	if fieldsVal == nil {
		return nil, false, nil
	}

	r, err := row.FromNoms(sch, pk, fieldsVal.(types.Tuple))

	if err != nil {
		return nil, false, err
	}

	return r, true, nil
}

// GetRows takes in a PKItr which will supply a stream of primary keys to be pulled from the table.  Each key is
// looked up sequentially.  If row data exists for a given pk it is converted to a TableRow, and added to the rows
// slice. If row data does not exist for a given pk it will be added to the missing slice.  The numPKs argument, if
// known helps allocate the right amount of memory for the results, but if the number of pks being requested isn't
// known then 0 can be used.
func (t *Table) GetRows(ctx context.Context, pkItr PKItr, numPKs int, sch schema.Schema) (rows []row.Row, missing []types.Value, err error) {
	if numPKs < 0 {
		numPKs = 0
	}

	rows = make([]row.Row, 0, numPKs)
	missing = make([]types.Value, 0, numPKs)

	rowMap, err := t.GetRowData(ctx)

	if err != nil {
		return nil, nil, err
	}

	for pk, ok, err := pkItr(); ok; pk, ok, err = pkItr() {
		if err != nil {
			return nil, nil, err
		}

		fieldsVal, _, err := rowMap.MaybeGet(ctx, pk)

		if err != nil {
			return nil, nil, err
		}

		if fieldsVal == nil {
			missing = append(missing, pk)
		} else {
			r, err := row.FromNoms(sch, pk, fieldsVal.(types.Tuple))

			if err != nil {
				return nil, nil, err
			}

			rows = append(rows, r)
		}
	}

	return rows, missing, nil
}

// UpdateRows replaces the current row data and returns and updated Table.  Calls to UpdateRows will not be written to the
// database.  The root must be updated with the updated table, and the root must be committed or written.
func (t *Table) UpdateRows(ctx context.Context, updatedRows types.Map) (*Table, error) {
	rowDataRef, err := writeValAndGetRef(ctx, t.vrw, updatedRows)

	if err != nil {
		return nil, err
	}

	updatedSt, err := t.tableStruct.Set(tableRowsKey, rowDataRef)

	if err != nil {
		return nil, err
	}

	return &Table{t.vrw, updatedSt}, nil
}

// GetRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t *Table) GetRowData(ctx context.Context) (types.Map, error) {
	val, _, err := t.tableStruct.MaybeGet(tableRowsKey)

	if err != nil {
		return types.EmptyMap, err
	}

	rowMapRef := val.(types.Ref)

	val, err = rowMapRef.TargetValue(ctx, t.vrw)

	if err != nil {
		return types.EmptyMap, err
	}

	rowMap := val.(types.Map)
	return rowMap, nil
}

/*func (t *Table) ResolveConflicts(keys []map[uint64]string) (invalid, notFound []types.Value, tbl *Table, err error) {
	sch := t.GetSchema()
	pkCols := sch.GetPKCols()
	convFuncs := make(map[uint64]doltcore.ConvFunc)

	pkCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		convFuncs[tag] = doltcore.GetConvFunc(types.StringKind, col.Kind)
		return false
	})

	var pkTuples []types.Tuple
	for _, keyStrs := range keys {
		i := 0
		pk := make([]types.Value, pkCols.Size()*2)
		pkCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
			strForTag, ok := keyStrs[tag]
			pk[i] = types.Uint(tag)

			if ok {
				convFunc, _ := convFuncs[tag]
				pk[i+1], err = convFunc(types.String(strForTag))

				if err != nil {
					invalid = append(invalid, keyStrs)
				}
			} else {
				pk[i+1] = types.NullValue
			}

			i += 2
			return false
		})

		pkTupleVal := types.NewTuple(pk...)
		pkTuples = append(pkTuples, pkTupleVal)
	}
}*/

func (t *Table) ResolveConflicts(ctx context.Context, pkTuples []types.Value) (invalid, notFound []types.Value, tbl *Table, err error) {
	removed := 0
	_, confData, err := t.GetConflicts(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	confEdit := confData.Edit()
	for _, pkTupleVal := range pkTuples {
		if has, err := confData.Has(ctx, pkTupleVal); err != nil {
			return nil, nil, nil, err
		} else if has {
			removed++
			confEdit.Remove(pkTupleVal)
		} else {
			notFound = append(notFound, pkTupleVal)
		}
	}

	if removed == 0 {
		return invalid, notFound, tbl, nil
	}

	conflicts, err := confEdit.Map(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	conflictsRef, err := writeValAndGetRef(ctx, t.vrw, conflicts)

	if err != nil {
		return nil, nil, nil, err
	}

	updatedSt, err := t.tableStruct.Set(conflictsKey, conflictsRef)

	if err != nil {
		return nil, nil, nil, err
	}

	return invalid, notFound, &Table{t.vrw, updatedSt}, nil
}
