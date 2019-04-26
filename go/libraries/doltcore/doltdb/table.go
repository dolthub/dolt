package doltdb

import (
	"context"
	"regexp"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
)

const (
	tableStructName = "table"

	schemaRefKey       = "schema_ref"
	tableRowsKey       = "rows"
	conflictsKey       = "conflicts"
	conflictSchemasKey = "conflict_schemas"

	// TableNameRegexStr is the regular expression that valid tables must match.
	TableNameRegexStr = `^[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
)

var tableNameRegex, _ = regexp.Compile(TableNameRegexStr)

// IsValidTableName returns true if the name matches the regular expression `[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
func IsValidTableName(name string) bool {
	return tableNameRegex.MatchString(name)
}

// Table is a struct which holds row data, as well as a reference to it's schema.
type Table struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

// NewTable creates a noms Struct which stores the schema and the row data
func NewTable(ctx context.Context, vrw types.ValueReadWriter, schema types.Value, rowData types.Map) *Table {
	schemaRef := writeValAndGetRef(ctx, vrw, schema)
	rowDataRef := writeValAndGetRef(ctx, vrw, rowData)

	sd := types.StructData{
		schemaRefKey: schemaRef,
		tableRowsKey: rowDataRef,
	}

	tableStruct := types.NewStruct(tableStructName, sd)
	return &Table{vrw, tableStruct}
}

func (t *Table) SetConflicts(ctx context.Context, schemas Conflict, conflictData types.Map) *Table {
	conflictsRef := writeValAndGetRef(ctx, t.vrw, conflictData)

	updatedSt := t.tableStruct.Set(conflictSchemasKey, schemas.ToNomsList(t.vrw))
	updatedSt = updatedSt.Set(conflictsKey, conflictsRef)

	return &Table{t.vrw, updatedSt}
}

func (t *Table) GetConflicts() (Conflict, types.Map, error) {
	schemasVal, ok := t.tableStruct.MaybeGet(conflictSchemasKey)

	if !ok {
		return Conflict{}, types.EmptyMap, ErrNoConflicts
	}

	schemas := ConflictFromTuple(schemasVal.(types.Tuple))
	conflictsVal := t.tableStruct.Get(conflictsKey)

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		confMap = confMapRef.TargetValue(context.TODO(), t.vrw).(types.Map)
	}

	return schemas, confMap, nil
}

func (t *Table) HasConflicts() bool {
	if t == nil {
		return false
	}

	_, ok := t.tableStruct.MaybeGet(conflictSchemasKey)

	return ok
}

func (t *Table) NumRowsInConflict() uint64 {
	if t == nil {
		return 0
	}

	conflictsVal, ok := t.tableStruct.MaybeGet(conflictsKey)

	if !ok {
		return 0
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		confMap = confMapRef.TargetValue(context.TODO(), t.vrw).(types.Map)
	}

	return confMap.Len()
}

func (t *Table) ClearConflicts() *Table {
	tSt := t.tableStruct.Delete(conflictSchemasKey)
	tSt = tSt.Delete(conflictsKey)

	return &Table{t.vrw, tSt}
}

func (t *Table) GetConflictSchemas() (base, sch, mergeSch schema.Schema, err error) {
	schemasVal, ok := t.tableStruct.MaybeGet(conflictSchemasKey)

	if ok {
		schemas := ConflictFromTuple(schemasVal.(types.Tuple))
		baseRef := schemas.Base.(types.Ref)
		valRef := schemas.Value.(types.Ref)
		mergeRef := schemas.MergeValue.(types.Ref)

		var err error
		var baseSch, sch, mergeSch schema.Schema
		if baseSch, err = refToSchema(context.TODO(), t.vrw, baseRef); err == nil {
			if sch, err = refToSchema(context.TODO(), t.vrw, valRef); err == nil {
				mergeSch, err = refToSchema(context.TODO(), t.vrw, mergeRef)
			}
		}

		return baseSch, sch, mergeSch, err
	}
	return nil, nil, nil, ErrNoConflicts
}

func refToSchema(ctx context.Context, vrw types.ValueReadWriter, ref types.Ref) (schema.Schema, error) {
	var schema schema.Schema
	err := pantoerr.PanicToErrorInstance(ErrNomsIO, func() error {
		schemaVal := ref.TargetValue(ctx, vrw)

		var err error
		schema, err = encoding.UnmarshalNomsValue(ctx, schemaVal)

		if err != nil {
			return err
		}

		return nil
	})

	return schema, err
}

// GetSchema will retrieve the schema being referenced from the table in noms and unmarshal it.
func (t *Table) GetSchema() schema.Schema {
	schemaRefVal := t.tableStruct.Get(schemaRefKey)
	schemaRef := schemaRefVal.(types.Ref)
	schema, _ := refToSchema(context.TODO(), t.vrw, schemaRef)

	return schema
}

func (t *Table) GetSchemaRef() types.Ref {
	return t.tableStruct.Get(schemaRefKey).(types.Ref)
}

// HasTheSameSchema tests the schema within 2 tables for equality
func (t *Table) HasTheSameSchema(t2 *Table) bool {
	schemaVal := t.tableStruct.Get(schemaRefKey)
	schemaRef := schemaVal.(types.Ref)

	schema2Val := t2.tableStruct.Get(schemaRefKey)
	schema2Ref := schema2Val.(types.Ref)

	return schemaRef.TargetHash() == schema2Ref.TargetHash()
}

// HashOf returns the hash of the underlying table struct
func (t *Table) HashOf() hash.Hash {
	return t.tableStruct.Hash()
}

func (t *Table) GetRowByPKVals(pkVals row.TaggedValues, sch schema.Schema) (row.Row, bool) {
	pkTuple := pkVals.NomsTupleForTags(sch.GetPKCols().Tags, true)
	return t.GetRow(pkTuple, sch)
}

// GetRow uses the noms Map containing the row data to lookup a row by primary key.  If a valid row exists with this pk
// then the supplied TableRowFactory will be used to create a TableRow using the row data.
func (t *Table) GetRow(pk types.Tuple, sch schema.Schema) (row.Row, bool) {
	rowMap := t.GetRowData()
	fieldsVal := rowMap.Get(context.TODO(), pk)

	if fieldsVal == nil {
		return nil, false
	}

	return row.FromNoms(sch, pk, fieldsVal.(types.Tuple)), true
}

// GetRows takes in a PKItr which will supply a stream of primary keys to be pulled from the table.  Each key is
// looked up sequentially.  If row data exists for a given pk it is converted to a TableRow, and added to the rows
// slice. If row data does not exist for a given pk it will be added to the missing slice.  The numPKs argument, if
// known helps allocate the right amount of memory for the results, but if the number of pks being requested isn't
// known then 0 can be used.
func (t *Table) GetRows(pkItr PKItr, numPKs int, sch schema.Schema) (rows []row.Row, missing []types.Value) {
	if numPKs < 0 {
		numPKs = 0
	}

	rows = make([]row.Row, 0, numPKs)
	missing = make([]types.Value, 0, numPKs)

	rowMap := t.GetRowData()

	for pk, ok := pkItr(); ok; pk, ok = pkItr() {
		fieldsVal := rowMap.Get(context.TODO(), pk)

		if fieldsVal == nil {
			missing = append(missing, pk)
		} else {
			r := row.FromNoms(sch, pk, fieldsVal.(types.Tuple))
			rows = append(rows, r)
		}
	}

	return rows, missing
}

// UpdateRows replaces the current row data and returns and updated Table.  Calls to UpdateRows will not be written to the
// database.  The root must be updated with the updated table, and the root must be committed or written.
func (t *Table) UpdateRows(ctx context.Context, updatedRows types.Map) *Table {
	rowDataRef := writeValAndGetRef(ctx, t.vrw, updatedRows)
	updatedSt := t.tableStruct.Set(tableRowsKey, rowDataRef)

	return &Table{t.vrw, updatedSt}
}

// GetRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t *Table) GetRowData() types.Map {
	rowMapRef := t.tableStruct.Get(tableRowsKey).(types.Ref)
	rowMap := rowMapRef.TargetValue(context.TODO(), t.vrw).(types.Map)
	return rowMap
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
	_, confData, err := t.GetConflicts()

	if err != nil {
		return nil, nil, nil, err
	}

	confEdit := confData.Edit()
	for _, pkTupleVal := range pkTuples {
		if confEdit.Has(context.TODO(), pkTupleVal) {
			removed++
			confEdit.Remove(pkTupleVal)
		} else {
			notFound = append(notFound, pkTupleVal)
		}
	}

	if removed == 0 {
		return invalid, notFound, tbl, nil
	}

	conflicts := confEdit.Map(context.TODO())
	conflictsRef := writeValAndGetRef(ctx, t.vrw, conflicts)
	updatedSt := t.tableStruct.Set(conflictsKey, conflictsRef)

	return invalid, notFound, &Table{t.vrw, updatedSt}, nil
}
