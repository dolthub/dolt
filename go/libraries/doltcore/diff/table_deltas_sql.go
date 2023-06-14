// Copyright 2019 Dolthub, Inc.
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
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/queries"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"math/rand"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// TableDelta represents the change of a single table between two roots.
// FromFKs and ToFKs contain Foreign Keys that constrain columns in this table,
// they do not contain Foreign Keys that reference this table.
type TableDeltaSql struct {
	TableDeltaBase
	Summary       TableDeltaSummary
	FromTableHash string
	ToTableHash   string

	fromCreateTableStmt string
	toCreateTableStmt   string
}

var _ TableDelta = &TableDeltaSql{}

type schemaDiffInfo struct {
	FromTableHash       string
	FromCreateTableStmt string
	ToTableHash         string
	ToCreateTableStmt   string
}

type diffInfo struct {
	FromTableHash string
	ToTableHash   string
	DiffType      string
}

// GetStagedUnstagedTableDeltas represents staged and unstaged changes as TableDelta slices.
func GetStagedUnstagedTableDeltasFromSql(queryist queries.Queryist, sqlCtx *sql.Context) (staged, unstaged []TableDeltaSql, err error) {
	staged, err = GetTableDeltasFromSql(queryist, sqlCtx, "HEAD", "STAGED")
	if err != nil {
		return nil, nil, err
	}

	unstaged, err = GetTableDeltasFromSql(queryist, sqlCtx, "STAGED", "WORKING")
	if err != nil {
		return nil, nil, err
	}

	return staged, unstaged, nil
}

func getTableSchemaAtRef(queryist queries.Queryist, sqlCtx *sql.Context, tableName string, ref string, tc *tagCreator) (schema.Schema, error) {
	q := fmt.Sprintf("show create table %s as of '%s'", tableName, ref)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	if len(rows) != 1 {
		return nil, fmt.Errorf("creating schema, expected 1 row, got %d", len(rows))
	}
	createStmt := rows[0][1].(string)
	sch, err := schemaFromCreateTableStmt(sqlCtx, createStmt, tc)
	if err != nil {
		return nil, err
	}

	return sch, nil
}

type tagCreator struct {
	existingColTypes []string
	existingTags     schema.TagMapping
}

func (tc *tagCreator) CreateTagForCol(tableName, colName, colType string) (uint64, error) {
	seedBuffer := []byte{}
	for _, existingColType := range tc.existingColTypes {
		seedBuffer = append(seedBuffer, []byte(existingColType)...)
	}
	seedBuffer = append(seedBuffer, []byte(colType)...)
	seedBuffer = append(seedBuffer, []byte(tableName)...)
	seedBuffer = append(seedBuffer, []byte(colName)...)
	h := sha512.Sum512(seedBuffer)
	r := rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(h[:]))))

	var randTag uint64
	for {
		randTag = r.Uint64()
		if !tc.existingTags.Contains(randTag) {
			break
		}
	}

	tc.existingTags.Add(randTag, tableName)
	tc.existingColTypes = append(tc.existingColTypes, colType)

	return randTag, nil
}

func NewTagCreator() *tagCreator {
	return &tagCreator{
		existingColTypes: []string{},
		existingTags:     schema.TagMapping{},
	}
}

func schemaFromCreateTableStmt(sqlCtx *sql.Context, createTableStmt string, tc *tagCreator) (schema.Schema, error) {
	p, err := sqlparser.Parse(createTableStmt)
	if err != nil {
		return nil, err
	}
	ddl := p.(*sqlparser.DDL)

	sctx := sql.NewContext(sqlCtx)
	s, _, err := parse.TableSpecToSchema(sctx, ddl.TableSpec, false)
	if err != nil {
		return nil, err
	}
	tableName := ddl.Table.Name.String()

	cols := []schema.Column{}

	for _, col := range s.Schema {
		// TODO: is this necessary?
		if collatedType, ok := col.Type.(sql.TypeWithCollation); ok {
			col.Type, err = collatedType.WithNewCollation(sql.Collation_Default)
			if err != nil {
				return nil, err
			}
		}

		colName := col.Name
		colType := col.Type
		typeInfo, err := typeinfo.FromSqlType(colType)
		if err != nil {
			return nil, err
		}

		tag, err := tc.CreateTagForCol(tableName, colName, colType.String())
		if err != nil {
			return nil, err
		}
		sCol, err := schema.NewColumnWithTypeInfo(
			col.Name,
			tag,
			typeInfo,
			col.PrimaryKey,
			"",
			false,
			col.Comment,
		)
		cols = append(cols, sCol)
	}

	sch, err := schema.NewSchema(schema.NewColCollection(cols...), nil, schema.Collation_Default, nil, nil)
	if err != nil {
		return nil, err
	}

	return sch, err
}

func getDiffChangeBetweenRefs(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef string) ([]TableDeltaSummary, error) {
	q := fmt.Sprintf("select * from dolt_diff_summary('%s', '%s')", fromRef, toRef)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	summaries := []TableDeltaSummary{}
	for _, row := range rows {
		fromTableName := row[0].(string)
		toTableName := row[1].(string)
		diffType := row[2].(string)
		dataChangeVal := row[3]
		schemaChangeVal := row[4]

		dataChange, err := queries.GetTinyIntColAsBool(dataChangeVal)
		if err != nil {
			return nil, err
		}
		schemaChange, err := queries.GetTinyIntColAsBool(schemaChangeVal)
		if err != nil {
			return nil, err
		}

		summary := TableDeltaSummary{
			DiffType:      diffType,
			DataChange:    dataChange,
			SchemaChange:  schemaChange,
			TableName:     toTableName, // TODO: should this be fromTableName?
			FromTableName: fromTableName,
			ToTableName:   toTableName,
		}
		summaries = append(summaries, summary)
	}

	return summaries, nil
}

func isTableEmpty(queryist queries.Queryist, sqlCtx *sql.Context, ref, tableName string) (bool, error) {
	q := fmt.Sprintf("select count(*) from %s as of '%s' limit 1;", tableName, ref)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, fmt.Sprintf("table not found: %s", tableName)) {
			return true, nil
		}
		return false, err
	}
	if len(rows) == 0 {
		return true, nil
	}
	return false, nil
}

func tableDataDiffsBetweenRefsExist(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef, tableName string) (bool, error) {
	q := fmt.Sprintf("select count(*) from dolt_diff('%s', '%s', '%s') limit 1", fromRef, toRef, tableName)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	return true, nil
}

func getTableDataDiffsBetweenRefs(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef, tableName string) ([]diffInfo, error) {
	diffs := []diffInfo{}
	q := fmt.Sprintf("select to_table_hash, from_table_hash, diff_type from dolt_diff('%s', '%s', '%s')", fromRef, toRef, tableName)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return diffs, err
	}

	for _, row := range rows {
		toTableHash := row[0].(string)
		fromTableHash := row[1].(string)
		diffType := row[2].(string)
		d := diffInfo{ToTableHash: toTableHash, FromTableHash: fromTableHash, DiffType: diffType}
		diffs = append(diffs, d)
	}
	return diffs, nil
}

func getTableSchemaDiffBetweenRefs(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef, tableName string) (schemaDiffInfo, error) {
	q := fmt.Sprintf("select * from dolt_schema_diff('%s', '%s', '%s')", fromRef, toRef, tableName)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return schemaDiffInfo{}, err
	}
	if len(rows) != 1 {
		return schemaDiffInfo{}, fmt.Errorf("expected 1 schema diff row, got %d", len(rows))
	}
	row := rows[0]

	fromCreateTableStmt := row[2].(string)
	toCreateTableStmt := row[3].(string)
	fromTableHash := row[4].(string)
	toTableHash := row[5].(string)
	diff := schemaDiffInfo{
		FromCreateTableStmt: fromCreateTableStmt,
		ToCreateTableStmt:   toCreateTableStmt,
		FromTableHash:       fromTableHash,
		ToTableHash:         toTableHash,
	}

	return diff, nil
}

// GetTableDeltas returns a slice of TableDelta objects for each table that changed between fromRoot and toRoot.
// It matches tables across roots by finding Schemas with Column tags in common.
func GetTableDeltasFromSql(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef string) (deltas []TableDeltaSql, err error) {
	fromDeltas := []TableDeltaSql{}
	fromTables, err := getAllTablesAtRef(queryist, sqlCtx, fromRef)
	if err != nil {
		return nil, err
	}
	for _, table := range fromTables {
		delta := TableDeltaSql{
			TableDeltaBase: TableDeltaBase{
				FromName:         table.name,
				FromSch:          table.sch,
				FromFks:          table.fks,
				FromFksParentSch: table.fkParentSch,
			},
		}
		fromDeltas = append(fromDeltas, delta)
	}

	toDeltas := []TableDeltaSql{}
	toTables, err := getAllTablesAtRef(queryist, sqlCtx, toRef)
	if err != nil {
		return nil, err
	}
	for _, table := range toTables {
		delta := TableDeltaSql{
			TableDeltaBase: TableDeltaBase{
				ToName:         table.name,
				ToSch:          table.sch,
				ToFks:          table.fks,
				ToFksParentSch: table.fkParentSch,
			},
		}
		toDeltas = append(toDeltas, delta)
	}

	deltas, err = matchTableDeltasSql(fromDeltas, toDeltas)
	if err != nil {
		return nil, err
	}

	for i, delta := range deltas {
		isAdded := delta.FromName == "" && delta.ToName != ""
		isDropped := delta.FromName != "" && delta.ToName == ""
		isRenamed := delta.FromName != "" && delta.ToName != "" && delta.FromName != delta.ToName

		var schemaChanged bool
		var dataChanged bool
		var schemaDiff schemaDiffInfo
		if isAdded {
			schemaDiff, err = getTableSchemaDiffBetweenRefs(queryist, sqlCtx, fromRef, toRef, delta.ToName)
			if err != nil {
				return nil, err
			}
			isEmpty, err := isTableEmpty(queryist, sqlCtx, toRef, delta.ToName)
			if err != nil {
				return nil, err
			}
			dataChanged = !isEmpty
			schemaChanged = true
		} else if isDropped {
			schemaDiff, err = getTableSchemaDiffBetweenRefs(queryist, sqlCtx, fromRef, toRef, delta.FromName)
			if err != nil {
				return nil, err
			}
			isEmpty, err := isTableEmpty(queryist, sqlCtx, fromRef, delta.FromName)
			if err != nil {
				return nil, err
			}
			dataChanged = !isEmpty
			schemaChanged = true
		} else if isRenamed {
			panic("not sure if this is possible")
		} else {
			name := delta.FromName
			schemaDiff, err = getTableSchemaDiffBetweenRefs(queryist, sqlCtx, fromRef, toRef, name)
			if err != nil {
				return nil, err
			}
			dataChanged = schemaDiff.FromTableHash != schemaDiff.ToTableHash

			// WORKS, BUT GETS ALL ROWS
			//rows, err := getTableDataDiffsBetweenRefs(queryist, sqlCtx, fromRef, toRef, name)
			//if err != nil {
			//	return nil, err
			//}
			//dataChanged = len(rows) > 0

			fromSchHash, err := delta.FromSch.GetHash()
			if err != nil {
				return nil, err
			}
			toSchHash, err := delta.ToSch.GetHash()
			if err != nil {
				return nil, err
			}
			schemaChanged = fromSchHash != toSchHash
		}

		delta.FromTableHash = schemaDiff.FromTableHash
		delta.ToTableHash = schemaDiff.ToTableHash
		delta.fromCreateTableStmt = schemaDiff.FromCreateTableStmt
		delta.toCreateTableStmt = schemaDiff.ToCreateTableStmt

		delta.Summary.DataChange = dataChanged
		delta.Summary.SchemaChange = schemaChanged

		deltas[i] = delta
	}

	deltas, err = filterUnmodifiedTableDeltasSql(deltas)
	if err != nil {
		return nil, err
	}

	// Make sure we always return the same order of deltas
	sort.Slice(deltas, func(i, j int) bool {
		if deltas[i].FromName == deltas[j].FromName {
			return deltas[i].ToName < deltas[j].ToName
		}
		return deltas[i].FromName < deltas[j].FromName
	})

	return deltas, nil
}

type sqlTable struct {
	name        string
	sch         schema.Schema
	fks         []doltdb.ForeignKey
	fkParentSch map[string]schema.Schema
}

func getAllTablesAtRef(queryist queries.Queryist, sqlCtx *sql.Context, ref string) ([]sqlTable, error) {
	tableNames, err := queries.GetTableNamesAtRef(queryist, sqlCtx, ref)
	if err != nil {
		return nil, err
	}

	cache := newTableSchemaCache(ref)
	tagCreator := &tagCreator{
		existingColTypes: []string{},
		existingTags:     schema.TagMapping{},
	}

	tables := []sqlTable{}
	for tableName := range tableNames {
		sch, err := cache.GetTableSchema(queryist, sqlCtx, tableName, tagCreator)
		if err != nil {
			return nil, err
		}
		fks, err := getForeignKeysForTableSql(queryist, sqlCtx, tableName, tagCreator, cache)
		if err != nil {
			return nil, err
		}
		fkParentSch, err := getFkParentSchsSql(queryist, sqlCtx, fks, tagCreator, cache)
		if err != nil {
			return nil, err
		}
		table := sqlTable{
			name:        tableName,
			sch:         sch,
			fks:         fks,
			fkParentSch: fkParentSch,
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func matchTableDeltasSql(fromDeltas, toDeltas []TableDeltaSql) (deltas []TableDeltaSql, err error) {
	var matchedNames []string
	from := make(map[string]TableDeltaSql, len(fromDeltas))
	for _, f := range fromDeltas {
		from[f.FromName] = f
	}

	to := make(map[string]TableDeltaSql, len(toDeltas))
	for _, t := range toDeltas {
		to[t.ToName] = t
		if _, ok := from[t.ToName]; ok {
			matchedNames = append(matchedNames, t.ToName)
		}
	}

	match := func(t, f TableDeltaSql) (TableDeltaSql, error) {
		return TableDeltaSql{
			TableDeltaBase: TableDeltaBase{
				FromName:         f.FromName,
				ToName:           t.ToName,
				FromSch:          f.FromSch,
				ToSch:            t.ToSch,
				FromFks:          f.FromFks,
				ToFks:            t.ToFks,
				FromFksParentSch: f.FromFksParentSch,
				ToFksParentSch:   t.ToFksParentSch,
			},
			FromTableHash:       f.FromTableHash,
			ToTableHash:         t.ToTableHash,
			fromCreateTableStmt: f.fromCreateTableStmt,
			toCreateTableStmt:   t.toCreateTableStmt,
		}, nil
	}

	deltas = []TableDeltaSql{}

	for _, name := range matchedNames {
		t := to[name]
		tInfo := t.GetBaseInfo()
		f := from[name]
		fInfo := f.GetBaseInfo()
		if schemasOverlap(tInfo.ToSch, fInfo.FromSch) {
			matched, err := match(t, f)
			if err != nil {
				return nil, err
			}
			deltas = append(deltas, matched)
			delete(from, fInfo.FromName)
			delete(to, tInfo.ToName)
		}
	}

	for _, f := range from {
		for _, t := range to {
			tInfo := t.GetBaseInfo()
			fInfo := f.GetBaseInfo()
			if schemasOverlap(fInfo.FromSch, tInfo.ToSch) {
				matched, err := match(t, f)
				if err != nil {
					return nil, err
				}
				deltas = append(deltas, matched)
				delete(from, fInfo.FromName)
				delete(to, tInfo.ToName)
			}
		}
	}

	// append unmatched TableDeltas
	for _, f := range from {
		deltas = append(deltas, f)
	}
	for _, t := range to {
		deltas = append(deltas, t)
	}

	return deltas, nil
}

func getFkParentSchsSql(queryist queries.Queryist, sqlCtx *sql.Context, fks []doltdb.ForeignKey, tc *tagCreator, cache *tableSchemaCache) (map[string]schema.Schema, error) {
	parentSchs := map[string]schema.Schema{}
	for _, fk := range fks {
		tableName := fk.ReferencedTableName
		sch, err := cache.GetTableSchema(queryist, sqlCtx, tableName, tc)
		if err != nil {
			return nil, err
		}
		parentSchs[tableName] = sch
	}
	return parentSchs, nil
}

func getForeignKeysForTableSql(queryist queries.Queryist, sqlCtx *sql.Context, tableName string, tc *tagCreator, cache *tableSchemaCache) ([]doltdb.ForeignKey, error) {
	refConstrQuery := fmt.Sprintf("select constraint_name, table_name, referenced_table_name, update_rule, delete_rule from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS where table_name = '%s';", tableName)
	refConstrRows, err := queries.GetRowsForSql(queryist, sqlCtx, refConstrQuery)
	if err != nil {
		return nil, err
	}

	var foreignKeys []doltdb.ForeignKey
	for _, row := range refConstrRows {
		name := row[0].(string)
		tableName := row[1].(string)
		referencedTableName := row[2].(string)
		updateRule := row[3].(string)
		deleteRule := row[4].(string)

		onUpdate, err := parseFkReferentialAction(sql.ForeignKeyReferentialAction(updateRule))
		if err != nil {
			return nil, err
		}
		onDelete, err := parseFkReferentialAction(sql.ForeignKeyReferentialAction(deleteRule))
		if err != nil {
			return nil, err
		}
		fk := doltdb.ForeignKey{
			Name:                name,
			TableName:           tableName,
			ReferencedTableName: referencedTableName,
			OnUpdate:            onUpdate,
			OnDelete:            onDelete,
		}

		tableSchema, err := cache.GetTableSchema(queryist, sqlCtx, tableName, tc)
		if err != nil {
			return nil, err
		}
		referenceTableSchema, err := cache.GetTableSchema(queryist, sqlCtx, referencedTableName, tc)
		if err != nil {
			return nil, err
		}

		keyUsageQuery := fmt.Sprintf("select column_name, referenced_column_name from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where table_name = '%s' and constraint_name = '%s';", tableName, name)
		keyUsageRows, err := queries.GetRowsForSql(queryist, sqlCtx, keyUsageQuery)
		if err != nil {
			return nil, err
		}
		for _, keyUsageRow := range keyUsageRows {
			columnName := keyUsageRow[0].(string)
			referencedColumnName := keyUsageRow[1].(string)

			column, ok := tableSchema.GetAllCols().GetByName(columnName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", columnName, tableName)
			}
			fk.TableColumns = append(fk.TableColumns, column.Tag)

			refColumn, ok := referenceTableSchema.GetAllCols().GetByName(referencedColumnName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", referencedColumnName, referencedTableName)
			}
			fk.ReferencedTableColumns = append(fk.ReferencedTableColumns, refColumn.Tag)
		}

		foreignKeys = append(foreignKeys, fk)
	}
	return foreignKeys, nil
}

func parseFkReferentialAction(refOp sql.ForeignKeyReferentialAction) (doltdb.ForeignKeyReferentialAction, error) {
	switch refOp {
	case sql.ForeignKeyReferentialAction_DefaultAction:
		return doltdb.ForeignKeyReferentialAction_DefaultAction, nil
	case sql.ForeignKeyReferentialAction_Restrict:
		return doltdb.ForeignKeyReferentialAction_Restrict, nil
	case sql.ForeignKeyReferentialAction_Cascade:
		return doltdb.ForeignKeyReferentialAction_Cascade, nil
	case sql.ForeignKeyReferentialAction_NoAction:
		return doltdb.ForeignKeyReferentialAction_NoAction, nil
	case sql.ForeignKeyReferentialAction_SetNull:
		return doltdb.ForeignKeyReferentialAction_SetNull, nil
	case sql.ForeignKeyReferentialAction_SetDefault:
		return doltdb.ForeignKeyReferentialAction_DefaultAction, sql.ErrForeignKeySetDefault.New()
	default:
		return doltdb.ForeignKeyReferentialAction_DefaultAction, fmt.Errorf("unknown foreign key referential action: %v", refOp)
	}
}

func filterUnmodifiedTableDeltasSql(deltas []TableDeltaSql) ([]TableDeltaSql, error) {
	var filtered []TableDeltaSql
	for _, d := range deltas {
		if d.FromName == "" || d.ToName == "" {
			// Table was added or dropped
			filtered = append(filtered, d)
			continue
		}

		hasChanges := d.Summary.SchemaChange || d.Summary.DataChange

		if hasChanges {
			// Take only modified tables
			filtered = append(filtered, d)
		}
	}

	return filtered, nil
}

type tableSchemaCache struct {
	tableSchemas map[string]schema.Schema
	ref          string
}

func (t *tableSchemaCache) GetTableSchema(queryist queries.Queryist, sqlCtx *sql.Context, tableName string, tc *tagCreator) (schema.Schema, error) {
	if t.tableSchemas == nil {
		t.tableSchemas = map[string]schema.Schema{}
	}
	if _, ok := t.tableSchemas[tableName]; !ok {
		tableSchema, err := getTableSchemaAtRef(queryist, sqlCtx, tableName, t.ref, tc)
		if err != nil {
			return nil, err
		}
		t.tableSchemas[tableName] = tableSchema
	}
	return t.tableSchemas[tableName], nil
}

func newTableSchemaCache(ref string) *tableSchemaCache {
	cache := tableSchemaCache{ref: ref}
	return &cache
}

func (td TableDeltaSql) GetBaseInfo() TableDeltaBase {
	return td.TableDeltaBase
}

// IsAdd returns true if the table was added between the fromRoot and toRoot.
func (td TableDeltaSql) IsAdd() bool {
	//return td.FromTable == nil && td.ToTable != nil
	return len(td.FromName) == 0 && len(td.ToName) > 0
}

// IsDrop returns true if the table was dropped between the fromRoot and toRoot.
func (td TableDeltaSql) IsDrop() bool {
	//return td.FromTable != nil && td.ToTable == nil
	return len(td.FromName) > 0 && len(td.ToName) == 0
}

// IsRename return true if the table was renamed between the fromRoot and toRoot.
func (td TableDeltaSql) IsRename() bool {
	if td.IsAdd() || td.IsDrop() {
		return false
	}
	return td.FromName != td.ToName
}

// HasHashChanged returns true if the hash of the table content has changed between
// the fromRoot and toRoot.
func (td TableDeltaSql) HasHashChanged() (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	toHash := td.ToTableHash
	fromHash := td.FromTableHash
	changed := toHash != fromHash

	return changed, nil
}

// HasSchemaChanged returns true if the table schema has changed between the
// fromRoot and toRoot.
func (td TableDeltaSql) HasSchemaChanged(ctx context.Context) (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	return td.Summary.SchemaChange, nil

	//if td.HasFKChanges() {
	//	return true, nil
	//}
	//
	//fromSchemaHash, err := td.FromTable.GetSchemaHash(ctx)
	//if err != nil {
	//	return false, err
	//}
	//
	//toSchemaHash, err := td.ToTable.GetSchemaHash(ctx)
	//if err != nil {
	//	return false, err
	//}
	//
	//return !fromSchemaHash.Equal(toSchemaHash), nil
}

func (td TableDeltaSql) HasDataChanged(ctx context.Context) (bool, error) {
	return td.Summary.DataChange, nil
}

func (td TableDeltaSql) GetTableCreateStatement(ctx context.Context, isFromTable bool) (string, error) {
	if isFromTable {
		return td.fromCreateTableStmt, nil
	} else {
		return td.toCreateTableStmt, nil
	}
}

func (td TableDeltaSql) HasPrimaryKeySetChanged() bool {
	return !schema.ArePrimaryKeySetsDiffable(types.Format_DOLT, td.FromSch, td.ToSch)
}

func (td TableDeltaSql) HasChanges() (bool, error) {
	hashChanged, err := td.HasHashChanged()
	if err != nil {
		return false, err
	}

	return td.HasFKChanges() || td.IsRename() || td.HasPrimaryKeySetChanged() || hashChanged, nil
}

// CurName returns the most recent name of the table.
func (td TableDeltaSql) CurName() string {
	if td.ToName != "" {
		return td.ToName
	}
	return td.FromName
}

func (td TableDeltaSql) HasFKChanges() bool {
	if len(td.FromFks) != len(td.ToFks) {
		return true
	}

	sort.Slice(td.FromFks, func(i, j int) bool {
		return td.FromFks[i].Name < td.FromFks[j].Name
	})
	sort.Slice(td.ToFks, func(i, j int) bool {
		return td.ToFks[i].Name < td.ToFks[j].Name
	})

	fromSchemaMap := td.FromFksParentSch
	fromSchemaMap[td.FromName] = td.FromSch
	toSchemaMap := td.ToFksParentSch
	toSchemaMap[td.ToName] = td.ToSch

	for i := range td.FromFks {
		if !td.FromFks[i].Equals(td.ToFks[i], fromSchemaMap, toSchemaMap) {
			return true
		}
	}

	return false
}

// GetSchemas returns the table's schema at the fromRoot and toRoot, or schema.Empty if the table did not exist.
func (td TableDeltaSql) GetSchemas(ctx context.Context) (from, to schema.Schema, err error) {
	if td.FromSch == nil {
		td.FromSch = schema.EmptySchema
	}
	if td.ToSch == nil {
		td.ToSch = schema.EmptySchema
	}
	return td.FromSch, td.ToSch, nil
}

func (td TableDeltaSql) IsKeyless(ctx context.Context) (bool, error) {
	f, t, err := td.GetSchemas(ctx)
	if err != nil {
		return false, err
	}

	// nil table is neither keyless nor keyed
	from, to := schema.IsKeyless(f), schema.IsKeyless(t)
	if td.FromName == "" {
		return to, nil
	} else if td.ToName == "" {
		return from, nil
	} else {
		if from && to {
			return true, nil
		} else if !from && !to {
			return false, nil
		} else {
			return false, fmt.Errorf("mismatched keyless and keyed schemas for table %s", td.CurName())
		}
	}
}

// GetSummary returns a summary of the table delta.
func (td TableDeltaSql) GetSummary(ctx context.Context) (*TableDeltaSummary, error) {
	dataChange, err := td.HasDataChanged(ctx)
	if err != nil {
		return nil, err
	}

	// Dropping a table is always a schema change, and also a data change if the table contained data
	if td.IsDrop() {
		return &TableDeltaSummary{
			TableName:     td.FromName,
			FromTableName: td.FromName,
			DataChange:    dataChange,
			SchemaChange:  true,
			DiffType:      "dropped",
		}, nil
	}

	// Creating a table is always a schema change, and also a data change if data was inserted
	if td.IsAdd() {
		return &TableDeltaSummary{
			TableName:    td.ToName,
			ToTableName:  td.ToName,
			DataChange:   dataChange,
			SchemaChange: true,
			DiffType:     "added",
		}, nil
	}

	// Renaming a table is always a schema change, and also a data change if the table data differs
	if td.IsRename() {
		return &TableDeltaSummary{
			TableName:     td.ToName,
			FromTableName: td.FromName,
			ToTableName:   td.ToName,
			DataChange:    dataChange,
			SchemaChange:  true,
			DiffType:      "renamed",
		}, nil
	}

	schemaChange, err := td.HasSchemaChanged(ctx)
	if err != nil {
		return nil, err
	}

	return &TableDeltaSummary{
		TableName:     td.FromName,
		FromTableName: td.FromName,
		ToTableName:   td.ToName,
		DataChange:    dataChange,
		SchemaChange:  schemaChange,
		DiffType:      "modified",
	}, nil
}
