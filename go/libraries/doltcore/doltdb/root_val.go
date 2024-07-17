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

package doltdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	ddbRootStructName = "dolt_db_root"

	tablesKey        = "tables"
	foreignKeyKey    = "foreign_key"
	featureVersKey   = "feature_ver"
	rootCollationKey = "root_collation_key"

	// deprecated
	superSchemasKey = "super_schemas"
)

type FeatureVersion int64

// DoltFeatureVersion is described in feature_version.md.
// only variable for testing.
var DoltFeatureVersion FeatureVersion = 7 // last bumped when fixing bug related to GeomAddrs not getting pushed

// RootValue is the value of the Database and is the committed value in every Dolt or Doltgres commit.
type RootValue interface {
	Rootish

	// CreateDatabaseSchema creates the given schema. This differs from a table's schema.
	CreateDatabaseSchema(ctx context.Context, dbSchema schema.DatabaseSchema) (RootValue, error)
	// DebugString returns a human readable string with the contents of this root. If |transitive| is true, row data from
	// all tables is also included. This method is very expensive for large root values, so |transitive| should only be used
	// when debugging tests.
	DebugString(ctx context.Context, transitive bool) string
	// GetCollation returns the database collation.
	GetCollation(ctx context.Context) (schema.Collation, error)
	// GetDatabaseSchemas returns all schemas. These differ from a table's schema.
	GetDatabaseSchemas(ctx context.Context) ([]schema.DatabaseSchema, error)
	// GetFeatureVersion returns the feature version of this root, if one is written
	GetFeatureVersion(ctx context.Context) (ver FeatureVersion, ok bool, err error)
	// GetForeignKeyCollection returns the ForeignKeyCollection for this root. As collections are meant to be modified
	// in-place, each returned collection may freely be altered without affecting future returned collections from this root.
	GetForeignKeyCollection(ctx context.Context) (*ForeignKeyCollection, error)
	// GetTable will retrieve a table by its case-sensitive name.
	GetTable(ctx context.Context, tName TableName) (*Table, bool, error)
	// GetTableHash returns the hash of the given case-sensitive table name.
	GetTableHash(ctx context.Context, tName TableName) (hash.Hash, bool, error)
	// GetTableNames retrieves the lists of all tables for a RootValue
	GetTableNames(ctx context.Context, schemaName string) ([]string, error)
	// HandlePostMerge handles merging for any root elements that are not handled by the standard merge workflow. This
	// is called at the end of the standard merge workflow. The calling root is the merged root, so it is valid to
	// return it if no further merge work needs to be done. This is primarily used by Doltgres.
	HandlePostMerge(ctx context.Context, ourRoot, theirRoot, ancRoot RootValue) (RootValue, error)
	// HasTable returns whether the root has a table with the given case-sensitive name.
	HasTable(ctx context.Context, tName TableName) (bool, error)
	// IterTables calls the callback function cb on each table in this RootValue.
	IterTables(ctx context.Context, cb func(name TableName, table *Table, sch schema.Schema) (stop bool, err error)) error
	// NodeStore returns this root's NodeStore.
	NodeStore() tree.NodeStore
	// NomsValue returns this root's storage as a noms value.
	NomsValue() types.Value
	// PutForeignKeyCollection returns a new root with the given foreign key collection.
	PutForeignKeyCollection(ctx context.Context, fkc *ForeignKeyCollection) (RootValue, error)
	// PutTable inserts a table by name into the map of tables. If a table already exists with that name it will be replaced
	PutTable(ctx context.Context, tName TableName, table *Table) (RootValue, error)
	// RemoveTables removes the given case-sensitive tables from the root, and returns a new root.
	RemoveTables(ctx context.Context, skipFKHandling bool, allowDroppingFKReferenced bool, tables ...TableName) (RootValue, error)
	// RenameTable renames a table by changing its string key in the RootValue's table map. In order to preserve
	// column tag information, use this method instead of a table drop + add.
	RenameTable(ctx context.Context, oldName, newName TableName) (RootValue, error)
	// ResolveTableName resolves a case-insensitive name to the exact name as stored in Dolt. Returns false if no matching
	// name was found.
	ResolveTableName(ctx context.Context, tName TableName) (string, bool, error)
	// SetCollation sets the given database collation and returns a new root.
	SetCollation(ctx context.Context, collation schema.Collation) (RootValue, error)
	// SetFeatureVersion sets the feature version and returns a new root.
	SetFeatureVersion(v FeatureVersion) (RootValue, error)
	// SetTableHash sets the table with the given case-sensitive name to the new hash, and returns a new root.
	SetTableHash(ctx context.Context, tName string, h hash.Hash) (RootValue, error)
	// TableListHash returns a unique digest of the list of table names
	TableListHash() uint64
	// VRW returns this root's ValueReadWriter.
	VRW() types.ValueReadWriter
}

// rootValue is Dolt's implementation of RootValue.
type rootValue struct {
	vrw        types.ValueReadWriter
	ns         tree.NodeStore
	st         rootValueStorage
	fkc        *ForeignKeyCollection // cache the first load
	rootHash   hash.Hash             // cache the first load
	tablesHash uint64
}

var _ RootValue = (*rootValue)(nil)

type tableEdit struct {
	name TableName
	ref  *types.Ref

	// Used for rename.
	old_name TableName
}

// NewRootValue returns a new RootValue. This is a variable as it's changed in Doltgres.
var NewRootValue = func(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, v types.Value) (RootValue, error) {
	var storage rootValueStorage

	if vrw.Format().UsesFlatbuffers() {
		srv, err := serial.TryGetRootAsRootValue([]byte(v.(types.SerialMessage)), serial.MessagePrefixSz)
		if err != nil {
			return nil, err
		}
		storage = fbRvStorage{srv}
	} else {
		st, ok := v.(types.Struct)
		if !ok {
			return nil, errors.New("invalid value passed to newRootValue")
		}

		storage = nomsRvStorage{st}
	}
	ver, ok, err := storage.GetFeatureVersion()
	if err != nil {
		return nil, err
	}
	if ok {
		if DoltFeatureVersion < ver {
			return nil, ErrClientOutOfDate{
				ClientVer: DoltFeatureVersion,
				RepoVer:   ver,
			}
		}
	}

	return &rootValue{vrw, ns, storage, nil, hash.Hash{}, 0}, nil
}

// EmptyRootValue returns an empty RootValue. This is a variable as it's changed in Doltgres.
var EmptyRootValue = func(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore) (RootValue, error) {
	if vrw.Format().UsesFlatbuffers() {
		builder := flatbuffers.NewBuilder(80)

		emptyam, err := prolly.NewEmptyAddressMap(ns)
		if err != nil {
			return nil, err
		}
		ambytes := []byte(tree.ValueFromNode(emptyam.Node()).(types.SerialMessage))
		tablesoff := builder.CreateByteVector(ambytes)

		var empty hash.Hash
		fkoff := builder.CreateByteVector(empty[:])
		serial.RootValueStart(builder)
		serial.RootValueAddFeatureVersion(builder, int64(DoltFeatureVersion))
		serial.RootValueAddCollation(builder, serial.Collationutf8mb4_0900_bin)
		serial.RootValueAddTables(builder, tablesoff)
		serial.RootValueAddForeignKeyAddr(builder, fkoff)
		bs := serial.FinishMessage(builder, serial.RootValueEnd(builder), []byte(serial.RootValueFileID))
		return NewRootValue(ctx, vrw, ns, types.SerialMessage(bs))
	}

	empty, err := types.NewMap(ctx, vrw)
	if err != nil {
		return nil, err
	}

	sd := types.StructData{
		tablesKey:       empty,
		superSchemasKey: empty,
		foreignKeyKey:   empty,
		featureVersKey:  types.Int(DoltFeatureVersion),
	}

	st, err := types.NewStruct(vrw.Format(), ddbRootStructName, sd)
	if err != nil {
		return nil, err
	}

	return NewRootValue(ctx, vrw, ns, st)
}

// LoadRootValueFromRootIshAddr takes the hash of the commit or the hash of a
// working set and returns the corresponding RootValue.
func LoadRootValueFromRootIshAddr(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, h hash.Hash) (RootValue, error) {
	val, err := datas.LoadRootNomsValueFromRootIshAddr(ctx, vrw, h)
	if err != nil {
		return nil, err
	}
	return decodeRootNomsValue(ctx, vrw, ns, val)
}

func decodeRootNomsValue(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, val types.Value) (RootValue, error) {
	if val == nil {
		return nil, ErrNoRootValAtHash
	}

	if !isRootValue(vrw.Format(), val) {
		return nil, ErrNoRootValAtHash
	}

	return NewRootValue(ctx, vrw, ns, val)
}

// isRootValue returns whether the value is a RootValue. This is a variable as it's changed in Doltgres.
func isRootValue(nbf *types.NomsBinFormat, val types.Value) bool {
	if nbf.UsesFlatbuffers() {
		if sm, ok := val.(types.SerialMessage); ok {
			fileID := serial.GetFileID(sm)
			return fileID == serial.RootValueFileID || fileID == serial.DoltgresRootValueFileID
		}
	} else {
		if st, ok := val.(types.Struct); ok {
			return st.Name() == ddbRootStructName
		}
	}
	return false
}

func (root *rootValue) ResolveRootValue(ctx context.Context) (RootValue, error) {
	return root, nil
}

func (root *rootValue) VRW() types.ValueReadWriter {
	return root.vrw
}

func (root *rootValue) NodeStore() tree.NodeStore {
	return root.ns
}

// GetFeatureVersion returns the feature version of this root, if one is written
func (root *rootValue) GetFeatureVersion(ctx context.Context) (ver FeatureVersion, ok bool, err error) {
	return root.st.GetFeatureVersion()
}

func (root *rootValue) SetFeatureVersion(v FeatureVersion) (RootValue, error) {
	newStorage, err := root.st.SetFeatureVersion(v)
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

func (root *rootValue) GetCollation(ctx context.Context) (schema.Collation, error) {
	return root.st.GetCollation(ctx)
}

func (root *rootValue) SetCollation(ctx context.Context, collation schema.Collation) (RootValue, error) {
	newStorage, err := root.st.SetCollation(ctx, collation)
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

func (root *rootValue) HandlePostMerge(ctx context.Context, ourRoot, theirRoot, ancRoot RootValue) (RootValue, error) {
	return root, nil
}

func (root *rootValue) HasTable(ctx context.Context, tName TableName) (bool, error) {
	tableMap, err := root.st.GetTablesMap(ctx, root.vrw, root.ns, tName.Schema)
	if err != nil {
		return false, err
	}
	a, err := tableMap.Get(ctx, tName.Name)
	if err != nil {
		return false, err
	}
	return !a.IsEmpty(), nil
}

func GenerateTagsForNewColColl(ctx context.Context, root RootValue, tableName string, cc *schema.ColCollection) (*schema.ColCollection, error) {
	newColNames := make([]string, 0, cc.Size())
	newColKinds := make([]types.NomsKind, 0, cc.Size())
	_ = cc.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		newColNames = append(newColNames, col.Name)
		newColKinds = append(newColKinds, col.Kind)
		return false, nil
	})

	newTags, err := GenerateTagsForNewColumns(ctx, root, tableName, newColNames, newColKinds, nil)
	if err != nil {
		return nil, err
	}

	idx := 0
	return schema.MapColCollection(cc, func(col schema.Column) schema.Column {
		col.Tag = newTags[idx]
		idx++
		return col
	}), nil
}

// GenerateTagsForNewColumns deterministically generates a slice of new tags that are unique within the history of this root. The names and NomsKinds of
// the new columns are used to see the tag generator.
func GenerateTagsForNewColumns(
	ctx context.Context,
	root RootValue,
	tableName string,
	newColNames []string,
	newColKinds []types.NomsKind,
	headRoot RootValue,
) ([]uint64, error) {
	if len(newColNames) != len(newColKinds) {
		return nil, fmt.Errorf("error generating tags, newColNames and newColKinds must be of equal length")
	}

	newTags := make([]*uint64, len(newColNames))

	// Get existing columns from the current root, or the head root if the table doesn't exist in the current root. The
	// latter case is to support reusing table tags in the case of drop / create in the same session, which is common
	// during import.
	existingCols, err := GetExistingColumns(ctx, root, headRoot, tableName, newColNames, newColKinds)
	if err != nil {
		return nil, err
	}

	// If we found any existing columns set them in the newTags list.
	for _, col := range existingCols {
		col := col
		for i := range newColNames {
			// Only re-use tags if the noms kind didn't change
			// TODO: revisit this when new storage format is further along
			if strings.ToLower(newColNames[i]) == strings.ToLower(col.Name) &&
				newColKinds[i] == col.TypeInfo.NomsKind() {
				newTags[i] = &col.Tag
				break
			}
		}
	}

	var existingColKinds []types.NomsKind
	for _, col := range existingCols {
		existingColKinds = append(existingColKinds, col.Kind)
	}

	existingTags, err := GetAllTagsForRoots(ctx, headRoot, root)
	if err != nil {
		return nil, err
	}

	outputTags := make([]uint64, len(newTags))
	for i := range newTags {
		if newTags[i] != nil {
			outputTags[i] = *newTags[i]
			continue
		}

		outputTags[i] = schema.AutoGenerateTag(existingTags, tableName, existingColKinds, newColNames[i], newColKinds[i])
		existingColKinds = append(existingColKinds, newColKinds[i])
		existingTags.Add(outputTags[i], tableName)
	}

	return outputTags, nil
}

func GetExistingColumns(
	ctx context.Context,
	root, headRoot RootValue,
	tableName string,
	newColNames []string,
	newColKinds []types.NomsKind,
) ([]schema.Column, error) {

	var existingCols []schema.Column
	tbl, found, err := root.GetTable(ctx, TableName{Name: tableName})
	if err != nil {
		return nil, err
	}

	if found {
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			existingCols = append(existingCols, col)
			return false, nil
		})
	} else if headRoot != nil {
		tbl, found, err := headRoot.GetTable(ctx, TableName{Name: tableName})
		if err != nil {
			return nil, err
		}

		if found {
			sch, err := tbl.GetSchema(ctx)
			if err != nil {
				return nil, err
			}

			existingCols = schema.GetSharedCols(sch, newColNames, newColKinds)
		}
	}

	return existingCols, nil
}

func GetAllSchemas(ctx context.Context, root RootValue) (map[string]schema.Schema, error) {
	m := make(map[string]schema.Schema)
	err := root.IterTables(ctx, func(name TableName, table *Table, sch schema.Schema) (stop bool, err error) {
		// TODO: schema name
		m[name.Name] = sch
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return m, nil
}

func (root *rootValue) TableListHash() uint64 {
	return root.tablesHash
}

func (root *rootValue) GetTableHash(ctx context.Context, tName TableName) (hash.Hash, bool, error) {
	tableMap, err := root.getTableMap(ctx, tName.Schema)
	if err != nil {
		return hash.Hash{}, false, err
	}

	tVal, err := tableMap.Get(ctx, tName.Name)
	if err != nil {
		return hash.Hash{}, false, err
	}

	return tVal, !tVal.IsEmpty(), nil
}

func (root *rootValue) SetTableHash(ctx context.Context, tName string, h hash.Hash) (RootValue, error) {
	val, err := root.vrw.ReadValue(ctx, h)

	if err != nil {
		return nil, err
	}

	ref, err := types.NewRef(val, root.vrw.Format())

	if err != nil {
		return nil, err
	}

	// TODO: schema
	return root.putTable(ctx, TableName{Name: tName}, ref)
}

// ResolveTableName resolves a case-insensitive name to the exact name as stored in Dolt. Returns false if no matching
// name was found.
func (root *rootValue) ResolveTableName(ctx context.Context, tName TableName) (string, bool, error) {
	tableMap, err := root.getTableMap(ctx, tName.Schema)
	if err != nil {
		return "", false, err
	}

	a, err := tableMap.Get(ctx, tName.Name)
	if err != nil {
		return "", false, err
	}
	if !a.IsEmpty() {
		return tName.Name, true, nil
	}

	found := false
	lwrName := strings.ToLower(tName.Name)
	resolvedName := tName.Name
	err = tmIterAll(ctx, tableMap, func(name string, addr hash.Hash) {
		if found == false && lwrName == strings.ToLower(name) {
			resolvedName = name
			found = true
		}
	})
	if err != nil {
		return "", false, nil
	}
	return resolvedName, found, nil
}

// GetTable will retrieve a table by its case-sensitive name.
func (root *rootValue) GetTable(ctx context.Context, tName TableName) (*Table, bool, error) {
	tableMap, err := root.getTableMap(ctx, tName.Schema)
	if err != nil {
		return nil, false, err
	}

	addr, err := tableMap.Get(ctx, tName.Name)
	if err != nil {
		return nil, false, err
	}

	return GetTable(ctx, root, addr)
}

func GetTable(ctx context.Context, root RootValue, addr hash.Hash) (*Table, bool, error) {
	if addr.IsEmpty() {
		return nil, false, nil
	}
	table, err := durable.TableFromAddr(ctx, root.VRW(), root.NodeStore(), addr)
	if err != nil {
		return nil, false, err
	}
	return &Table{table: table}, true, err
}

// GetTableInsensitive will retrieve a table by its case-insensitive name.
func GetTableInsensitive(ctx context.Context, root RootValue, tName TableName) (*Table, string, bool, error) {
	resolvedName, ok, err := root.ResolveTableName(ctx, tName)
	if err != nil {
		return nil, "", false, err
	}
	if !ok {
		return nil, "", false, nil
	}
	tbl, ok, err := root.GetTable(ctx, TableName{Name: resolvedName, Schema: tName.Schema})
	if err != nil {
		return nil, "", false, err
	}
	return tbl, resolvedName, ok, nil
}

// GetTableByColTag looks for the table containing the given column tag.
func GetTableByColTag(ctx context.Context, root RootValue, tag uint64) (tbl *Table, name TableName, found bool, err error) {
	err = root.IterTables(ctx, func(tn TableName, t *Table, s schema.Schema) (bool, error) {
		_, found = s.GetAllCols().GetByTag(tag)
		if found {
			name, tbl = tn, t
		}

		return found, nil
	})

	if err != nil {
		return nil, TableName{}, false, err
	}

	return tbl, name, found, nil
}

// GetTableNames retrieves the lists of all tables for a RootValue
func (root *rootValue) GetTableNames(ctx context.Context, schemaName string) ([]string, error) {
	tableMap, err := root.getTableMap(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	md5 := xxhash.New()

	var names []string
	err = tmIterAll(ctx, tableMap, func(name string, _ hash.Hash) {
		md5.Write([]byte(name))
		// avoid distinct table names converging to the same hash
		md5.Write([]byte{0x0000})
		names = append(names, name)
	})
	if err != nil {
		return nil, err
	}

	root.tablesHash = md5.Sum64()

	return names, nil
}

func (root *rootValue) getTableMap(ctx context.Context, schemaName string) (tableMap, error) {
	if schemaName == "" {
		schemaName = DefaultSchemaName
	}
	return root.st.GetTablesMap(ctx, root.vrw, root.ns, schemaName)
}

func TablesWithDataConflicts(ctx context.Context, root RootValue) ([]string, error) {
	names, err := root.GetTableNames(ctx, DefaultSchemaName)
	if err != nil {
		return nil, err
	}

	conflicted := make([]string, 0, len(names))
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, TableName{Name: name})
		if err != nil {
			return nil, err
		}

		ok, err := tbl.HasConflicts(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			conflicted = append(conflicted, name)
		}
	}

	return conflicted, nil
}

// TablesWithConstraintViolations returns all tables that have constraint violations.
func TablesWithConstraintViolations(ctx context.Context, root RootValue) ([]string, error) {
	// TODO: schema name
	names, err := root.GetTableNames(ctx, DefaultSchemaName)
	if err != nil {
		return nil, err
	}

	violating := make([]string, 0, len(names))
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, TableName{Name: name})
		if err != nil {
			return nil, err
		}

		n, err := tbl.NumConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}

		if n > 0 {
			violating = append(violating, name)
		}
	}

	return violating, nil
}

func HasConflicts(ctx context.Context, root RootValue) (bool, error) {
	cnfTbls, err := TablesWithDataConflicts(ctx, root)

	if err != nil {
		return false, err
	}

	return len(cnfTbls) > 0, nil
}

// HasConstraintViolations returns whether any tables have constraint violations.
func HasConstraintViolations(ctx context.Context, root RootValue) (bool, error) {
	tbls, err := TablesWithConstraintViolations(ctx, root)
	if err != nil {
		return false, err
	}
	return len(tbls) > 0, nil
}

// IterTables calls the callback function cb on each table in this RootValue.
func (root *rootValue) IterTables(ctx context.Context, cb func(name TableName, table *Table, sch schema.Schema) (stop bool, err error)) error {
	schemaNames, err := schemaNames(ctx, root)
	if err != nil {
		return err
	}

	for _, schemaName := range schemaNames {
		tm, err := root.getTableMap(ctx, schemaName)
		if err != nil {
			return err
		}

		err = tm.Iter(ctx, func(name string, addr hash.Hash) (bool, error) {
			nt, err := durable.TableFromAddr(ctx, root.VRW(), root.ns, addr)
			if err != nil {
				return true, err
			}
			tbl := &Table{table: nt}

			sch, err := tbl.GetSchema(ctx)
			if err != nil {
				return true, err
			}

			return cb(TableName{Name: name, Schema: schemaName}, tbl, sch)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (root *rootValue) withStorage(st rootValueStorage) *rootValue {
	return &rootValue{root.vrw, root.ns, st, nil, hash.Hash{}, 0}
}

func (root *rootValue) NomsValue() types.Value {
	return root.st.nomsValue()
}

// TableName identifies a table in a database uniquely.
type TableName struct {
	// Name is the name of the table
	Name string
	// Schema is the name of the schema that the table belongs to, empty in the case of the default schema.
	Schema string
}

func (tn TableName) ToLower() TableName {
	return TableName{
		Name:   strings.ToLower(tn.Name),
		Schema: strings.ToLower(tn.Schema),
	}
}

func (tn TableName) Less(o TableName) bool {
	if tn.Schema < o.Schema {
		return true
	}
	return tn.Name < o.Name
}

func (tn TableName) String() string {
	if tn.Schema == "" {
		return tn.Name
	}
	return tn.Schema + "." + tn.Name
}

// ToTableNames is a migration helper function that converts a slice of table names to a slice of TableName structs.
func ToTableNames(names []string, schemaName string) []TableName {
	tbls := make([]TableName, len(names))
	for i, name := range names {
		tbls[i] = TableName{Name: name, Schema: schemaName}
	}
	return tbls
}

// FlattenTableNames is a migration helper function that converts a slice of table names to a slice of strings by
// stripping off any schema elements.
func FlattenTableNames(names []TableName) []string {
	tbls := make([]string, len(names))
	for i, name := range names {
		tbls[i] = name.Name
	}
	return tbls
}

// DefaultSchemaName is the name of the default schema. Tables with this schema name will be stored in the
// primary (unnamed) table store in a root.
var DefaultSchemaName = ""

// PutTable inserts a table by name into the map of tables. If a table already exists with that name it will be replaced
func (root *rootValue) PutTable(ctx context.Context, tName TableName, table *Table) (RootValue, error) {
	err := ValidateTagUniqueness(ctx, root, tName.Name, table)
	if err != nil {
		return nil, err
	}

	tableRef, err := RefFromNomsTable(ctx, table)
	if err != nil {
		return nil, err
	}

	return root.putTable(ctx, tName, tableRef)
}

func RefFromNomsTable(ctx context.Context, table *Table) (types.Ref, error) {
	return durable.RefFromNomsTable(ctx, table.table)
}

func (root *rootValue) putTable(ctx context.Context, tName TableName, ref types.Ref) (RootValue, error) {
	if !IsValidTableName(tName.Name) {
		panic("Don't attempt to put a table with a name that fails the IsValidTableName check")
	}

	newStorage, err := root.st.EditTablesMap(ctx, root.VRW(), root.NodeStore(), []tableEdit{{name: tName, ref: &ref}})
	if err != nil {
		return nil, err
	}

	return root.withStorage(newStorage), nil
}

// CreateEmptyTable creates an empty table in this root with the name and schema given, returning the new root value.
func CreateEmptyTable(ctx context.Context, root RootValue, tName TableName, sch schema.Schema) (RootValue, error) {
	ns := root.NodeStore()
	vrw := root.VRW()
	empty, err := durable.NewEmptyIndex(ctx, vrw, ns, sch)
	if err != nil {
		return nil, err
	}

	indexes, err := durable.NewIndexSet(ctx, vrw, ns)
	if err != nil {
		return nil, err
	}
	err = sch.Indexes().Iter(func(index schema.Index) (stop bool, err error) {
		// create an empty map for every index
		indexes, err = indexes.PutIndex(ctx, index.Name(), empty)
		return
	})
	if err != nil {
		return nil, err
	}

	tbl, err := NewTable(ctx, vrw, ns, sch, empty, indexes, nil)
	if err != nil {
		return nil, err
	}

	newRoot, err := root.PutTable(ctx, tName, tbl)
	if err != nil {
		return nil, err
	}

	return newRoot, nil
}

func (root *rootValue) GetDatabaseSchemas(ctx context.Context) ([]schema.DatabaseSchema, error) {
	existingSchemas, err := root.st.GetSchemas(ctx)
	if err != nil {
		return nil, err
	}

	return existingSchemas, nil
}

func (root *rootValue) CreateDatabaseSchema(ctx context.Context, dbSchema schema.DatabaseSchema) (RootValue, error) {
	existingSchemas, err := root.st.GetSchemas(ctx)
	if err != nil {
		return nil, err
	}

	for _, s := range existingSchemas {
		if strings.EqualFold(s.Name, dbSchema.Name) {
			return nil, fmt.Errorf("A schema with the name %s already exists", dbSchema.Name)
		}
	}

	existingSchemas = append(existingSchemas, dbSchema)
	sort.Slice(existingSchemas, func(i, j int) bool {
		return existingSchemas[i].Name < existingSchemas[j].Name
	})

	r, err := root.st.SetSchemas(ctx, existingSchemas)
	if err != nil {
		return nil, err
	}

	return root.withStorage(r), nil
}

// HashOf gets the hash of the root value
func (root *rootValue) HashOf() (hash.Hash, error) {
	if root.rootHash.IsEmpty() {
		var err error
		root.rootHash, err = root.st.nomsValue().Hash(root.vrw.Format())
		if err != nil {
			return hash.Hash{}, nil
		}
	}
	return root.rootHash, nil
}

// RenameTable renames a table by changing its string key in the RootValue's table map. In order to preserve
// column tag information, use this method instead of a table drop + add.
func (root *rootValue) RenameTable(ctx context.Context, oldName, newName TableName) (RootValue, error) {
	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, root.ns, []tableEdit{{old_name: oldName, name: newName}})
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

func (root *rootValue) RemoveTables(ctx context.Context, skipFKHandling bool, allowDroppingFKReferenced bool, tables ...TableName) (RootValue, error) {
	if len(tables) == 0 {
		return root, nil
	}

	// TODO: support multiple schemas in same operation, or make an error
	tableMap, err := root.getTableMap(ctx, tables[0].Schema)
	if err != nil {
		return nil, err
	}

	edits := make([]tableEdit, len(tables))
	for i, name := range tables {
		a, err := tableMap.Get(ctx, name.Name)
		if err != nil {
			return nil, err
		}
		if a.IsEmpty() {
			return nil, fmt.Errorf("%w: '%s'", ErrTableNotFound, name)
		}
		edits[i].name = name
	}

	newStorage, err := root.st.EditTablesMap(ctx, root.vrw, root.ns, edits)
	if err != nil {
		return nil, err
	}

	newRoot := root.withStorage(newStorage)
	if skipFKHandling {
		return newRoot, nil
	}

	fkc, err := newRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	if allowDroppingFKReferenced {
		err = fkc.RemoveAndUnresolveTables(ctx, root, tables...)
	} else {
		err = fkc.RemoveTables(ctx, tables...)
	}

	if err != nil {
		return nil, err
	}

	return newRoot.PutForeignKeyCollection(ctx, fkc)
}

// GetForeignKeyCollection returns the ForeignKeyCollection for this root. As collections are meant to be modified
// in-place, each returned collection may freely be altered without affecting future returned collections from this root.
func (root *rootValue) GetForeignKeyCollection(ctx context.Context) (*ForeignKeyCollection, error) {
	if root.fkc == nil {
		fkMap, ok, err := root.st.GetForeignKeys(ctx, root.vrw)
		if err != nil {
			return nil, err
		}
		if !ok {
			fkc := &ForeignKeyCollection{
				foreignKeys: map[string]ForeignKey{},
			}
			return fkc, nil
		}

		root.fkc, err = DeserializeForeignKeys(ctx, root.vrw.Format(), fkMap)
		if err != nil {
			return nil, err
		}
	}
	return root.fkc.Copy(), nil
}

// PutForeignKeyCollection returns a new root with the given foreign key collection.
func (root *rootValue) PutForeignKeyCollection(ctx context.Context, fkc *ForeignKeyCollection) (RootValue, error) {
	value, err := SerializeForeignKeys(ctx, root.vrw, fkc)
	if err != nil {
		return nil, err
	}
	newStorage, err := root.st.SetForeignKeyMap(ctx, root.vrw, value)
	if err != nil {
		return nil, err
	}
	return root.withStorage(newStorage), nil
}

// ValidateForeignKeysOnSchemas ensures that all foreign keys' tables are present, removing any foreign keys where the declared
// table is missing, and returning an error if a key is in an invalid state or a referenced table is missing. Does not
// check any tables' row data.
func ValidateForeignKeysOnSchemas(ctx context.Context, root RootValue) (RootValue, error) {
	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: schema name
	allTablesSlice, err := root.GetTableNames(ctx, DefaultSchemaName)
	if err != nil {
		return nil, err
	}
	allTablesSet := make(map[string]schema.Schema)
	for _, tableName := range allTablesSlice {
		tbl, ok, err := root.GetTable(ctx, TableName{Name: tableName})
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("found table `%s` in staging but could not load for foreign key check", tableName)
		}
		tblSch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		allTablesSet[tableName] = tblSch
	}

	// some of these checks are sanity checks and should never happen
	allForeignKeys := fkCollection.AllKeys()
	for _, foreignKey := range allForeignKeys {
		tblSch, existsInRoot := allTablesSet[foreignKey.TableName]
		if existsInRoot {
			if err := foreignKey.ValidateTableSchema(tblSch); err != nil {
				return nil, err
			}
			parentSch, existsInRoot := allTablesSet[foreignKey.ReferencedTableName]
			if !existsInRoot {
				return nil, fmt.Errorf("foreign key `%s` requires the referenced table `%s`", foreignKey.Name, foreignKey.ReferencedTableName)
			}
			if err := foreignKey.ValidateReferencedTableSchema(parentSch); err != nil {
				return nil, err
			}
		} else {
			if !fkCollection.RemoveKeyByName(foreignKey.Name) {
				return nil, fmt.Errorf("`%s` does not exist as a foreign key", foreignKey.Name)
			}
		}
	}

	return root.PutForeignKeyCollection(ctx, fkCollection)
}

// GetAllTagsForRoots gets all tags for |roots|.
func GetAllTagsForRoots(ctx context.Context, roots ...RootValue) (tags schema.TagMapping, err error) {
	tags = make(schema.TagMapping)
	for _, root := range roots {
		if root == nil {
			continue
		}
		err = root.IterTables(ctx, func(tblName TableName, _ *Table, sch schema.Schema) (stop bool, err error) {
			for _, t := range sch.GetAllCols().Tags {
				// TODO: schema names
				tags.Add(t, tblName.Name)
			}
			return
		})
		if err != nil {
			break
		}
	}
	return
}

// UnionTableNames returns an array of all table names in all roots passed as params.
// The table names are in order of the RootValues passed in.
func UnionTableNames(ctx context.Context, roots ...RootValue) ([]TableName, error) {
	seenTblNamesMap := make(map[TableName]bool)
	var tblNames []TableName

	for _, root := range roots {
		schemaNames, err := schemaNames(ctx, root)
		if err != nil {
			return nil, err
		}

		for _, schemaName := range schemaNames {
			rootTblNames, err := root.GetTableNames(ctx, schemaName)
			if err != nil {
				return nil, err
			}
			for _, tn := range rootTblNames {
				tn := TableName{Name: tn, Schema: schemaName}
				if _, ok := seenTblNamesMap[tn]; !ok {
					seenTblNamesMap[tn] = true
					tblNames = append(tblNames, tn)
				}
			}
		}
	}

	return tblNames, nil
}

// schemaNames returns all names of all schemas which may have tables
func schemaNames(ctx context.Context, root RootValue) ([]string, error) {
	dbSchemas, err := root.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, err
	}

	schemaNames := make([]string, len(dbSchemas)+1)
	for i, dbSchema := range dbSchemas {
		schemaNames[i] = dbSchema.Name
	}
	schemaNames[len(dbSchemas)] = DefaultSchemaName
	return schemaNames, nil
}

// FilterIgnoredTables takes a slice of table names and divides it into new slices based on whether the table is ignored, not ignored, or matches conflicting ignore patterns.
func FilterIgnoredTables(ctx context.Context, tables []TableName, roots Roots) (ignoredTables IgnoredTables, err error) {
	ignorePatterns, err := GetIgnoredTablePatterns(ctx, roots)
	if err != nil {
		return ignoredTables, err
	}
	for _, tableName := range tables {
		ignored, err := ignorePatterns.IsTableNameIgnored(tableName)
		if conflict := AsDoltIgnoreInConflict(err); conflict != nil {
			ignoredTables.Conflicts = append(ignoredTables.Conflicts, *conflict)
		} else if err != nil {
			return ignoredTables, err
		} else if ignored == DontIgnore {
			ignoredTables.DontIgnore = append(ignoredTables.DontIgnore, tableName)
		} else if ignored == Ignore {
			ignoredTables.Ignore = append(ignoredTables.Ignore, tableName)
		} else {
			panic("IsTableNameIgnored returned ErrorOccurred but no error!")
		}
	}

	return ignoredTables, nil
}

// ValidateTagUniqueness checks for tag collisions between the given table and the set of tables in then given root.
func ValidateTagUniqueness(ctx context.Context, root RootValue, tableName string, table *Table) error {
	prev, ok, err := root.GetTable(ctx, TableName{Name: tableName})
	if err != nil {
		return err
	}
	if ok {
		prevHash, err := prev.GetSchemaHash(ctx)
		if err != nil {
			return err
		}

		newHash, err := table.GetSchemaHash(ctx)
		if err != nil {
			return err
		}

		// short-circuit if schema unchanged
		if prevHash == newHash {
			return nil
		}
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	existing, err := GetAllTagsForRoots(ctx, root)
	if err != nil {
		return err
	}

	err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		oldTableName, ok := existing.Get(tag)
		if ok && oldTableName != tableName {
			return true, schema.ErrTagPrevUsed(tag, col.Name, tableName, oldTableName)
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	return nil
}

// DebugString returns a human readable string with the contents of this root. If |transitive| is true, row data from
// all tables is also included. This method is very expensive for large root values, so |transitive| should only be used
// when debugging tests.
func (root *rootValue) DebugString(ctx context.Context, transitive bool) string {
	var buf bytes.Buffer
	buf.WriteString(root.st.DebugString(ctx))

	if transitive {
		buf.WriteString("\nTables:")
		root.IterTables(ctx, func(name TableName, table *Table, sch schema.Schema) (stop bool, err error) {
			buf.WriteString("\nTable ")
			buf.WriteString(name.Name)
			buf.WriteString(":\n")

			buf.WriteString(table.DebugString(ctx, root.ns))

			return false, nil
		})
	}

	return buf.String()
}

// MapTableHashes returns a map of each table name and hash.
func MapTableHashes(ctx context.Context, root RootValue) (map[string]hash.Hash, error) {
	// TODO: schema name
	names, err := root.GetTableNames(ctx, DefaultSchemaName)
	if err != nil {
		return nil, err
	}
	nameToHash := make(map[string]hash.Hash)
	for _, name := range names {
		h, ok, err := root.GetTableHash(ctx, TableName{Name: name})
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, fmt.Errorf("root found a table with name '%s' but no hash", name)
		} else {
			nameToHash[name] = h
		}
	}
	return nameToHash, nil
}

type DataCacheKey struct {
	hash.Hash
}

func NewDataCacheKey(rv RootValue) (DataCacheKey, error) {
	hash, err := rv.HashOf()
	if err != nil {
		return DataCacheKey{}, err
	}

	return DataCacheKey{hash}, nil
}

// ResolveDatabaseSchema returns the case-sensitive name for the schema requested, if it exists, and an error if
// schemas could not be loaded.
func ResolveDatabaseSchema(ctx *sql.Context, root RootValue, schemaName string) (string, bool, error) {
	schemas, err := root.GetDatabaseSchemas(ctx)
	if err != nil {
		return "", false, err
	}

	for _, databaseSchema := range schemas {
		if strings.EqualFold(databaseSchema.Name, schemaName) {
			return databaseSchema.Name, true, nil
		}
	}

	return "", false, nil
}
