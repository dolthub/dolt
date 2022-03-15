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

package sqle

//
//type TempTable struct {
//	tableName string
//	dbName    string
//	pkSch    sql.PrimaryKeySchema
//
//	table *doltdb.Table
//	sch   schema.Schema
//	opts  editor.Options
//}
//
//func (t TempTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
//	return nil, nil
//}
//
//func (t TempTable) GetAutoIncrementValue(ctx *sql.Context) (interface{}, error) {
//	return nil, nil
//}
//
//func (t TempTable) Name() string {
//	return ""
//}
//
//func (t TempTable) String() string {
//	return ""
//}
//
//func (t TempTable) NumRows(ctx *sql.Context) (uint64, error) {
//	return 0, nil
//}
//
//func (t TempTable) Format() *types.NomsBinFormat {
//	return t.table.Format()
//}
//
//func (t TempTable) Schema() sql.Schema {
//	return t.sch
//}
//
//func (t TempTable) sqlSchema() sql.PrimaryKeySchema {
//	return t.pkSch
//}
//
//func (t TempTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
//	return nil, nil
//}
//
//func (t TempTable) IsTemporary() bool {
//	return True
//}
//
//func (t TempTable) DataLength(ctx *sql.Context) (uint64, error) {
//	return nil
//}
//
//func (t TempTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
//	return nil
//}
//
//func (t TempTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
//	return nil
//}
//
//func (t TempTable) WithProjection(colNames []string) sql.Table {
//	return nil
//}
//
//func (t TempTable) Inserter(ctx *sql.Context) sql.RowInserter {
//	return nil
//}
//
//func (t TempTable) getTableEditor(ctx *sql.Context) (ed writer.TableWriter, err error) {
//	return nil
//}
//
//func (t TempTable) Deleter(ctx *sql.Context) sql.RowDeleter {
//	return nil
//}
//
//func (t TempTable) Replacer(ctx *sql.Context) sql.RowReplacer {
//	return nil
//}
//
//func (t TempTable) Truncate(ctx *sql.Context) (int, error) {
//	return nil
//}
//
//func (t TempTable) Updater(ctx *sql.Context) sql.RowUpdater {
//	return nil
//}
//
//func (t TempTable) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
//	return nil
//}
//
//func (t TempTable) PrimaryKeySchema() sql.PrimaryKeySchema {
//	return nil
//}
//
//func (t TempTable) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
//	return nil
//}
//
//func (t TempTable) DropColumn(ctx *sql.Context, columnName string) error {
//	return nil
//}
//
//func (t TempTable) dropColumnData(ctx *sql.Context, updatedTable *doltdb.Table, sch schema.Schema, columnName string) (*doltdb.Table, error) {
//	return nil
//}
//
//func (t TempTable) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
//	return nil
//}
//
//func (t TempTable) DropIndex(ctx *sql.Context, indexName string) error {
//	return nil
//}
//
//func (t TempTable) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
//	return nil
//}
//
//func (t TempTable) dropIndex(ctx *sql.Context, indexName string) (*doltdb.Table, schema.Schema, error) {
//	return nil
//}
//
//func (t TempTable) updateFromRoot(ctx *sql.Context, root *doltdb.RootValue) error {
//	return nil
//}
//
//func (t TempTable) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
//	return nil
//}
//
//func (t TempTable) DropCheck(ctx *sql.Context, chName string) error {
//	return nil
//}
//
//func (t TempTable) generateCheckName(ctx *sql.Context, check *sql.CheckDefinition) (string, error) {
//	return nil
//}
//
//func (t TempTable) constraintNameExists(ctx *sql.Context, name string) (bool, error) {
//	return nil
//}
