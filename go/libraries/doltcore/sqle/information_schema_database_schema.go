// Copyright 2019-2020 Dolthub, Inc.
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

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
)

// informationSchemaDatabaseSchema is a DatabaseSchema implementation that provides access to the INFORMATION_SCHEMA tables. This is relevant only for Doltgres.
type informationSchemaDatabaseSchema struct {
	name       string
	schemaName string
	tables     map[string]sql.Table
}

var _ sql.DatabaseSchema = (*informationSchemaDatabaseSchema)(nil)

// newInformationSchemaDatabase creates a new INFORMATION_SCHEMA DatabaseSchema for doltgres databases.
func newInformationSchemaDatabase(dbName string) sql.DatabaseSchema {
	isDb := &informationSchemaDatabaseSchema{
		name:       dbName,
		schemaName: sql.InformationSchemaDatabaseName,
		tables:     information_schema.GetInformationSchemaTables(),
	}

	isDb.tables[information_schema.StatisticsTableName] = information_schema.NewDefaultStats()

	return isDb
}

// Name implements the sql.DatabaseSchema interface.
func (db *informationSchemaDatabaseSchema) Name() string { return db.name }

// SchemaName implements the sql.DatabaseSchema interface.
func (db *informationSchemaDatabaseSchema) SchemaName() string { return db.schemaName }

// GetTableInsensitive implements the sql.DatabaseSchema interface.
func (db *informationSchemaDatabaseSchema) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	// The columns table has dynamic information that can't be cached across queries
	if strings.ToLower(tblName) == information_schema.ColumnsTableName {
		return information_schema.NewColumnsTable(), true, nil
	}

	tbl, ok := sql.GetTableInsensitive(tblName, db.tables)
	return tbl, ok, nil
}

// GetTableNames implements the sql.DatabaseSchema interface.
func (db *informationSchemaDatabaseSchema) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames := make([]string, 0, len(db.tables))
	for k := range db.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}
