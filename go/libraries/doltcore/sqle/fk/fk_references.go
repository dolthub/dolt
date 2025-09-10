// Copyright 2025 Dolthub, Inc.
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

package fk

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

// GetReferencedForeignKeys returns a slice of ForeignKeyConstraints that reference the table |tableName| in the database
// |databaseName|. The RootValue |root| is used to load the referenced tables, and the schema of the referenced table is
// specified in |tableSchema| and used to convert FK metadata into ForeignKeyConstraint instances.
func GetReferencedForeignKeys(ctx *sql.Context, root doltdb.RootValue, databaseName string, tableName doltdb.TableName, tableSchema schema.Schema) ([]sql.ForeignKeyConstraint, error) {
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	_, referencedByFk := fkc.KeysForTable(tableName)
	toReturn := make([]sql.ForeignKeyConstraint, len(referencedByFk))

	for i, fk := range referencedByFk {
		if len(fk.UnresolvedFKDetails.TableColumns) > 0 && len(fk.UnresolvedFKDetails.ReferencedTableColumns) > 0 {
			//TODO: implement multi-db support for foreign keys
			toReturn[i] = sql.ForeignKeyConstraint{
				Name:           fk.Name,
				Database:       databaseName,
				Table:          fk.TableName.Name, // TODO: schema name
				Columns:        fk.UnresolvedFKDetails.TableColumns,
				ParentDatabase: databaseName,
				ParentTable:    fk.ReferencedTableName.Name, // TODO: schema name
				ParentColumns:  fk.UnresolvedFKDetails.ReferencedTableColumns,
				OnUpdate:       sqlutil.ToReferentialAction(fk.OnUpdate),
				OnDelete:       sqlutil.ToReferentialAction(fk.OnDelete),
				IsResolved:     fk.IsResolved(),
			}
			continue
		}
		child, ok, err := root.GetTable(ctx, fk.TableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot find table %s declared by foreign key %s", fk.TableName, fk.Name)
		}
		childSch, err := child.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		toReturn[i], err = sqlutil.ToForeignKeyConstraint(fk, databaseName, childSch, tableSchema)
		if err != nil {
			return nil, err
		}
	}

	return toReturn, nil
}
