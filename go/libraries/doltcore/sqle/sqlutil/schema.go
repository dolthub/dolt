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

package sqlutil

import (
	"context"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// ParseCreateTableStatement will parse a CREATE TABLE ddl statement and use it to create a Dolt Schema. A RootValue
// is used to generate unique tags for the Schema
func ParseCreateTableStatement(ctx context.Context, root *doltdb.RootValue, query string) (string, schema.Schema, error) {
	// todo: verify create table statement
	p, err := sqlparser.Parse(query)
	if err != nil {
		return "", nil, err
	}
	ddl := p.(*sqlparser.DDL)

	sctx := sql.NewContext(ctx)
	s, collation, err := parse.TableSpecToSchema(sctx, ddl.TableSpec, false)
	if err != nil {
		return "", nil, err
	}

	for _, col := range s.Schema {
		if collatedType, ok := col.Type.(sql.TypeWithCollation); ok {
			col.Type, err = collatedType.WithNewCollation(sql.Collation_Default)
			if err != nil {
				return "", nil, err
			}
		}
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	ddl.Table.Format(buf)
	tableName := buf.String()
	sch, err := ToDoltSchema(ctx, root, tableName, s, nil, collation)
	if err != nil {
		return "", nil, err
	}

	indexes, err := parse.ConvertIndexDefs(sctx, ddl.TableSpec)
	if err != nil {
		return "", nil, err
	}

	for _, idx := range indexes {
		var prefixes []uint16
		for _, c := range idx.Columns {
			prefixes = append(prefixes, uint16(c.Length))
		}
		props := schema.IndexProperties{
			IsUnique:  idx.IsUnique(),
			IsSpatial: idx.IsSpatial(),
			Comment:   idx.Comment,
		}
		name := getIndexName(idx)
		_, err = sch.Indexes().AddIndexByColNames(name, idx.ColumnNames(), prefixes, props)
		if err != nil {
			return "", nil, err
		}
	}

	// foreign keys are stored on the *doltdb.Table object, ignore them here
	_, checks, err := parse.ConvertConstraintsDefs(sctx, ddl.Table, ddl.TableSpec)
	if err != nil {
		return "", nil, err
	}
	for _, chk := range checks {
		name := getCheckConstraintName(chk)
		_, err = sch.Checks().AddCheck(name, chk.Expr.String(), chk.Enforced)
		if err != nil {
			return "", nil, err
		}
	}
	return tableName, sch, err
}

func getIndexName(def *plan.IndexDefinition) string {
	if def.IndexName != "" {
		return def.IndexName
	}
	return strings.Join(def.ColumnNames(), "_") + "_key"
}

func getCheckConstraintName(chk *sql.CheckConstraint) string {
	if chk.Name != "" {
		return chk.Name
	}
	return chk.DebugString()
}
