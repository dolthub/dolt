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
	"fmt"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// ParseCreateTableStatement will parse a CREATE TABLE ddl statement and use it to create a Dolt Schema. A RootValue
// is used to generate unique tags for the Schema
func ParseCreateTableStatement(ctx *sql.Context, root doltdb.RootValue, engine *sqle.Engine, query string) (string, schema.Schema, error) {
	binder := planbuilder.New(ctx, engine.Analyzer.Catalog, engine.EventScheduler, engine.Parser)
	parsed, _, _, _, err := binder.Parse(query, nil, false)
	if err != nil {
		return "", nil, err
	}
	create, ok := parsed.(*plan.CreateTable)
	if !ok {
		return "", nil, fmt.Errorf("expected create table, found %T", create)
	}

	// NOTE: We don't support setting a schema name here since this code is only intended to be used from the Dolt
	//       codebase, and will not work correctly from Doltgres.
	sch, err := ToDoltSchema(ctx, root, doltdb.TableName{Name: create.Name()}, create.PkSchema(), nil, create.Collation)
	if err != nil {
		return "", nil, err
	}

	for _, idx := range create.Indexes() {
		var prefixes []uint16
		for _, c := range idx.Columns {
			prefixes = append(prefixes, uint16(c.Length))
		}
		props := schema.IndexProperties{
			IsUnique:   idx.IsUnique(),
			IsSpatial:  idx.IsSpatial(),
			IsFullText: idx.IsFullText(),
			Comment:    idx.Comment,
		}
		name := getIndexName(idx)
		_, err = sch.Indexes().AddIndexByColNames(name, idx.ColumnNames(), prefixes, props)
		if err != nil {
			return "", nil, err
		}
	}

	// foreign keys are stored on the *doltdb.Table object, ignore them here
	for _, chk := range create.Checks() {
		name := getCheckConstraintName(chk)
		_, err = sch.Checks().AddCheck(name, chk.Expr.String(), chk.Enforced)
		if err != nil {
			return "", nil, err
		}
	}
	return create.Name(), sch, err
}

func getIndexName(def *sql.IndexDef) string {
	if def.Name != "" {
		return def.Name
	}
	return strings.Join(def.ColumnNames(), "_") + "_key"
}

func getCheckConstraintName(chk *sql.CheckConstraint) string {
	if chk.Name != "" {
		return chk.Name
	}
	return chk.DebugString()
}
