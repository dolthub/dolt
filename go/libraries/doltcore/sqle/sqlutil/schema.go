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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// ParseCreateTableStatement will parse a CREATE TABLE ddl statement and use it to create a Dolt Schema. A RootValue
// is used to generate unique tags for the Schema
func ParseCreateTableStatement(ctx context.Context, root *doltdb.RootValue, query string) (string, schema.Schema, error) {
	// todo: verify create table statement
	ddl, err := sqlparser.Parse(query)

	if err != nil {
		return "", nil, err
	}

	ts := ddl.(*sqlparser.DDL).TableSpec
	s, err := parse.TableSpecToSchema(sql.NewContext(ctx), ts)

	if err != nil {
		return "", nil, err
	}

	tn := ddl.(*sqlparser.DDL).Table
	buf := sqlparser.NewTrackedBuffer(nil)
	tn.Format(buf)
	tableName := buf.String()
	sch, err := ToDoltSchema(ctx, root, tableName, s, nil)

	if err != nil {
		return "", nil, err
	}

	return tableName, sch, err
}
