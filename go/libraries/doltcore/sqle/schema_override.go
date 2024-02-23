// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveOverriddenNonexistentTable check if there is an overridden schema commit set for this session, and if so
// returns an empty table with that schema if |tblName| exists in the overridden schema commit. If no schema override
// is set, this function returns a nil sql.Table and a false boolean return parameter.
func resolveOverriddenNonexistentTable(ctx *sql.Context, tblName string, db Database) (sql.Table, bool, error) {
	// Check to see if table schemas have been overridden
	schemaRoot, err := resolveOverriddenSchemaRoot(ctx, db)
	if err != nil {
		return nil, false, err
	}
	if schemaRoot == nil {
		return nil, false, nil
	}

	// If schema overrides are in place, see if the table exists in the overridden schema
	t, _, ok, err := schemaRoot.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	// Load the overridden schema and convert it to a sql.Schema
	overriddenSchema, err := t.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}
	overriddenSqlSchema, err := sqlutil.FromDoltSchema(db.Name(), tblName, overriddenSchema)
	if err != nil {
		return nil, false, err
	}

	// Return an empty table with the overridden schema
	emptyTable := plan.NewEmptyTableWithSchema(overriddenSqlSchema.Schema)
	return emptyTable.(sql.Table), true, nil
}

// overrideSchemaForTable loads the schema from |overriddenSchemaRoot| for the table named |tableName| and sets the
// override on |tbl|. If there are any problems loading the overridden schema, this function returns an error.
func overrideSchemaForTable(ctx *sql.Context, tableName string, tbl *doltdb.Table, overriddenSchemaRoot *doltdb.RootValue) error {
	differentTable, _, ok, err := overriddenSchemaRoot.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return fmt.Errorf("unable to find table at overridden schema root: %s", err.Error())
	}
	if !ok {
		return fmt.Errorf("unable to find table at overridden schema root")
	}
	overriddenSchema, err := differentTable.GetSchema(ctx)
	if err != nil {
		return fmt.Errorf("unable to find table at overridden schema root: %s", err.Error())
	}

	tbl.OverrideSchema(overriddenSchema)
	return nil
}

// getOverriddenSchemaValue returns a pointer to the string value of the Dolt schema override session variable. If the
// variable is not set (i.e. NULL or empty string) then this function returns nil so that callers can simply check for
// nil.
func getOverriddenSchemaValue(ctx *sql.Context) (*string, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	varValue, err := doltSession.GetSessionVariable(ctx, dsess.DoltOverrideSchema)
	if err != nil {
		return nil, err
	}

	if varValue == nil {
		return nil, nil
	}

	varString, ok := varValue.(string)
	if !ok {
		return nil, fmt.Errorf("value of %s is not a string", dsess.DoltOverrideSchema)
	}

	if varString == "" {
		return nil, nil
	}

	return &varString, nil
}

// resolveOverriddenSchemaRoot loads the Dolt schema override session variable, resolves that commit reference, and
// loads the RootValue for that commit. If the session variable is not set, this function returns nil. If there are
// any problems resolving the commit or loading the root value, this function returns an error.
func resolveOverriddenSchemaRoot(ctx *sql.Context, db Database) (*doltdb.RootValue, error) {
	overriddenSchemaValue, err := getOverriddenSchemaValue(ctx)
	if err != nil {
		return nil, err
	}

	if overriddenSchemaValue == nil {
		return nil, nil
	}

	commitSpec, err := doltdb.NewCommitSpec(*overriddenSchemaValue)
	if err != nil {
		return nil, fmt.Errorf("invalid commit spec specified in %s: %s", dsess.DoltOverrideSchema, err.Error())
	}

	doltSession := dsess.DSessFromSess(ctx.Session)
	headRef, err := doltSession.CWBHeadRef(ctx, db.Name())
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve current working branch head: " + err.Error())
	}

	commit, err := db.GetDoltDB().Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve schema override value: " + err.Error())
	}

	rootValue, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load root value for schema override commit: " + err.Error())
	}

	return rootValue, nil
}
