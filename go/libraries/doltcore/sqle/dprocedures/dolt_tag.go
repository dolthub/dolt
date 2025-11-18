// Copyright 2022 Dolthub, Inc.
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

package dprocedures

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// doltTag is the stored procedure version for the CLI command `dolt tag`.
func doltTag(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltTag(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

// doDoltTag is used as sql dolt_tag command for only creating or deleting tags, not listing.
// To read/select tags, dolt_tags system table is used.
func doDoltTag(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}
	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	apr, err := cli.CreateTagArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	// list tags
	if len(apr.Args) == 0 || apr.Contains(cli.VerboseFlag) {
		return 1, fmt.Errorf("error: invalid argument, use 'dolt_tags' system table to list tags")
	}

	// delete tag
	if apr.Contains(cli.DeleteFlag) {
		if apr.Contains(cli.MessageArg) {
			return 1, fmt.Errorf("delete and tag message options are incompatible")
		}
		err = actions.DeleteTagsOnDB(ctx, dbData.Ddb, apr.Args...)
		if err != nil {
			return 1, err
		}
		return 0, nil
	}

	// create tag
	if len(apr.Args) > 2 {
		return 1, fmt.Errorf("create tag takes at most two args")
	}

	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return 1, err
		}
	} else {
		name = dSess.Username()
		email = dSess.Email()
	}

	msg, _ := apr.GetValue(cli.MessageArg)

	props := actions.TagProps{
		TaggerName:  name,
		TaggerEmail: email,
		Description: msg,
	}

	tagName := apr.Arg(0)
	startPoint := "head"
	if len(apr.Args) > 1 {
		startPoint = apr.Arg(1)
	}
	headRef, err := dbData.Rsr.CWBHeadRef(ctx)
	if err != nil {
		return 0, err
	}
	err = actions.CreateTagOnDB(ctx, dbData.Ddb, tagName, startPoint, props, headRef)
	if err != nil {
		return 1, err
	}

	return 0, nil
}
