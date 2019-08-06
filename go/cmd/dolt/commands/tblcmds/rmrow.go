// Copyright 2019 Liquidata, Inc.
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

package tblcmds

import (
	"context"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var rmRowShortDesc = "Removes row(s) from a table"
var rmRowLongDesc = "dolt table rm-row will remove one or more rows from a table in the working set."
var rmRowSynopsis = []string{
	"<table> [<key_definition>] <key>...",
}

type rmRowArgs struct {
	TableName string
	PKs       []string
}

func parseRmRowArgs(commandStr string, args []string) *rmRowArgs {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "The table being edited."
	ap.ArgListHelp["primary_key"] = "Primary key of the row(s) to delete."
	help, usage := cli.HelpAndUsagePrinters(commandStr, rmRowShortDesc, rmRowLongDesc, rmRowSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return nil
	}

	tableName := apr.Arg(0)

	pks := []string{}
	if apr.NArg() > 1 {
		pks = apr.Args()[1:]
	}

	return &rmRowArgs{tableName, pks}
}

func RmRow(commandStr string, args []string, dEnv *env.DoltEnv) int {
	rmArgs := parseRmRowArgs(commandStr, args)

	if rmArgs == nil {
		return 1
	}

	var root *doltdb.RootValue
	var tbl *doltdb.Table
	root, tbl, verr := getRootAndTable(dEnv, rmArgs.TableName)

	if verr == nil {
		sch, err := tbl.GetSchema(context.TODO())

		if err != nil {
			verr = errhand.BuildDError("error: failed to get schema").AddCause(err).Build()
		} else {
			pkVals, err := cli.ParseKeyValues(root.VRW().Format(), sch, rmArgs.PKs)

			if err != nil {
				verr = errhand.BuildDError("error parsing keys to delete").AddCause(err).Build()
			} else {
				verr = updateTableWithRowsRemoved(root, tbl, rmArgs.TableName, pkVals, dEnv)
			}
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func getRootAndTable(dEnv *env.DoltEnv, tblName string) (*doltdb.RootValue, *doltdb.Table, errhand.VerboseError) {
	root, err := dEnv.WorkingRoot(context.Background())

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get working value for the dolt data repository.").Build()
	}

	tbl, ok, err := root.GetTable(context.TODO(), tblName)

	if err != nil {
		return nil, nil, errhand.BuildDError("error: failde to get tables").AddCause(err).Build()
	}

	if !ok {
		return nil, nil, errhand.BuildDError("Unknown table %s", tblName).Build()
	}

	return root, tbl, nil
}

func updateTableWithRowsRemoved(root *doltdb.RootValue, tbl *doltdb.Table, tblName string, pkVals []types.Value, dEnv *env.DoltEnv) errhand.VerboseError {
	m, err := tbl.GetRowData(context.TODO())

	if err != nil {
		return errhand.BuildDError("error: failed to get row data").Build()
	}

	updates := 0
	for _, pk := range pkVals {
		_, ok, err := m.MaybeGet(context.TODO(), pk)

		if err != nil {
			return errhand.BuildDError("error: failed to read from database").Build()
		}

		if !ok {
			str, err := types.EncodedValue(context.TODO(), pk)

			if err != nil {
				panic(err)
			}

			cli.PrintErrln(color.YellowString(`No row with pk equal to %s was found.`, str))
			continue
		}

		me := m.Edit()
		me.Remove(pk)
		m, err = me.Map(context.TODO())

		if err != nil {
			return errhand.BuildDError("error: failed to remove row from table").Build()
		}

		updates++
	}

	tbl, err = tbl.UpdateRows(context.Background(), m)

	if err != nil {
		return errhand.BuildDError("error: failed to update the table").AddCause(err).Build()
	}

	root, err = root.PutTable(context.Background(), dEnv.DoltDB, tblName, tbl)

	if err != nil {
		return errhand.BuildDError("error: failed to update the table").AddCause(err).Build()
	}

	verr := commands.UpdateWorkingWithVErr(dEnv, root)

	if verr == nil {
		cli.Printf("Removed %d rows\n", updates)
	}

	return verr
}
