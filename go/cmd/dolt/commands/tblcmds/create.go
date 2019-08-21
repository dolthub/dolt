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
	"io/ioutil"
	"os"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var tblCreateShortDesc = "Creates or overwrite existing table(s) with an empty table(s)."
var tblCreateLongDesc = `dolt table create will create a new table with a given schema.  Newly created tables are empty.
If the <b>--force | -f</b> parameter is provided create will overwrite existing tables.

` + schemaFileHelp + `

You may also consider using <b>dolt sql -q 'CREATE TABLE ...'</b>:
`

var tblCreateSynopsis = []string{
	"[-f] -s <schema_file> <table>...",
}

func Create(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	//ap.ArgListHelp["field_descriptor"] = fieldDescriptorHelp
	ap.ArgListHelp["table"] = "name of the table being created."
	ap.SupportsFlag(forceParam, "f", "Force table creation if a table with this name already exists by overwriting it. ")
	ap.SupportsString(outSchemaParam, "s", "schema_file", "The schema the new table should be created with.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblCreateShortDesc, tblCreateLongDesc, tblCreateSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 1
	}

	schVal, verr := readSchema(ctx, apr, dEnv)

	if verr == nil {
		var root *doltdb.RootValue
		root, verr = commands.GetWorkingWithVErr(dEnv)

		if verr == nil {
			force := apr.Contains(forceParam)
			m, err := types.NewMap(ctx, root.VRW())

			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("").AddCause(err).Build(), nil)
			}

			tbl, err := doltdb.NewTable(ctx, root.VRW(), schVal, m)

			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("").AddCause(err).Build(), nil)
			}

			for i := 0; i < apr.NArg() && verr == nil; i++ {
				tblName := apr.Arg(i)
				has, err := root.HasTable(ctx, tblName)

				if err != nil {

				} else if !force && has {
					bdr := errhand.BuildDError("table '%s' already exists.", tblName)
					bdr.AddDetails("Use -f to overwrite the table with the specified schema and empty row data.")
					verr = bdr.AddDetails("aborting").Build()
				} else {
					root, err = root.PutTable(ctx, dEnv.DoltDB, tblName, tbl)

					if err != nil {
						verr = errhand.BuildDError("error: failed to write table back to database.").AddCause(err).Build()
					}
				}
			}

			if verr == nil {
				verr = commands.UpdateWorkingWithVErr(dEnv, root)
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func readSchema(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) (types.Value, errhand.VerboseError) {
	if !apr.Contains(outSchemaParam) {
		return nil, errhand.BuildDError("fatal: missing required parameter 'schema'").SetPrintUsage().Build()
	}

	schemaFile := apr.MustGetValue(outSchemaParam)
	rd, err := dEnv.FS.OpenForRead(schemaFile)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, errhand.BuildDError("File %s does not exist.", schemaFile).Build()
		} else {
			return nil, errhand.BuildDError("Failed to read %s", schemaFile).AddCause(err).Build()
		}
	}

	data, err := ioutil.ReadAll(rd)

	if err != nil {
		return nil, errhand.BuildDError("Failed to read %s", schemaFile).AddCause(err).Build()
	}

	sch, err := encoding.UnmarshalJson(string(data))

	if err != nil {
		return nil, errhand.BuildDError("Invalid json schema at %s", schemaFile).AddCause(err).Build()
	} else if sch.GetAllCols().Size() == 0 {
		return nil, errhand.BuildDError("Invalid schema does not have any valid fields.").Build()
	} else if sch.GetPKCols().Size() == 0 {
		return nil, errhand.BuildDError("Invalid schema does not have a primary key.").Build()
	}

	schVal, err := encoding.MarshalAsNomsValue(ctx, dEnv.DoltDB.ValueReadWriter(), sch)

	if err != nil {
		//I dont really understand the cases where this would happen.
		return nil, errhand.BuildDError("fatal: internal schema error").Build()
	}

	return schVal, nil
}
