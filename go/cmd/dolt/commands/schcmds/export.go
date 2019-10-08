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

package schcmds

import (
	"context"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var schExportShortDesc = "Exports a table's schema."
var schExportLongDesc = ""
var schExportSynopsis = []string{
	"<table> <file>",
}

func Export(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "table whose schema is being exported."
	ap.ArgListHelp["commit"] = "commit at which point the schema will be displayed."
	ap.SupportsString(defaultParam, "", "default-value", "If provided all existing rows will be given this value as their default.")
	ap.SupportsUint(tagParam, "", "tag-number", "The numeric tag for the new column.")
	ap.SupportsFlag(notNullFlag, "", "If provided rows without a value in this column will be considered invalid.  If rows already exist and not-null is specified then a default value must be provided.")

	help, usage := cli.HelpAndUsagePrinters(commandStr, schExportShortDesc, schExportLongDesc, schExportSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr = exportSchemas(ctx, apr, root, dEnv)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func exportSchemas(ctx context.Context, apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("Must specify table and file to which table will be exported.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	fileName := apr.Arg(1)
	root, _ = commands.GetWorkingWithVErr(dEnv)
	if has, err := root.HasTable(ctx, tblName); err != nil {
		return errhand.BuildDError("unable to read from database").AddCause(err).Build()
	} else if !has {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _, err := root.GetTable(ctx, tblName)

	if err != nil {
		return errhand.BuildDError("unable to get table").AddCause(err).Build()
	}

	err = exportTblSchema(ctx, tbl, fileName, dEnv)
	if err != nil {
		return errhand.BuildDError("file path not valid.").Build()
	}

	return nil
}

func exportTblSchema(ctx context.Context, tbl *doltdb.Table, filename string, dEnv *env.DoltEnv) errhand.VerboseError {
	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to get schema").AddCause(err).Build()
	}

	jsonSchStr, err := encoding.MarshalAsJson(sch)
	if err != nil {
		return errhand.BuildDError("Failed to encode as json").AddCause(err).Build()
	}

	err = dEnv.FS.WriteFile(filename, []byte(jsonSchStr))
	return errhand.BuildIf(err, "Unable to write "+filename).AddCause(err).Build()
}
