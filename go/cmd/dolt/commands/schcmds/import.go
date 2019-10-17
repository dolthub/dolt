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
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	createFlag          = "create"
	updateFlag          = "update"
	replaceFlag         = "replace"
	dryRunFlag          = "dry-run"
	fileTypeParam       = "file-type"
	pksParam            = "pks"
	mappingParam        = "map"
	floatThresholdParam = "float-threshold"
	keepTypesParam      = "keep-types"
)

var schImportShortDesc = "Creates a new table with an inferred schema."
var schImportLongDesc = ""
var schImportSynopsis = []string{
	"[--create|--update|--replace] [--force] [--dry-run] [--lower|--upper] [--keep-types] [--file-type <type>] [--float-threshold] [--map <mapping-file>] --pks <field>,... <table> <file>",
}

type importOp int

const (
	createOp importOp = iota
	updateOp
	replaceOp
)

type importArgs struct {
	op        importOp
	fileType  string
	fileName  string
	inferArgs *actions.InferenceArgs
}

// Import is a schema command that will take a file and infer it's schema, and then create a table matching that schema.
func Import(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "Name of the table to be created."
	ap.ArgListHelp["file"] = "The file being used to infer the schema."
	ap.SupportsFlag(dryRunFlag, "", "")
	ap.SupportsFlag(createFlag, "c", "")
	ap.SupportsFlag(updateFlag, "u", "")
	ap.SupportsFlag(replaceFlag, "r", "")
	ap.SupportsFlag(keepTypesParam, "", "")
	ap.SupportsString(fileTypeParam, "", "type", "")
	ap.SupportsString(pksParam, "", "comma-separated-col-names", "")
	ap.SupportsString(mappingParam, "", "mapping-file", "")
	ap.SupportsString(floatThresholdParam, "", "float", "")

	help, usage := cli.HelpAndUsagePrinters(commandStr, schImportShortDesc, schImportLongDesc, schImportSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 0
	}

	verr := importSchema(ctx, dEnv, apr)

	return commands.HandleVErrAndExitCode(verr, usage)
}

func importSchema(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr != nil {
		return verr
	}

	tblName := apr.Arg(0)
	fileName := apr.Arg(1)

	fileExists, _ := dEnv.FS.Exists(fileName)

	if !fileExists {
		return errhand.BuildDError("error: file '%s' not found.", fileName).Build()
	}

	op := createOp
	if !apr.ContainsAny(createFlag, updateFlag, replaceFlag) {
		return errhand.BuildDError("error: missing required parameter.").AddDetails("Must provide exactly one of the operation flags '--create', '--update', or '--replace'").SetPrintUsage().Build()
	} else if apr.Contains(updateFlag) {
		if apr.ContainsAny(createFlag, replaceFlag) {
			return errhand.BuildDError("error: multiple operations supplied").AddDetails("Only one of the flags '--create', '--update', or '--replace' may be provided").SetPrintUsage().Build()
		}
		op = updateOp
	} else if apr.Contains(replaceFlag) {
		if apr.Contains(createFlag) {
			return errhand.BuildDError("error: multiple operations supplied").AddDetails("Only one of the flags '--create', '--update', or '--replace' may be provided").SetPrintUsage().Build()
		}
		op = replaceOp
	} else {
		if apr.Contains(keepTypesParam) {
			return errhand.BuildDError("error: parameter keep-types not supported for create operations").AddDetails("keep-types parameter is used to keep the existing column types as is without modification.").Build()
		}
	}

	tbl, tblExists, err := root.GetTable(ctx, tblName)

	if err != nil {
		return errhand.BuildDError("error: failed to read from database.").AddCause(err).Build()
	} else if tblExists && op == createOp {
		return errhand.BuildDError("error: failed to create table.").AddDetails("A table named '%s' already exists.", tblName).AddDetails("Use --replace or --update instead of --create.").Build()
	}

	var existingSch schema.Schema = schema.EmptySchema
	if tblExists {
		existingSch, err = tbl.GetSchema(ctx)

		if err != nil {
			return errhand.BuildDError("error: failed to read schema from '%s'", tblName).AddCause(err).Build()
		}
	}

	val, pksOK := apr.GetValue(pksParam)

	var pks []string
	if pksOK {
		pks = strings.Split(val, ",")
		goodPKS := make([]string, 0, len(pks))

		for _, pk := range pks {
			pk = strings.TrimSpace(pk)

			if pk != "" {
				goodPKS = append(goodPKS, pk)
			}
		}

		pks = goodPKS
	} else {
		return errhand.BuildDError("error: missing required parameter pks").SetPrintUsage().Build()
	}

	mappingFile := apr.GetValueOrDefault(mappingParam, "")

	var colMapper actions.StrMapper
	if mappingFile != "" {
		if mappingExists, _ := dEnv.FS.Exists(mappingFile); mappingExists {
			var m map[string]string
			err := filesys.UnmarshalJSONFile(dEnv.FS, mappingFile, &m)

			if err != nil {
				return errhand.BuildDError("error: invalid mapper file.").AddCause(err).Build()
			}

			colMapper = actions.MapMapper(m)
		} else {
			return errhand.BuildDError("error: '%s' does not exist.", mappingFile).Build()
		}
	} else {
		colMapper = actions.IdentityMapper{}
	}

	floatThresholdStr := apr.GetValueOrDefault(floatThresholdParam, "0.0")
	floatThreshold, err := strconv.ParseFloat(floatThresholdStr, 64)

	if err != nil {
		return errhand.BuildDError("error: '%s' is not a valid float in the range 0.0 (all floats) to 1.0 (no floats)", floatThresholdStr).SetPrintUsage().Build()
	}

	impArgs := importArgs{
		op:       op,
		fileName: fileName,
		fileType: apr.GetValueOrDefault(fileTypeParam, filepath.Ext(fileName)),
		inferArgs: &actions.InferenceArgs{
			ExistingSch:    existingSch,
			ColMapper:      colMapper,
			FloatThreshold: floatThreshold,
			KeepTypes:      apr.Contains(keepTypesParam),
		},
	}

	sch, verr := inferSchemaFromFile(ctx, dEnv.DoltDB.ValueReadWriter().Format(), pks, &impArgs)

	if verr != nil {
		return verr
	}

	cli.Println(sql.SchemaAsCreateStmt(tblName, sch))

	if !apr.Contains(dryRunFlag) {
		schVal, err := encoding.MarshalAsNomsValue(context.Background(), root.VRW(), sch)

		if err != nil {
			return errhand.BuildDError("error: failed to encode schema.").AddCause(err).Build()
		}

		m, err := types.NewMap(ctx, root.VRW())

		if err != nil {
			return errhand.BuildDError("error: failed to create table.").AddCause(err).Build()
		}

		tbl, err = doltdb.NewTable(ctx, root.VRW(), schVal, m)

		if err != nil {
			return errhand.BuildDError("error: failed to create table.").AddCause(err).Build()
		}

		root, err = root.PutTable(ctx, tblName, tbl)

		if err != nil {
			return errhand.BuildDError("error: failed to add table.").AddCause(err).Build()
		}

		err = dEnv.UpdateWorkingRoot(ctx, root)

		if err != nil {
			return errhand.BuildDError("error: failed to update the working set.").AddCause(err).Build()
		}

		cli.PrintErrln(color.CyanString("Created table successfully."))
	}

	return nil
}

func inferSchemaFromFile(ctx context.Context, nbf *types.NomsBinFormat, pkCols []string, args *importArgs) (schema.Schema, errhand.VerboseError) {
	if args.fileType[0] == '.' {
		args.fileType = args.fileType[1:]
	}

	var rd table.TableReadCloser
	switch args.fileType {
	case "csv":
		f, err := os.Open(args.fileName)

		if err != nil {
			return nil, errhand.BuildDError("error: failed to open '%s'", args.fileName).Build()
		}

		defer f.Close()

		rd, err = csv.NewCSVReader(nbf, f, csv.NewCSVInfo())

		if err != nil {
			return nil, errhand.BuildDError("error: failed to create a CSVReader.").AddCause(err).Build()
		}

		defer rd.Close(ctx)
	}

	sch, err := actions.InferSchemaFromTableReader(ctx, rd, pkCols, args.inferArgs)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to infer schema").AddCause(err).Build()
	}

	return sch, nil
}
