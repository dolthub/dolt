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
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	defaultParam = "default"
	tagParam     = "tag"
	notNullFlag  = "not-null"
)

var schAddColShortDesc = "Adds a column to specified table's schema"
var schAddColLongDesc = "Adds a column to the specified table's schema. If no default value is provided the column will be empty"
var schAddColSynopsis = []string{
	"[--default <default_value>] [--not-null] [--tag <tag-number>] <table> <name> <type>",
}

type AddColumnCmd struct{}

func (cmd AddColumnCmd) Name() string {
	return "add-column"
}

func (cmd AddColumnCmd) Description() string {
	return "Adds a column to specified table's schema."
}

func (cmd AddColumnCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

func (cmd AddColumnCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "table where the new column should be added."
	ap.SupportsString(defaultParam, "", "default-value", "If provided all existing rows will be given this value as their default.")
	ap.SupportsUint(tagParam, "", "tag-number", "The numeric tag for the new column.")
	ap.SupportsFlag(notNullFlag, "", "If provided rows without a value in this column will be considered invalid.  If rows already exist and not-null is specified then a default value must be provided.")

	help, usage := cli.HelpAndUsagePrinters(commandStr, schAddColShortDesc, schAddColLongDesc, schAddColSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr = addField(ctx, apr, root, dEnv)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func addField(ctx context.Context, apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("Must specify table name, column name, column type, and if column required.").SetPrintUsage().Build()
	}

	tblName := apr.Arg(0)
	if has, err := root.HasTable(ctx, tblName); err != nil {
		return errhand.BuildDError("error: could not read tables from database").AddCause(err).Build()
	} else if !has {
		return errhand.BuildDError(tblName + " not found").Build()
	}

	tbl, _, err := root.GetTable(ctx, tblName)

	if err != nil {
		return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
	}

	tblSch, err := tbl.GetSchema(ctx)
	newFieldName := apr.Arg(1)

	var tag uint64
	if val, ok := apr.GetUint(tagParam); ok {
		tag = val
	} else {
		tag = schema.AutoGenerateTag(tblSch)
	}

	newFieldType := strings.ToLower(apr.Arg(2))
	newFieldKind, ok := schema.LwrStrToKind[newFieldType]
	if !ok {
		return errhand.BuildDError(newFieldType + " is not a valid type for this new column.").SetPrintUsage().Build()
	}

	var defaultVal types.Value
	if val, ok := apr.GetValue(defaultParam); ok {
		if nomsVal, err := doltcore.StringToValue(val, newFieldKind); err != nil {
			return errhand.VerboseErrorFromError(err)
		} else {
			defaultVal = nomsVal
		}
	}

	nullable := alterschema.Null
	if apr.Contains(notNullFlag) {
		nullable = alterschema.NotNull
	}

	newTable, err := alterschema.AddColumnToTable(ctx, tbl, tag, newFieldName, newFieldKind, nullable, defaultVal, nil)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	root, err = root.PutTable(ctx, tblName, newTable)

	if err != nil {
		return errhand.BuildDError("error: failed to write table back to database").Build()
	}

	return commands.UpdateWorkingWithVErr(dEnv, root)
}
