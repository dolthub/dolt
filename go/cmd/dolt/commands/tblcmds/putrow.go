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
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var putRowShortDesc = "Adds or updates a row in a table"
var putRowLongDesc = "dolt table put-row will put a row in a table.  If a row already exists with a matching primary key" +
	"it will be overwritten with the new value. All required fields for rows in this table must be supplied or the command" +
	"will fail.  example usage:\n" +
	"\n" +
	"  dolt table put-row \"field0:value0\" \"field1:value1\" ... \"fieldN:valueN\"\n"
var putRowSynopsis = []string{
	"<table> <field_name:field_value>...",
}

type putRowArgs struct {
	FieldNames []string
	KVPs       map[string]string
	TableName  string
}

func parsePutRowArgs(ap *argparser.ArgParser, commandStr string, args []string) *putRowArgs {
	help, usage := cli.HelpAndUsagePrinters(commandStr, putRowShortDesc, putRowLongDesc, putRowSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return nil
	}

	parsedArgs := apr.Args()
	tableName := parsedArgs[0]
	fieldNames, kvps, verr := parseKVPs(parsedArgs[1:])

	if verr != nil {
		cli.PrintErrln(verr.Error())
		return nil
	}

	return &putRowArgs{fieldNames, kvps, tableName}
}

func parseKVPs(args []string) ([]string, map[string]string, errhand.VerboseError) {
	fieldNames := make([]string, len(args))
	kvps := make(map[string]string, len(args))
	for i, arg := range args {
		colonIndex := strings.IndexByte(arg, ':')

		if colonIndex != -1 {
			key := strings.TrimSpace(arg[:colonIndex])
			value := arg[colonIndex+1:]

			if key != "" {
				kvps[key] = value
				fieldNames[i] = key
			} else {
				bdr := errhand.BuildDError(`"%s" is not a valid key value pair.`, strings.TrimSpace(arg))
				bdr.AddDetails("Key value pairs must be in the format key:value, where the length of key must be at least 1 character.  \"%s\" has a length of 0 characters", strings.TrimSpace(arg))
				return nil, nil, bdr.Build()
			}
		} else {
			bdr := errhand.BuildDError(`"%s" is not a valid key value pair.`, strings.TrimSpace(arg))
			bdr.AddDetails("Key value pairs must be in the format key:value.  \"%s\" has no key value separator ':'.  ", strings.TrimSpace(arg))
			bdr.AddDetails("To set a value to empty you may use \"key:\" but not just \"key\", however leaving this key off of the command line has the same effect.")
			return nil, nil, bdr.Build()
		}
	}

	return fieldNames, kvps, nil
}

type PutRowCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd PutRowCmd) Name() string {
	return "put-row"
}

// Description returns a description of the command
func (cmd PutRowCmd) Description() string {
	return "Add a row to a table."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd PutRowCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, putRowShortDesc, putRowLongDesc, putRowSynopsis, ap)
}

func (cmd PutRowCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table being inserted into"})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{
		"field_name:field_value",
		"There should be a <field_name>:<field_value> pair for each field that you want set on this row.  If all " +
			"required fields are not set, then this command will fail."})
	return ap
}

// EventType returns the type of the event to log
func (cmd PutRowCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_PUT_ROW
}

// Exec executes the command
func (cmd PutRowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	prArgs := parsePutRowArgs(ap, commandStr, args)

	if prArgs == nil {
		return 1
	}

	if prArgs.TableName == doltdb.DocTableName {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Table '%s' is not a valid table name", doltdb.DocTableName).Build(), nil)
	}

	root, err := dEnv.WorkingRoot(ctx)
	fmt := root.VRW().Format()

	if err != nil {
		cli.PrintErrln(color.RedString("Unable to get working value."))
		return 1
	}

	tbl, ok, err := root.GetTable(ctx, prArgs.TableName)

	if err != nil {
		cli.PrintErrln(color.RedString("error: failed to read tables: " + err.Error()))
		return 1
	}

	if !ok {
		cli.PrintErrln(color.RedString("Unknown table %s", prArgs.TableName))
		return 1
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		cli.PrintErrln(color.RedString("error: failed to read schema: " + err.Error()))
		return 1
	}

	row, verr := createRow(fmt, sch, prArgs)

	if verr == nil {
		m, err := tbl.GetRowData(ctx)

		if err != nil {
			verr = errhand.BuildDError("error: failed to get row data.").AddCause(err).Build()
		} else {
			me := m.Edit()
			updated, err := me.Set(row.NomsMapKey(sch), row.NomsMapValue(sch)).Map(ctx)

			if err != nil {
				verr = errhand.BuildDError("error: failed to modify table").AddCause(err).Build()
			} else {
				tbl, err = tbl.UpdateRows(ctx, updated)

				if err != nil {
					verr = errhand.BuildDError("error: failed to update rows").AddCause(err).Build()
				} else {
					root, err = root.PutTable(ctx, prArgs.TableName, tbl)

					if err != nil {
						verr = errhand.BuildDError("error: failed to write table back to database").AddCause(err).Build()
					} else {
						verr = commands.UpdateWorkingWithVErr(dEnv, root)
					}
				}
			}
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	cli.Println(color.CyanString("Successfully put row."))
	return 0
}

func createRow(nbf *types.NomsBinFormat, sch schema.Schema, prArgs *putRowArgs) (row.Row, errhand.VerboseError) {
	var unknownFields []string
	untypedTaggedVals := make(row.TaggedValues)
	for k, v := range prArgs.KVPs {
		if col, ok := schema.ColFromName(sch, k); ok {
			untypedTaggedVals[col.Tag] = types.String(v)
		} else {
			unknownFields = append(unknownFields, k)
		}
	}

	if len(unknownFields) > 0 {
		bdr := errhand.BuildDError("Not all supplied keys are known in this table's schema.")
		bdr.AddDetails("The fields %v were supplied but are not known in this table.", unknownFields)
		return nil, bdr.Build()
	}

	untypedSch, err := untyped.UntypeSchema(sch)

	if err != nil {
		return nil, errhand.BuildDError("error: failed to get schemas").AddCause(err).Build()
	}

	mapping, err := rowconv.TagMapping(untypedSch, sch)

	if err != nil {
		return nil, errhand.BuildDError("Failed to infer mapping").AddCause(err).Build()
	}

	rconv, err := rowconv.NewRowConverter(mapping)

	if err != nil {
		return nil, errhand.BuildDError("failed to create row converter").AddCause(err).Build()
	}

	untypedRow, err := row.New(nbf, untypedSch, untypedTaggedVals)

	if err != nil {
		return nil, errhand.BuildDError("").AddCause(err).Build()
	}

	typedRow, err := rconv.Convert(untypedRow)

	if err != nil {
		return nil, errhand.BuildDError("inserted row does not match schema").AddCause(err).Build()
	}

	if col, err := row.GetInvalidCol(typedRow, sch); err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	} else if col != nil {
		bdr := errhand.BuildDError("Missing required fields.")
		bdr.AddDetails("The value for the column %s is not valid", col.Name)
		return nil, bdr.Build()
	}

	return typedRow, nil
}
