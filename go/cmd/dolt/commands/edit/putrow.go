package edit

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"os"
	"strings"
)

type putRowArgs struct {
	FieldNames []string
	KVPs       map[string]string
	TableName  string
}

func putRowUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func parsePutRowArgs(commandStr string, args []string) (*putRowArgs, errhand.VerboseError) {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = putRowUsage(fs)

	tableName := fs.String(tableParam, "", "The table where the row should be added.")

	fs.Parse(args)

	if *tableName == "" {
		return nil, errhand.BuildDError("Missing required paramater -%s", tableParam).Build()
	}

	fieldNames, kvps, verr := parseKVPs(fs.Args())

	if verr != nil {
		return nil, verr
	}

	return &putRowArgs{fieldNames, kvps, *tableName}, nil
}

func parseKVPs(args []string) ([]string, map[string]string, errhand.VerboseError) {
	fieldNames := make([]string, len(args))
	kvps := make(map[string]string, len(args))
	for i, arg := range args {
		colonIndex := strings.IndexByte(arg, ':')

		if colonIndex != -1 {
			key := strings.ToLower(strings.TrimSpace(arg[:colonIndex]))
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

func PutRow(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	prArgs, verr := parsePutRowArgs(commandStr, args)

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	root, err := cliEnv.WorkingRoot()

	if err != nil {
		fmt.Fprintln(os.Stderr, color.RedString("Unable to get working value."))
		return 1
	}

	tbl, ok := root.GetTable(prArgs.TableName)

	if !ok {
		fmt.Fprintln(os.Stderr, color.RedString("Unknown table %s", prArgs.TableName))
		return 1
	}

	vrw := root.VRW()
	sch := tbl.GetSchema(vrw)
	row, verr := createRow(sch, prArgs)

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	me := tbl.GetRowData().Edit()
	updated := me.Set(table.GetPKFromRow(row), table.GetNonPKFieldListFromRow(row, vrw)).Map()
	tbl = tbl.UpdateRows(updated)
	root = root.PutTable(cliEnv.DoltDB, prArgs.TableName, tbl)

	verr = cliEnv.UpdateWorkingRoot(root)

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	fmt.Println(color.CyanString("Successfully put row."))
	return 0
}

func createRow(sch *schema.Schema, prArgs *putRowArgs) (*table.Row, errhand.VerboseError) {
	_, _, unknownFields := sch.IntersectFields(prArgs.FieldNames)
	if len(unknownFields) > 0 {
		bdr := errhand.BuildDError("Not all supplied keys are known in this table's schema.")
		bdr.AddDetails("The fields %v were supplied but are not known in this table.", unknownFields)
		return nil, bdr.Build()
	}

	rd, firstBad := table.RowDataFromUntypedMap(sch, prArgs.KVPs)
	row := table.NewRow(rd)
	if firstBad != nil {
		fld := sch.GetField(sch.GetFieldIndex(*firstBad))
		val := prArgs.KVPs[*firstBad]
		bdr := errhand.BuildDError("Not all parameter values could be converted to the appropriate types for the table.")
		bdr.AddDetails(`For parameter "%s", could not convert "%s" to a %s`, *firstBad, val, fld.KindString())
		return nil, bdr.Build()
	}

	if !table.RowIsValid(row) {
		invalidFlds := table.InvalidFieldsForRow(row)
		bdr := errhand.BuildDError("Missing required fields.")
		bdr.AddDetails("The following missing fields are required: %v", invalidFlds)
		return nil, bdr.Build()
	}

	return row, nil
}
