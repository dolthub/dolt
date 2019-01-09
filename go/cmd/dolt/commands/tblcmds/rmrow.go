package tblcmds

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
)

var rmRowShortDesc = "Removes row(s) from a table"
var rmRowLongDesc = "dolt table rm-row will remove one or more rows from a table in the working set."
var rmRowSynopsis = []string{
	"<table> <primary_key>...",
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
		cli.PrintErrln("invalid usage")
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
		var pkVals []types.Value
		var pkFldName string
		pkFldName, pkVals, verr = getPKsOfRowsToRm(root, tbl, rmArgs.PKs)

		if verr == nil {
			verr = updateTableWithRowsRemoved(root, tbl, rmArgs.TableName, pkFldName, pkVals, dEnv)
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func getRootAndTable(dEnv *env.DoltEnv, tblName string) (*doltdb.RootValue, *doltdb.Table, errhand.VerboseError) {
	root, err := dEnv.WorkingRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get working value for the dolt data repository.").Build()
	}

	tbl, ok := root.GetTable(tblName)

	if !ok {
		return nil, nil, errhand.BuildDError("Unknown table %s", tblName).Build()
	}

	return root, tbl, nil
}

func getPKsOfRowsToRm(root *doltdb.RootValue, tbl *doltdb.Table, pks []string) (string, []types.Value, errhand.VerboseError) {
	vrw := root.VRW()
	sch := tbl.GetSchema(vrw)

	fld := sch.GetField(sch.GetPKIndex())
	convFunc := table.GetConvFunc(types.StringKind, fld.NomsKind())

	if convFunc == nil {
		bdr := errhand.BuildDError(`Conversion from string to %s is not defined.`, fld.KindString())
		return "", nil, bdr.Build()
	}

	pkVals := make([]types.Value, len(pks))
	for i, pkStr := range pks {
		pkVal, err := convFunc(types.String(pkStr))

		if err != nil {
			return "", nil, errhand.BuildDError(`Failed to convert from "%s" to a %s`, pkStr, fld.KindString()).Build()
		}

		pkVals[i] = pkVal
	}

	return fld.NameStr(), pkVals, nil
}

func updateTableWithRowsRemoved(root *doltdb.RootValue, tbl *doltdb.Table, tblName string, pkFldName string, pkVals []types.Value, dEnv *env.DoltEnv) errhand.VerboseError {
	m := tbl.GetRowData()

	updates := 0
	for _, pk := range pkVals {
		_, ok := m.MaybeGet(pk)

		if !ok {
			cli.PrintErrln(color.YellowString(`No row with %s equal to %s was found.`, pkFldName, types.EncodedValue(pk)))
			continue
		}

		verr := errhand.PanicToVError("Failed to remove the row from the table.", func() errhand.VerboseError {
			me := m.Edit()
			me.Remove(pk)
			m = me.Map()
			return nil
		})

		if verr != nil {
			return verr
		}

		updates++
	}

	verr := errhand.PanicToVError("Failed to update the table.", func() errhand.VerboseError {
		tbl = tbl.UpdateRows(m)
		root = root.PutTable(dEnv.DoltDB, tblName, tbl)
		return nil
	})

	if verr != nil {
		return verr
	}

	verr = commands.UpdateWorkingWithVErr(dEnv, root)

	if verr == nil {
		cli.Printf("Removed %d rows", updates)
	}

	return verr
}
