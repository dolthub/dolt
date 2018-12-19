package edit

import (
	"flag"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"os"
	"strings"
)

func rmRowUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

type rmRowArgs struct {
	TableName string
	PKFldName string
	PKValue   string
}

func parseRmRowArgs(commandStr string, args []string) (*rmRowArgs, errhand.VerboseError) {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = updateUsage(fs)

	tableName := fs.String(tableParam, "", "The table where the row should be added.")

	fs.Parse(args)

	if *tableName == "" {
		return nil, errhand.BuildDError("Missing required paramater -%s", tableParam).Build()
	}

	pk := ""
	pkVal := ""
	if fs.NArg() > 1 {
		bdr := errhand.BuildDError("Must supply exactly one key:value for the row to delete.")
		bdr.AddDetails("Need exactly one key:value where key should be the name of the primary key field, and is the primary key's value for the row being deleted.")
		return nil, bdr.Build()
	} else if fs.NArg() == 1 {
		keys, kvps, verr := parseKVPs(fs.Args())

		if verr != nil {
			return nil, verr
		}

		pk = keys[0]
		pkVal = kvps[pk]
	}

	return &rmRowArgs{*tableName, pk, pkVal}, nil
}

func RmRow(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	rmArgs, verr := parseRmRowArgs(commandStr, args)

	if verr == nil {
		var root *doltdb.RootValue
		var tbl *doltdb.Table
		root, tbl, verr = getRootAndTable(cliEnv, rmArgs.TableName)

		if verr == nil {
			var pkVal types.Value
			pkVal, verr = getPKOfRowToDelete(root, tbl, rmArgs.PKFldName, rmArgs.PKValue)

			if verr == nil {
				verr = updateTableWithRowRemoved(root, tbl, rmArgs.TableName, pkVal, cliEnv)
			}
		}
	}

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	fmt.Println(color.CyanString("Successfully Removed row."))
	return 0
}

func getRootAndTable(cliEnv *env.DoltCLIEnv, tblName string) (*doltdb.RootValue, *doltdb.Table, errhand.VerboseError) {
	root, err := cliEnv.WorkingRoot()

	if err != nil {
		return nil, nil, errhand.BuildDError("Unable to get working value for the dolt data repository.").Build()
	}

	tbl, ok := root.GetTable(tblName)

	if !ok {
		return nil, nil, errhand.BuildDError("Unknown table %s", tblName).Build()
	}

	return root, tbl, nil
}

func getPKOfRowToDelete(root *doltdb.RootValue, tbl *doltdb.Table, pkFldName, pkValue string) (types.Value, errhand.VerboseError) {
	vrw := root.VRW()
	sch := tbl.GetSchema(vrw)

	fld := sch.GetField(sch.GetPKIndex())

	if pkFldName == "" {
		return nil, errhand.BuildDError("Missing required parameter %s:PK_VALUE", fld.NameStr()).Build()
	} else if fld.NameStr() != strings.ToLower(pkFldName) {
		bdr := errhand.BuildDError("Missing required parameter %s:PK_VALUE", fld.NameStr())
		bdr.AddDetails("Supplied parameter %[1]s:%[2]s is not valid as %[1]s is not the primary key.", pkFldName, pkValue)
		return nil, bdr.Build()
	}

	convFunc := table.GetConvFunc(types.StringKind, fld.NomsKind())

	if convFunc == nil {
		bdr := errhand.BuildDError(`Could not convert from "%[1]s" to a %[2]s as conversion from string to %[2]s is not defined.`, pkValue, fld.KindString())
		return nil, bdr.Build()
	}

	pk, err := convFunc(types.String(pkValue))

	if err != nil {
		return nil, errhand.BuildDError(`Failed to convert from "%s" to a %s`, pkValue, fld.KindString()).Build()
	}

	return pk, nil
}

func updateTableWithRowRemoved(root *doltdb.RootValue, tbl *doltdb.Table, tblName string, pk types.Value, cliEnv *env.DoltCLIEnv) errhand.VerboseError {
	m := tbl.GetRowData()
	_, ok := m.MaybeGet(pk)

	if !ok {
		return errhand.BuildDError(`No row with the key of %s was found.`, types.EncodedValue(pk)).Build()
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

	verr = errhand.PanicToVError("Failed to update the table.", func() errhand.VerboseError {
		tbl = tbl.UpdateRows(m)
		root = root.PutTable(cliEnv.DoltDB, tblName, tbl)
		return nil
	})

	if verr != nil {
		return verr
	}

	verr = cliEnv.UpdateWorkingRoot(root)

	return verr
}
