package cli

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

var ErrEmptyDefTuple = errors.New("empty definition tuple")

type UsagePrinter func()

func ParseArgs(ap *argparser.ArgParser, args []string, usagePrinter UsagePrinter) *argparser.ArgParseResults {
	apr, err := ap.Parse(args)

	if err != nil {
		if err != argparser.ErrHelp {
			PrintErrln(err.Error())
			usagePrinter()
			os.Exit(1)
		}

		// --help param
		usagePrinter()
		os.Exit(0)
	}

	return apr
}

func HelpAndUsagePrinters(commandStr, shortDesc, longDesc string, synopsis []string, ap *argparser.ArgParser) (UsagePrinter, UsagePrinter) {
	return func() {
			PrintHelpText(commandStr, shortDesc, longDesc, synopsis, ap)
		}, func() {
			PrintUsage(commandStr, synopsis, ap)
		}
}

type ColumnError struct {
	name string
	msg  string
}

func IsColumnError(err error) bool {
	_, ok := err.(ColumnError)

	return ok
}

func ColumnNameFromColumnError(err error) string {
	ce, ok := err.(ColumnError)

	if !ok {
		panic("Bug.  Test IsColumnError before calling")
	}

	return ce.name
}

func (ce ColumnError) Error() string {
	return ce.msg
}

func parseTuples(args []string, pkCols *schema.ColCollection) ([]map[uint64]string, error) {
	defTpl := strings.Split(args[0], ",")

	if len(defTpl) == 0 {
		return nil, ErrEmptyDefTuple
	}

	defTags := make([]uint64, len(defTpl))
	for i, colName := range defTpl {
		col, ok := pkCols.GetByName(colName)

		if !ok {
			return nil, ColumnError{colName, colName + " is not a known primary key column."}
		}

		defTags[i] = col.Tag
	}

	var results []map[uint64]string
	for _, arg := range args[1:] {
		valTpl := strings.Split(arg, ",")

		result := make(map[uint64]string)
		for i, key := range defTags {
			if i < len(valTpl) {
				result[key] = valTpl[i]
			}
		}

		results = append(results, result)
	}

	return results, nil
}

func ParseKeyValues(sch schema.Schema, args []string) ([]types.Value, error) {
	pkCols := sch.GetPKCols()

	var pkMaps []map[uint64]string
	if sch.GetPKCols().Size() == 1 {
		pkCol := pkCols.GetByIndex(0)

		start := 0
		if args[start] == pkCol.Name {
			start = 1
		}

		for _, pk := range args[start:] {
			pkMaps = append(pkMaps, map[uint64]string{pkCol.Tag: pk})
		}

	} else {
		var err error
		pkMaps, err = parseTuples(args, pkCols)

		if err != nil {
			return nil, err
		}
	}

	var err error
	convFuncs := make(map[uint64]doltcore.ConvFunc)
	sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		convFunc := doltcore.GetConvFunc(types.StringKind, col.Kind)
		if convFunc == nil {
			err = ColumnError{col.Name, "Conversion from string to " + col.KindString() + "is not defined."}
			return true
		}
		convFuncs[tag] = convFunc
		return false
	})

	if err != nil {
		return nil, err
	}

	var pkVals []types.Value
	for _, pkMap := range pkMaps {
		taggedVals := make(row.TaggedValues)
		for k, v := range pkMap {
			val, err := convFuncs[k](types.String(v))

			if err != nil {

			}

			taggedVals[k] = val
		}

		pkVals = append(pkVals, taggedVals.NomsTupleForTags(types.Format_7_18, pkCols.Tags, true).Value(context.TODO()))
	}

	return pkVals, nil
}
