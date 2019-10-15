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
	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

type StrMapper interface {
	Map(str string) (string, bool)
}

type IdentityMapper struct{}

func (m IdentityMapper) Map(str string) (string, bool) {
	return str, true
}

type MapMapper map[string]string

func (m MapMapper) Map(str string) (string, bool) {
	v, ok := m[str]
	return v, ok
}

const (
	createFlag          = "create"
	updateFlag          = "update"
	replaceFlag			= "replace"
	dryRunFlag          = "dry-run"
	fileTypeParam       = "file-type"
	pksParam            = "pks"
	mappingParam        = "map"
	floatThresholdParam = "float-threshold"
	upperParam          = "upper"
	lowerParam          = "lower"
)

var schImportShortDesc = "Creates a new table with an inferred schema."
var schImportLongDesc = ""
var schImportSynopsis = []string{
	"[--create|--update|--replace] [--force] [--dry-run] [--lower|--upper] [--file-type <type>] [--float-threshold] [--map <mapping-file>] --pks <field>,... <table> <file>",
}

type importOp int

const (
	createOp importOp = iota
	updateOp
	replaceOp
)

type importArgs struct {
	op             importOp
	existingSch    schema.Schema
	fileType       string
	fileName       string
	colMapper      StrMapper
	floatThreshold float64
	lowerCols      bool
	upperCols      bool
}


func Import(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "Name of the table to be created."
	ap.ArgListHelp["file"] = "The file being used to infer the schema."
	ap.SupportsFlag(dryRunFlag, "", "")
	ap.SupportsString(floatThresholdParam, "", "float", "")
	ap.SupportsFlag(upperParam, "", "")
	ap.SupportsFlag(lowerParam, "", "")
	ap.SupportsFlag(createFlag, "c", "")
	ap.SupportsFlag(updateFlag, "u", "")
	ap.SupportsFlag(replaceFlag, "r", "")
	ap.SupportsString(fileTypeParam, "", "type", "")
	ap.SupportsString(pksParam, "", "comma-separated-col-names", "")
	ap.SupportsString(mappingParam, "", "mapping-file", "")

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

	var colMapper StrMapper
	if mappingFile != "" {
		if mappingExists, _ := dEnv.FS.Exists(mappingFile); mappingExists {
			var m map[string]string
			err := filesys.UnmarshalJSONFile(dEnv.FS, mappingFile, &m)

			if err != nil {
				return errhand.BuildDError("error: invalid mapper file.").AddCause(err).Build()
			}

			colMapper = MapMapper(m)
		} else {
			return errhand.BuildDError("error: '%s' does not exist.", mappingFile).Build()
		}
	} else {
		colMapper = IdentityMapper{}
	}

	pkCols := set.NewStrSet(pks)

	floatThresholdStr := apr.GetValueOrDefault(floatThresholdParam, "0.0")
	floatThreshold, err := strconv.ParseFloat(floatThresholdStr, 64)

	if err != nil {
		return errhand.BuildDError("error: '%s' is not a valid float in the range 0.0 (all floats) to 1.0 (no floats)", floatThresholdStr).SetPrintUsage().Build()
	}

	impArgs := importArgs{
		op:             op,
		existingSch:    existingSch,
		fileName:       fileName,
		fileType:       apr.GetValueOrDefault(fileTypeParam, filepath.Ext(fileName)),
		colMapper:      colMapper,
		floatThreshold: floatThreshold,
		lowerCols:      apr.Contains(lowerParam),
		upperCols:      apr.Contains(upperParam),
	}

	sch, verr := inferSchemaFromFile(ctx, dEnv.DoltDB.ValueReadWriter().Format(), pkCols, &impArgs)

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

		cli.PrintErrln("Created table successfully.")
	}

	return nil
}

func inferSchemaFromFile(ctx context.Context, nbf *types.NomsBinFormat, pkCols *set.StrSet, args *importArgs) (schema.Schema, errhand.VerboseError) {
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
	}

	inferrer := newInferrer(pkCols, rd.GetSchema(), args)
	rdProcFunc := pipeline.ProcFuncForReader(ctx, rd)
	p := pipeline.NewAsyncPipeline(rdProcFunc, inferrer.sinkRow, nil, inferrer.badRow)
	p.Start()

	err := p.Wait()

	if err != nil {
		return nil, errhand.BuildDError("error: failed to infer schema").AddCause(err).Build()
	}

	return inferrer.inferSchema()
}

type inferrer struct {
	sch     schema.Schema
	pkCols  *set.StrSet
	impArgs *importArgs

	colNames  []string
	colCount  int
	colType   []map[types.NomsKind]int
	negatives []bool
}

func newInferrer(pkCols *set.StrSet, sch schema.Schema, args *importArgs) *inferrer {
	colColl := sch.GetAllCols()
	colNames := make([]string, 0, colColl.Size())

	_ = colColl.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		colNames = append(colNames, col.Name)
		return false, nil
	})

	colCount := len(colNames)
	colType := make([]map[types.NomsKind]int, colCount)
	negatives := make([]bool, colCount)
	for i := 0; i < colCount; i++ {
		colType[i] = make(map[types.NomsKind]int)
	}

	return &inferrer{sch, pkCols, args, colNames, colCount, colType, negatives}
}

func (inf *inferrer) inferSchema() (schema.Schema, errhand.VerboseError) {
	cols := make([]schema.Column, 0, inf.colCount)
	existingCols := inf.impArgs.existingSch.GetAllCols()

	tag := uint64(0)
	for i, name := range inf.colNames {
		if inf.impArgs.upperCols {
			name = strings.ToUpper(name)
		} else if inf.impArgs.lowerCols {
			name = strings.ToLower(name)
		}

		if mappedName, ok := inf.impArgs.colMapper.Map(name); ok {
			name = mappedName
		}

		partOfPK := inf.pkCols.Contains(name)
		typeToCount := inf.colType[i]
		hasNegatives := inf.negatives[i]
		kind, nullable := typeCountsToKind(name, typeToCount, hasNegatives)

		constraints := make([]schema.ColConstraint, 0, 1)
		if !nullable {
			constraints = append(constraints, schema.NotNullConstraint{})
		}

		tag = nextTag(tag, existingCols)
		thisTag := tag
		if existingCol, ok := existingCols.GetByName(name); ok {
			thisTag = existingCol.Tag
		} else {
			tag++
		}

		cols = append(cols, schema.NewColumn(name, thisTag, kind, partOfPK, constraints...))
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		return nil, errhand.BuildDError("").AddCause(err).Build()
	}

	return schema.SchemaFromCols(colColl), nil
}

func nextTag(tag uint64, cols *schema.ColCollection) uint64 {
	for {
		_, ok := cols.GetByTag(tag)

		if !ok {
			return tag
		}

		tag++
	}
}

func typeCountsToKind(name string, typeToCount map[types.NomsKind]int, hasNegatives bool) (types.NomsKind, bool) {
	var nullable bool
	kind := types.NullKind

	for t := range typeToCount {
		if t == types.NullKind {
			nullable = true
			continue
		} else if kind == types.NullKind {
			kind = t
		}

		if kind == t {
			continue
		}

		switch kind {
		case types.StringKind:
			if nullable {
				return types.StringKind, true
			}

		case types.UUIDKind:
			if t != types.UUIDKind {
				cli.PrintErrln(color.YellowString("warning: column %s has a mix of uuids and non uuid strings.", name))
				kind = types.StringKind
			}

		case types.BoolKind:
			if t != types.BoolKind {
				kind = types.StringKind
			}

		case types.IntKind:
			if t == types.FloatKind {
				kind = types.FloatKind
			} else if t == types.UintKind {
				if !hasNegatives {
					kind = types.UintKind
				} else {
					cli.PrintErrln(color.YellowString("warning: %s has values larger than a 64 bit signed integer can hold, and negative numbers.  This will be interpreted as a string.", name))
					kind = types.StringKind
				}
			} else {
				kind = types.StringKind
			}

		case types.UintKind:
			if t == types.IntKind {
				if hasNegatives {
					cli.PrintErrln(color.YellowString("warning: %s has values larger than a 64 bit signed integer can hold, and negative numbers.  This will be interpreted as a string.", name))
					kind = types.StringKind
				}
			} else {
				kind = types.StringKind
			}
		}
	}

	if kind == types.NullKind {
		kind = types.StringKind
	}

	return kind, nullable
}

func (inf *inferrer) sinkRow(p *pipeline.Pipeline, ch <-chan pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure) {
	for r := range ch {
		i := 0
		_, _ = r.Row.IterSchema(inf.sch, func(tag uint64, val types.Value) (stop bool, err error) {
			defer func() {
				i++
			}()

			strVal := string(val.(types.String))
			kind, hasNegs := leastPermissiveKind(strVal, inf.impArgs.floatThreshold)

			if hasNegs {
				inf.negatives[i] = true
			}

			count, _ := inf.colType[i][kind]
			count++

			inf.colType[i][kind] = count

			return false, nil
		})
	}
}

func leastPermissiveKind(strVal string, floatThreshold float64) (types.NomsKind, bool) {
	if len(strVal) == 0 {
		return types.NullKind, false
	}

	strVal = strings.TrimSpace(strVal)
	kind := types.StringKind
	hasNegativeNums := false

	if _, err := uuid.Parse(strVal); err == nil {
		kind = types.UUIDKind
	} else if _, err := strconv.ParseBool(strVal); err == nil {
		kind = types.BoolKind
	} else if negs, numKind := leastPermissiveNumericKind(strVal, floatThreshold); numKind != types.NullKind {
		kind = numKind
		hasNegativeNums = negs
	}

	return kind, hasNegativeNums
}

var lenDecEncodedMaxInt = len(strconv.FormatInt(math.MaxInt64, 10))

func leastPermissiveNumericKind(strVal string, floatThreshold float64) (isNegative bool, kind types.NomsKind) {
	isNum, isFloat, isNegative := stringNumericProperties(strVal)

	if !isNum {
		return false, types.NullKind
	} else if isFloat {
		if floatThreshold != 0.0 {
			floatParts := strings.Split(strVal, ".")
			decimalPart, err := strconv.ParseFloat("0." + floatParts[1], 64)

			if err != nil {
				panic(err)
			}

			if decimalPart > floatThreshold {
				return isNegative, types.FloatKind
			}

			return isNegative, types.IntKind
		}
		return isNegative, types.FloatKind
	} else if len(strVal) < lenDecEncodedMaxInt {
		// Prefer Ints if everything fits
		return isNegative, types.IntKind
	} else if isNegative {
		_, sErr := strconv.ParseInt(strVal, 10, 64)

		if sErr == nil {
			return isNegative, types.IntKind
		}
	} else {
		_, uErr := strconv.ParseUint(strVal, 10, 64)
		_, sErr := strconv.ParseInt(strVal, 10, 64)

		if sErr == nil {
			return false, types.IntKind
		} else if uErr == nil {
			return false, types.UintKind
		}
	}

	return false, types.NullKind
}

func stringNumericProperties(strVal string) (isNum, isFloat, isNegative bool) {
	if len(strVal) == 0 {
		return false, false, false
	}

	isNum = true
	for i, c := range strVal {
		if i == 0 && c == '-' {
			isNegative = true
			continue
		} else if i == 0 && c == '0' && len(strVal) > 1 && strVal[i+1] != '.' {
			// by default treat leading zeroes as invalid
			return false, false, false
		}

		if c != '.' && c < '0' && c > '9' {
			return false, false, false
		}

		if c == '.' {
			if isFloat {
				// found 2 decimal points
				return false, false, false
			} else {
				isFloat = true
			}
		}
	}

	return isNum, isFloat, isNegative
}

func (inf *inferrer) badRow(trf *pipeline.TransformRowFailure) (quit bool) {
	return false
}