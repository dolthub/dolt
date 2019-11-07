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

package commands

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	dtypes "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/types"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/libraries/utils/mathutil"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	SchemaOnlyDiff    = 1
	DataOnlyDiff      = 2
	SchemaAndDataDiff = SchemaOnlyDiff | DataOnlyDiff

	ColorDiffOutput = 1
	SQLDiffOutput = 2

	DataFlag    = "data"
	SchemaFlag  = "schema"
	SummaryFlag = "summary"
	SQLFlag = "sql"
)

type DiffSink interface {
	GetSchema() schema.Schema
	ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error
	Close() error
}

var diffShortDesc = "Show changes between commits, commit and working tree, etc"
var diffLongDesc = `Show changes between the working and staged tables, changes between the working tables and the tables within a commit, or changes between tables at two commits.

dolt diff [--options] [<tables>...]
   This form is to view the changes you made relative to the staging area for the next commit. In other words, the differences are what you could tell Dolt to further add but you still haven't. You can stage these changes by using dolt add.

dolt diff [--options] <commit> [<tables>...]
   This form is to view the changes you have in your working tables relative to the named <commit>. You can use HEAD to compare it with the latest commit, or a branch name to compare with the tip of a different branch.

dolt diff [--options] <commit> <commit> [<tables>...]
   This is to view the changes between two arbitrary <commit>.
`

var diffSynopsis = []string{
	"[options] [<commit>] [--data|--schema] [<tables>...]",
	"[options] <commit> <commit> [--data|--schema] [<tables>...]",
}

func Diff(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(DataFlag, "d", "Show only the data changes, do not show the schema changes (Both shown by default).")
	ap.SupportsFlag(SchemaFlag, "s", "Show only the schema changes, do not show the data changes (Both shown by default).")
	ap.SupportsFlag(SummaryFlag, "", "Show summary of data changes")
	ap.SupportsFlag(SQLFlag, "q", "Output changes as an SQL modification")
	help, _ := cli.HelpAndUsagePrinters(commandStr, diffShortDesc, diffLongDesc, diffSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.Contains(SQLFlag) {
		cli.PrintErrln("command not yet supported")
		return 1
	}

	diffParts := SchemaAndDataDiff
	if apr.Contains(DataFlag) && !apr.Contains(SchemaFlag) {
		diffParts = DataOnlyDiff
	} else if apr.Contains(SchemaFlag) && !apr.Contains(DataFlag) {
		diffParts = SchemaOnlyDiff
	}

	diffOutput := ColorDiffOutput
	if apr.Contains(SQLFlag) {
		diffOutput = SQLDiffOutput
	}

	r1, r2, tables, verr := getRoots(ctx, apr.Args(), dEnv)

	summary := apr.Contains(SummaryFlag)

	if verr == nil {
		verr = diffRoots(ctx, r1, r2, tables, diffParts, diffOutput, dEnv, summary)
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

// this doesnt work correctly.  Need to be able to distinguish commits from tables
func getRoots(ctx context.Context, args []string, dEnv *env.DoltEnv) (r1, r2 *doltdb.RootValue, tables []string, verr errhand.VerboseError) {
	roots := make([]*doltdb.RootValue, 2)

	i := 0
	for _, arg := range args {
		cs, err := doltdb.NewCommitSpec(arg, dEnv.RepoState.Head.Ref.String())
		if err != nil {
			break
		}

		cm, err := dEnv.DoltDB.Resolve(ctx, cs)
		if err != nil {
			break
		}

		roots[i], err = cm.GetRootValue()

		if err != nil {
			return nil, nil, nil, errhand.BuildDError("error: failed to get root").AddCause(err).Build()
		}

		i++
	}

	if i < 2 {
		roots[1] = roots[0]
		roots[0], verr = GetWorkingWithVErr(dEnv)

		if verr == nil && i == 0 {
			roots[1], verr = GetStagedWithVErr(dEnv)
		}

		if verr != nil {
			return nil, nil, args, verr
		}
	}

	for ; i < len(args); i++ {
		tbl := args[i]
		has0, err := roots[0].HasTable(ctx, tbl)

		if err != nil {
			return nil, nil, nil, errhand.BuildDError("error: failed to read tables").AddCause(err).Build()
		}

		has1, err := roots[1].HasTable(ctx, tbl)

		if err != nil {
			return nil, nil, nil, errhand.BuildDError("error: failed to read tables").AddCause(err).Build()
		}

		if !(has0 || has1) {
			verr := errhand.BuildDError("error: Unknown table: '%s'", tbl).Build()
			return nil, nil, nil, verr
		}

		tables = append(tables, tbl)
	}

	return roots[0], roots[1], tables, nil
}

func getRootForCommitSpecStr(ctx context.Context, csStr string, dEnv *env.DoltEnv) (string, *doltdb.RootValue, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(csStr, dEnv.RepoState.Head.Ref.String())

	if err != nil {
		bdr := errhand.BuildDError(`"%s" is not a validly formatted branch, or commit reference.`, csStr)
		return "", nil, bdr.AddCause(err).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return "", nil, errhand.BuildDError(`Unable to resolve "%s"`, csStr).AddCause(err).Build()
	}

	r, err := cm.GetRootValue()

	if err != nil {
		return "", nil, errhand.BuildDError("error: failed to get root").AddCause(err).Build()
	}

	h, err := cm.HashOf()

	if err != nil {
		return "", nil, errhand.BuildDError("error: failed to get commit hash").AddCause(err).Build()
	}

	return h.String(), r, nil
}

func diffRoots(ctx context.Context, r1, r2 *doltdb.RootValue, tblNames []string, diffParts int, diffOutput int, dEnv *env.DoltEnv, summary bool) errhand.VerboseError {
	var err error
	if len(tblNames) == 0 {
		tblNames, err = actions.AllTables(ctx, r1, r2)
	}

	if err != nil {
		return errhand.BuildDError("error: unable to read tables").AddCause(err).Build()
	}

	for _, tblName := range tblNames {
		tbl1, ok1, err := r1.GetTable(ctx, tblName)

		if err != nil {
			return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
		}

		tbl2, ok2, err := r2.GetTable(ctx, tblName)

		if err != nil {
			return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
		}

		if !ok1 && !ok2 {
			bdr := errhand.BuildDError("Table could not be found.")
			bdr.AddDetails("The table %s does not exist.", tblName)
			cli.PrintErrln(bdr.Build())
		} else if tbl1 != nil && tbl2 != nil {
			h1, err := tbl1.HashOf()

			if err != nil {
				return errhand.BuildDError("error: failed to get table hash").Build()
			}

			h2, err := tbl2.HashOf()

			if err != nil {
				return errhand.BuildDError("error: failed to get table hash").Build()
			}

			if h1 == h2 {
				continue
			}
		}

		printTableDiffSummary(tblName, tbl1, tbl2)

		if tbl1 == nil || tbl2 == nil {
			continue
		}

		var sch1 schema.Schema
		var sch2 schema.Schema
		var sch1Hash hash.Hash
		var sch2Hash hash.Hash
		rowData1, err := types.NewMap(ctx, dEnv.DoltDB.ValueReadWriter())

		if err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		}

		rowData2, err := types.NewMap(ctx, dEnv.DoltDB.ValueReadWriter())

		if err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		}

		if ok1 {
			sch1, err = tbl1.GetSchema(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get schema").AddCause(err).Build()
			}

			schRef, err := tbl1.GetSchemaRef()

			if err != nil {
				return errhand.BuildDError("error: failed to get schema ref").AddCause(err).Build()
			}

			sch1Hash = schRef.TargetHash()
			rowData1, err = tbl1.GetRowData(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get row data").AddCause(err).Build()
			}
		}

		if ok2 {
			sch2, err = tbl2.GetSchema(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get schema").AddCause(err).Build()
			}

			schRef, err := tbl2.GetSchemaRef()

			if err != nil {
				return errhand.BuildDError("error: failed to get schema ref").AddCause(err).Build()
			}

			sch2Hash = schRef.TargetHash()
			rowData2, err = tbl2.GetRowData(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get row data").AddCause(err).Build()
			}
		}

		var verr errhand.VerboseError

		if summary {
			colLen := sch2.GetAllCols().Size()
			verr = diffSummary(ctx, rowData1, rowData2, colLen)
		}

		if sch1Hash != sch2Hash && diffOutput&SQLDiffOutput != 0 {
			return errhand.BuildDError("SQL output of schema diffs is not yet supported").Build()
		}

		if diffParts&SchemaOnlyDiff != 0 && sch1Hash != sch2Hash && !summary {
			verr = diffSchemas(tblName, sch2, sch1)
		}

		if diffParts&DataOnlyDiff != 0 && !summary {
			verr = diffRows(ctx, rowData1, rowData2, sch1, sch2, diffOutput)
		}

		if verr != nil {
			return verr
		}
	}

	return nil
}

func diffSchemas(tableName string, sch1 schema.Schema, sch2 schema.Schema) errhand.VerboseError {
	diffs, err := diff.DiffSchemas(sch1, sch2)

	if err != nil {
		return errhand.BuildDError("error: failed to diff schemas").AddCause(err).Build()
	}

	tags := make([]uint64, 0, len(diffs))

	for tag := range diffs {
		tags = append(tags, tag)
	}

	sort.Slice(tags, func(i, j int) bool {
		return tags[i] < tags[j]
	})

	cli.Println("  CREATE TABLE", tableName, "(")

	for _, tag := range tags {
		dff := diffs[tag]
		switch dff.DiffType {
		case diff.SchDiffNone:
			cli.Println(sql.FmtCol(4, 0, 0, *dff.New))
		case diff.SchDiffColAdded:
			cli.Println(color.GreenString("+ " + sql.FmtCol(2, 0, 0, *dff.New)))
		case diff.SchDiffColRemoved:
			// removed from sch2
			cli.Println(color.RedString("- " + sql.FmtCol(2, 0, 0, *dff.Old)))
		case diff.SchDiffColModified:
			// changed in sch2
			oldType, err := dtypes.NomsKindToSqlTypeString(dff.Old.Kind)
			if err != nil {
				return errhand.BuildDError("error: failed to diff schemas").AddCause(err).Build()
			}
			newType, err := dtypes.NomsKindToSqlTypeString(dff.New.Kind)
			if err != nil {
				return errhand.BuildDError("error: failed to diff schemas").AddCause(err).Build()
			}

			n0, t0 := dff.Old.Name, oldType
			n1, t1 := dff.New.Name, newType

			nameLen := 0
			typeLen := 0

			if n0 != n1 {
				n0 = color.YellowString(n0)
				n1 = color.YellowString(n1)
				nameLen = mathutil.Max(len(n0), len(n1))
			}

			if t0 != t1 {
				t0 = color.YellowString(t0)
				t1 = color.YellowString(t1)
				typeLen = mathutil.Max(len(t0), len(t1))
			}

			cli.Println("< " + sql.FmtColWithNameAndType(2, nameLen, typeLen, n0, t0, *dff.Old))
			cli.Println("> " + sql.FmtColWithNameAndType(2, nameLen, typeLen, n1, t1, *dff.New))
		}
	}

	cli.Println("  );")
	cli.Println()

	return nil
}

func dumbDownSchema(in schema.Schema) (schema.Schema, error) {
	allCols := in.GetAllCols()

	dumbCols := make([]schema.Column, 0, allCols.Size())
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		col.Name = strconv.FormatUint(tag, 10)
		col.Constraints = nil
		dumbCols = append(dumbCols, col)

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	dumbColColl, _ := schema.NewColCollection(dumbCols...)

	return schema.SchemaFromCols(dumbColColl), nil
}

func diffRows(ctx context.Context, newRows, oldRows types.Map, newSch, oldSch schema.Schema, diffOutput int) errhand.VerboseError {
	dumbNewSch, err := dumbDownSchema(newSch)

	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	dumbOldSch, err := dumbDownSchema(oldSch)

	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	untypedUnionSch, err := untyped.UntypedSchemaUnion(dumbNewSch, dumbOldSch)

	if err != nil {
		return errhand.BuildDError("Failed to merge schemas").Build()
	}

	newToUnionConv := rowconv.IdentityConverter
	if newSch != nil {
		newToUnionMapping, err := rowconv.TagMapping(newSch, untypedUnionSch)

		if err != nil {
			return errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		newToUnionConv, _ = rowconv.NewRowConverter(newToUnionMapping)
	}

	oldToUnionConv := rowconv.IdentityConverter
	if oldSch != nil {
		oldToUnionMapping, err := rowconv.TagMapping(oldSch, untypedUnionSch)

		if err != nil {
			return errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		oldToUnionConv, _ = rowconv.NewRowConverter(oldToUnionMapping)
	}

	ad := diff.NewAsyncDiffer(1024)
	ad.Start(ctx, newRows, oldRows)
	defer ad.Close()

	src := diff.NewRowDiffSource(ad, oldToUnionConv, newToUnionConv, untypedUnionSch)
	defer src.Close()

	oldColNames := make(map[uint64]string)
	newColNames := make(map[uint64]string)
	err = untypedUnionSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		oldCol, oldOk := oldSch.GetAllCols().GetByTag(tag)
		newCol, newOk := newSch.GetAllCols().GetByTag(tag)

		if oldOk {
			oldColNames[tag] = oldCol.Name
		} else {
			oldColNames[tag] = ""
		}

		if newOk {
			newColNames[tag] = newCol.Name
		} else {
			newColNames[tag] = ""
		}

		return false, nil
	})

	if err != nil {
		return errhand.BuildDError("error: failed to map columns to tags").Build()
	}

	schemasEqual := reflect.DeepEqual(oldColNames, newColNames)
	numHeaderRows := 1
	if !schemasEqual {
		numHeaderRows = 2
	}

	// is there an easier way to do this?
	sink, err := func(diffOutput int) (DiffSink, error) {
		if diffOutput&ColorDiffOutput != 0 {
			return diff.NewColorDiffSink(iohelp.NopWrCloser(cli.CliOut), untypedUnionSch, numHeaderRows)
		} else {
			return diff.NewSQLDiffSink(iohelp.NopWrCloser(cli.CliOut), untypedUnionSch, numHeaderRows)
		}
	}(diffOutput)

	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	defer sink.Close()

	fwtTr := fwt.NewAutoSizingFWTTransformer(untypedUnionSch, fwt.HashFillWhenTooLong, 1000)
	nullPrinter := nullprinter.NewNullPrinter(untypedUnionSch)
	transforms := pipeline.NewTransformCollection(
		pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow),
		pipeline.NamedTransform{Name: fwtStageName, Func: fwtTr.TransformToFWT},
	)

	var verr errhand.VerboseError
	badRowCallback := func(trf *pipeline.TransformRowFailure) (quit bool) {
		verr = errhand.BuildDError("Failed transforming row").AddDetails(trf.TransformName).AddDetails(trf.Details).Build()
		return true
	}

	sinkProcFunc := pipeline.ProcFuncForSinkFunc(sink.ProcRowWithProps)
	p := pipeline.NewAsyncPipeline(pipeline.ProcFuncForSourceFunc(src.NextDiff), sinkProcFunc, transforms, badRowCallback)

	if schemasEqual {
		schRow, err := untyped.NewRowFromTaggedStrings(newRows.Format(), untypedUnionSch, newColNames)

		if err != nil {

		}

		p.InjectRow(fwtStageName, schRow)
	} else {
		newSchRow, err := untyped.NewRowFromTaggedStrings(newRows.Format(), untypedUnionSch, oldColNames)

		if err != nil {

		}

		p.InjectRowWithProps(fwtStageName, newSchRow, map[string]interface{}{diff.DiffTypeProp: diff.DiffModifiedOld})
		oldSchRow, err := untyped.NewRowFromTaggedStrings(newRows.Format(), untypedUnionSch, newColNames)

		if err != nil {

		}

		p.InjectRowWithProps(fwtStageName, oldSchRow, map[string]interface{}{diff.DiffTypeProp: diff.DiffModifiedNew})
	}

	p.Start()
	if err = p.Wait(); err != nil {
		return errhand.BuildDError("Error diffing: %v", err.Error()).Build()
	}

	return verr
}

var emptyHash = hash.Hash{}

func printTableDiffSummary(tblName string, tbl1, tbl2 *doltdb.Table) {
	bold := color.New(color.Bold)

	bold.Printf("diff --dolt a/%[1]s b/%[1]s\n", tblName)

	if tbl1 == nil {
		bold.Println("deleted table")
	} else if tbl2 == nil {
		bold.Println("added table")
	} else {
		h1, err := tbl1.HashOf()

		if err != nil {
			panic(err)
		}

		bold.Printf("--- a/%s @ %s\n", tblName, h1.String())

		h2, err := tbl2.HashOf()

		if err != nil {
			panic(err)
		}

		bold.Printf("+++ b/%s @ %s\n", tblName, h2.String())
	}
}

func diffSummary(ctx context.Context, v1, v2 types.Map, colLen int) errhand.VerboseError {
	ae := atomicerr.New()
	ch := make(chan diff.DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := diff.Summary(ctx, ch, v1, v2)

		ae.SetIfError(err)
	}()

	acc := diff.DiffSummaryProgress{}
	var count int64
	var pos int
	for p := range ch {
		if ae.IsSet() {
			break
		}

		acc.Adds += p.Adds
		acc.Removes += p.Removes
		acc.Changes += p.Changes
		acc.CellChanges += p.CellChanges
		acc.NewSize += p.NewSize
		acc.OldSize += p.OldSize

		if count%10000 == 0 {
			statusStr := fmt.Sprintf("prev size: %d, new size: %d, adds: %d, deletes: %d, modifications: %d", acc.OldSize, acc.NewSize, acc.Adds, acc.Removes, acc.Changes)
			pos = cli.DeleteAndPrint(pos, statusStr)
		}

		count++
	}

	pos = cli.DeleteAndPrint(pos, "")

	if err := ae.Get(); err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	if acc.NewSize > 0 || acc.OldSize > 0 {
		formatSummary(acc, colLen)
	} else {
		cli.Println("No data changes. See schema changes by using -s or --schema.")
	}

	return nil
}

func formatSummary(acc diff.DiffSummaryProgress, colLen int) {
	pluralize := func(singular, plural string, n uint64) string {
		var noun string
		if n != 1 {
			noun = plural
		} else {
			noun = singular
		}
		return fmt.Sprintf("%s %s", humanize.Comma(int64(n)), noun)
	}

	rowsUnmodified := uint64(acc.OldSize - acc.Changes - acc.Removes)
	unmodified := pluralize("Row Unmodified", "Rows Unmodified", rowsUnmodified)
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)
	changes := pluralize("Row Modified", "Rows Modified", acc.Changes)
	cellChanges := pluralize("Cell Modified", "Cells Modified", acc.CellChanges)

	oldValues := pluralize("Entry", "Entries", acc.OldSize)
	newValues := pluralize("Entry", "Entries", acc.NewSize)

	percentCellsChanged := float64(100*acc.CellChanges) / (float64(acc.OldSize) * float64(colLen))

	cli.Printf("%s (%.2f%%)\n", unmodified, (float64(100*rowsUnmodified) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", insertions, (float64(100*acc.Adds) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", deletions, (float64(100*acc.Removes) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", changes, (float64(100*acc.Changes) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", cellChanges, percentCellsChanged)
	cli.Printf("(%s vs %s)\n\n", oldValues, newValues)
}
