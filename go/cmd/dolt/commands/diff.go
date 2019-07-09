package commands

import (
	"context"
	"reflect"
	"sort"
	"strconv"

	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/mathutil"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

const (
	SchemaOnlyDiff    = 1
	DataOnlyDiff      = 2
	SchemaAndDataDiff = SchemaOnlyDiff | DataOnlyDiff

	DataFlag   = "data"
	SchemaFlag = "schema"
)

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

func Diff(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(DataFlag, "d", "Show only the data changes, do not show the schema changes (Both shown by default).")
	ap.SupportsFlag(SchemaFlag, "s", "Show only the schema changes, do not show the data changes (Both shown by default).")
	help, _ := cli.HelpAndUsagePrinters(commandStr, diffShortDesc, diffLongDesc, diffSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	diffParts := SchemaAndDataDiff
	if apr.Contains(DataFlag) && !apr.Contains(SchemaFlag) {
		diffParts = DataOnlyDiff
	} else if apr.Contains(SchemaFlag) && !apr.Contains(DataFlag) {
		diffParts = SchemaOnlyDiff
	}

	r1, r2, tables, verr := getRoots(apr.Args(), dEnv)

	if verr == nil {
		verr = diffRoots(r1, r2, tables, diffParts, dEnv)
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

// this doesnt work correctly.  Need to be able to distinguish commits from tables
func getRoots(args []string, dEnv *env.DoltEnv) (r1, r2 *doltdb.RootValue, tables []string, verr errhand.VerboseError) {
	roots := make([]*doltdb.RootValue, 2)

	i := 0
	for _, arg := range args {
		if cs, err := doltdb.NewCommitSpec(arg, dEnv.RepoState.Head.Ref.String()); err == nil {
			if cm, err := dEnv.DoltDB.Resolve(context.TODO(), cs); err == nil {
				roots[i] = cm.GetRootValue()
				i++
				continue
			}
		}

		break
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
		if !(roots[0].HasTable(context.TODO(), tbl) || roots[1].HasTable(context.TODO(), tbl)) {
			verr := errhand.BuildDError("error: Unknown table: '%s'", tbl).Build()
			return nil, nil, args, verr
		}

		tables = append(tables, tbl)
	}

	return roots[0], roots[1], tables, nil
}

func getRootForCommitSpecStr(csStr string, dEnv *env.DoltEnv) (string, *doltdb.RootValue, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(csStr, dEnv.RepoState.Head.Ref.String())

	if err != nil {
		bdr := errhand.BuildDError(`"%s" is not a validly formatted branch, or commit reference.`, csStr)
		return "", nil, bdr.AddCause(err).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(context.TODO(), cs)

	if err != nil {
		return "", nil, errhand.BuildDError(`Unable to resolve "%s"`, csStr).AddCause(err).Build()
	}

	r := cm.GetRootValue()

	return cm.HashOf().String(), r, nil
}

func diffRoots(r1, r2 *doltdb.RootValue, tblNames []string, diffParts int, dEnv *env.DoltEnv) errhand.VerboseError {
	if len(tblNames) == 0 {
		tblNames = actions.AllTables(context.TODO(), r1, r2)
	}

	for _, tblName := range tblNames {
		tbl1, ok1 := r1.GetTable(context.TODO(), tblName)
		tbl2, ok2 := r2.GetTable(context.TODO(), tblName)

		if !ok1 && !ok2 {
			bdr := errhand.BuildDError("Table could not be found.")
			bdr.AddDetails("The table %s does not exist.", tblName)
			cli.PrintErrln(bdr.Build())
		} else if tbl1 != nil && tbl2 != nil && tbl1.HashOf() == tbl2.HashOf() {
			continue
		}

		printTableDiffSummary(tblName, tbl1, tbl2)

		if tbl1 == nil || tbl2 == nil {
			continue
		}

		var sch1 schema.Schema
		var sch2 schema.Schema
		var sch1Hash hash.Hash
		var sch2Hash hash.Hash
		rowData1 := types.NewMap(context.TODO(), dEnv.DoltDB.ValueReadWriter())
		rowData2 := types.NewMap(context.TODO(), dEnv.DoltDB.ValueReadWriter())

		if ok1 {
			sch1 = tbl1.GetSchema(context.TODO())
			sch1Hash = tbl1.GetSchemaRef().TargetHash()
			rowData1 = tbl1.GetRowData(context.TODO())
		}

		if ok2 {
			sch2 = tbl2.GetSchema(context.TODO())
			sch2Hash = tbl2.GetSchemaRef().TargetHash()
			rowData2 = tbl2.GetRowData(context.TODO())
		}

		var verr errhand.VerboseError

		if diffParts&SchemaOnlyDiff != 0 && sch1Hash != sch2Hash {
			verr = diffSchemas(tblName, sch2, sch1)
		}

		if diffParts&DataOnlyDiff != 0 {
			verr = diffRows(rowData1, rowData2, sch1, sch2)
		}

		if verr != nil {
			return verr
		}
	}

	return nil
}

func diffSchemas(tableName string, sch1 schema.Schema, sch2 schema.Schema) errhand.VerboseError {
	diffs := diff.DiffSchemas(sch1, sch2)
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
			n0, t0 := dff.Old.Name, sql.DoltToSQLType[dff.Old.Kind]
			n1, t1 := dff.New.Name, sql.DoltToSQLType[dff.New.Kind]

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

func dumbDownSchema(in schema.Schema) schema.Schema {
	allCols := in.GetAllCols()

	dumbCols := make([]schema.Column, 0, allCols.Size())
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		col.Name = strconv.FormatUint(tag, 10)
		col.Constraints = nil
		dumbCols = append(dumbCols, col)

		return false
	})

	dumbColColl, _ := schema.NewColCollection(dumbCols...)

	return schema.SchemaFromCols(dumbColColl)
}

func diffRows(newRows, oldRows types.Map, newSch, oldSch schema.Schema) errhand.VerboseError {
	dumbNewSch := dumbDownSchema(newSch)
	dumbOldSch := dumbDownSchema(oldSch)

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
	ad.Start(context.TODO(), newRows.Format(), newRows, oldRows)
	defer ad.Close()

	src := diff.NewRowDiffSource(ad, oldToUnionConv, newToUnionConv, untypedUnionSch)
	defer src.Close()

	oldColNames := make(map[uint64]string)
	newColNames := make(map[uint64]string)
	untypedUnionSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
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

		return false
	})

	schemasEqual := reflect.DeepEqual(oldColNames, newColNames)
	numHeaderRows := 1
	if !schemasEqual {
		numHeaderRows = 2
	}

	sink := diff.NewColorDiffSink(iohelp.NopWrCloser(cli.CliOut), untypedUnionSch, numHeaderRows)
	defer sink.Close()

	fwtTr := fwt.NewAutoSizingFWTTransformer(untypedUnionSch, fwt.HashFillWhenTooLong, 1000)
	nullPrinter := nullprinter.NewNullPrinter(untypedUnionSch)
	transforms := pipeline.NewTransformCollection(
		pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow),
		pipeline.NamedTransform{fwtStageName, fwtTr.TransformToFWT},
	)

	var verr errhand.VerboseError
	badRowCallback := func(trf *pipeline.TransformRowFailure) (quit bool) {
		verr = errhand.BuildDError("Failed transforming row").AddDetails(trf.TransformName).AddDetails(trf.Details).Build()
		return true
	}

	sinkProcFunc := pipeline.ProcFuncForSinkFunc(sink.ProcRowWithProps)
	p := pipeline.NewAsyncPipeline(pipeline.ProcFuncForSourceFunc(src.NextDiff), sinkProcFunc, transforms, badRowCallback)

	if schemasEqual {
		p.InjectRow(fwtStageName, untyped.NewRowFromTaggedStrings(newRows.Format(), untypedUnionSch, newColNames))
	} else {
		p.InjectRowWithProps(fwtStageName, untyped.NewRowFromTaggedStrings(newRows.Format(), untypedUnionSch, oldColNames), map[string]interface{}{diff.DiffTypeProp: diff.DiffModifiedOld})
		p.InjectRowWithProps(fwtStageName, untyped.NewRowFromTaggedStrings(newRows.Format(), untypedUnionSch, newColNames), map[string]interface{}{diff.DiffTypeProp: diff.DiffModifiedNew})
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
		bold.Printf("--- a/%s @ %s\n", tblName, tbl1.HashOf().String())
		bold.Printf("+++ b/%s @ %s\n", tblName, tbl2.HashOf().String())
	}
}
