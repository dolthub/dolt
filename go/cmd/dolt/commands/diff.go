package commands

import (
	"context"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var diffShortDesc = "Show changes between commits, commit and working tree, etc"
var diffLongDesc = `Show changes between the working and staged tables, changes between the working tables and the tables within a commit, or changes between tables at two commits.

dolt diff [--options] [--] [<tables>...]
   This form is to view the changes you made relative to the staging area for the next commit. In other words, the differences are what you could tell Dolt to further add but you still haven't. You can stage these changes by using dolt add.

dolt diff [--options] <commit> [--] [<tables>...]
   This form is to view the changes you have in your working tables relative to the named <commit>. You can use HEAD to compare it with the latest commit, or a branch name to compare with the tip of a different branch.

dolt diff [--options] <commit> <commit> [--] [<tables>...]
   This is to view the changes between two arbitrary <commit>.
`

var diffSynopsis = []string{
	"[options] [<commit>] [--] [<tables>...]",
	"[options] <commit> <commit> [--] [<tables>...]",
}

func Diff(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, _ := cli.HelpAndUsagePrinters(commandStr, diffShortDesc, diffLongDesc, diffSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	r1, r2, tables, verr := getRoots(apr.Args(), dEnv)

	if verr == nil {
		verr = diffRoots(r1, r2, tables, dEnv)
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
		if cs, err := doltdb.NewCommitSpec(arg, dEnv.RepoState.Branch); err == nil {
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
	cs, err := doltdb.NewCommitSpec(csStr, dEnv.RepoState.Branch)

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

func diffRoots(r1, r2 *doltdb.RootValue, tblNames []string, dEnv *env.DoltEnv) errhand.VerboseError {
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

		printTableHeader(tblName, tbl1, tbl2)

		if tbl1 == nil || tbl2 == nil {
			continue
		}

		var sch1 schema.Schema
		var sch2 schema.Schema
		rowData1 := types.NewMap(context.TODO(), dEnv.DoltDB.ValueReadWriter())
		rowData2 := types.NewMap(context.TODO(), dEnv.DoltDB.ValueReadWriter())

		if ok1 {
			sch1 = tbl1.GetSchema(context.TODO())
			rowData1 = tbl1.GetRowData(context.TODO())
		}

		if ok2 {
			sch2 = tbl2.GetSchema(context.TODO())
			rowData2 = tbl2.GetRowData(context.TODO())
		}

		verr := diffRows(rowData1, rowData2, sch1, sch2)

		if verr != nil {
			return verr
		}
	}

	return nil
}

func diffRows(newRows, oldRows types.Map, newSch, oldSch schema.Schema) errhand.VerboseError {
	untypedUnionSch, err := untyped.UntypedSchemaUnion(newSch, oldSch)

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
	ad.Start(context.TODO(), newRows, oldRows)
	defer ad.Close()

	src := diff.NewRowDiffSource(ad, oldToUnionConv, newToUnionConv, untypedUnionSch)
	defer src.Close()

	fwtTr := fwt.NewAutoSizingFWTTransformer(untypedUnionSch, fwt.HashFillWhenTooLong, 1000)
	transforms := pipeline.NewTransformCollection(
		pipeline.NamedTransform{"fwt", fwtTr.TransformToFWT},
		pipeline.NewNamedTransform("color", diff.ColoringTransform))

	sink := diff.NewColorDiffWriter(cli.CliOut, untypedUnionSch, " | ")
	defer sink.Close()

	var verr errhand.VerboseError
	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		verr = errhand.BuildDError("Failed transforming row").AddDetails(trf.TransformName).AddDetails(trf.Details).Build()
		return true
	}

	srcProcFunc := pipeline.ProcFuncForSourceFunc(src.NextDiff)
	sinkProcFunc := pipeline.ProcFuncForSinkFunc(sink.ProcRowWithProps)
	p := pipeline.NewAsyncPipeline(srcProcFunc, sinkProcFunc, transforms, badRowCB)

	colNames := schema.ExtractAllColNames(untypedUnionSch)
	p.InjectRow("fwt", untyped.NewRowFromTaggedStrings(untypedUnionSch, colNames))
	p.Start()
	p.Wait()

	return verr
}

var emptyHash = hash.Hash{}

func printTableHeader(tblName string, tbl1, tbl2 *doltdb.Table) {
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
