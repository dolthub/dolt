package commands

import (
	"fmt"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped/fwt"
	"os"
)

var diffShortDesc = "Show changes between commits, commit and working tree, etc"
var diffLongDesc = `Show changes between the working and staged tables, changes between the working tables and the tables within a commit, or changes between tables at two commits.

dolt diff [--options] [--] [<tables>...]
   This form is to view the changes you made relative to the staging area for the next commit. In other words, the differences are what you could tell Git to further add but you still haven't. You can stage these changes by using dolt add.

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
		fmt.Fprintln(os.Stderr, verr.Verbose())
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
			if cm, err := dEnv.DoltDB.Resolve(cs); err == nil {
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
		if !(roots[0].HasTable(tbl) || roots[1].HasTable(tbl)) {
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

	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return "", nil, errhand.BuildDError(`Unable to resolve "%s"`, csStr).AddCause(err).Build()
	}

	r := cm.GetRootValue()

	return cm.HashOf().String(), r, nil
}

func diffRoots(r1, r2 *doltdb.RootValue, tblNames []string, dEnv *env.DoltEnv) errhand.VerboseError {
	if len(tblNames) == 0 {
		tblNames = actions.AllTables(r1, r2)
	}

	for _, tblName := range tblNames {
		tbl1, ok1 := r1.GetTable(tblName)
		tbl2, ok2 := r2.GetTable(tblName)

		if !ok1 && !ok2 {
			bdr := errhand.BuildDError("Table could not be found.")
			bdr.AddDetails("The table %s does not exist.", tblName)
			fmt.Fprintln(os.Stderr, bdr.Build())
		} else if tbl1 != nil && tbl2 != nil && tbl1.HashOf() == tbl2.HashOf() {
			continue
		}

		printTableHeader(tblName, tbl1, tbl2)

		if tbl1 == nil || tbl2 == nil {
			continue
		}

		var sch1 *schema.Schema
		var sch2 *schema.Schema
		rowData1 := types.NewMap(dEnv.DoltDB.ValueReadWriter())
		rowData2 := types.NewMap(dEnv.DoltDB.ValueReadWriter())

		if ok1 {
			sch1 = tbl1.GetSchema(dEnv.DoltDB.ValueReadWriter())
			rowData1 = tbl1.GetRowData()
		}

		if ok2 {
			sch2 = tbl2.GetSchema(dEnv.DoltDB.ValueReadWriter())
			rowData2 = tbl2.GetRowData()
		}

		verr := diffRows(rowData1, rowData2, sch1, sch2)

		if verr != nil {
			return verr
		}
	}

	return nil
}

func diffRows(newRows, oldRows types.Map, newSch, oldSch *schema.Schema) errhand.VerboseError {
	unionedSch := untyped.UntypedSchemaUnion(newSch, oldSch)

	newToUnionConv := table.IdentityConverter
	if newSch != nil {
		newToUnionMapping, err := schema.NewInferredMapping(newSch, unionedSch)

		if err != nil {
			return errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		newToUnionConv, _ = table.NewRowConverter(newToUnionMapping)
	}

	oldToUnionConv := table.IdentityConverter
	if oldSch != nil {
		oldToUnionMapping, err := schema.NewInferredMapping(oldSch, unionedSch)

		if err != nil {
			return errhand.BuildDError("Error creating unioned mapping").AddCause(err).Build()
		}

		oldToUnionConv, _ = table.NewRowConverter(oldToUnionMapping)
	}

	ad := doltdb.NewAsyncDiffer(1024)
	ad.Start(newRows, oldRows)
	defer ad.Close()

	rd := doltdb.NewRowDiffReader(ad, oldToUnionConv, newToUnionConv, unionedSch)
	defer rd.Close()

	fwtTr := fwt.NewAutoSizingFWTTransformer(unionedSch, fwt.HashFillWhenTooLong, 1000)
	colorTr := table.NewRowTransformer("coloring transform", doltdb.ColoringTransform)
	transforms := table.NewTransformCollection(
		table.NamedTransform{"fwt", fwtTr.TransformToFWT},
		table.NamedTransform{"color", colorTr})

	wr := doltdb.NewColorDiffWriter(os.Stdout, unionedSch, " | ")
	defer wr.Close()

	var verr errhand.VerboseError
	badRowCB := func(transfName string, row *table.Row, errDetails string) (quit bool) {
		verr = errhand.BuildDError("Failed transforming row").AddDetails(transfName).AddDetails(errDetails).Build()
		return true
	}

	pipeline, start := table.NewAsyncPipeline(rd, transforms, wr, badRowCB)

	ch, _ := pipeline.GetInChForTransf("fwt")
	ch <- untyped.NewRowFromStrings(unionedSch, unionedSch.GetFieldNames())
	start()
	pipeline.Wait()

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
