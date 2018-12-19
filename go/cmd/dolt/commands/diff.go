package commands

import (
	"flag"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped/fwt"
	"os"
)

func diffUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Diff(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = diffUsage(fs)

	tblName := fs.String("table", "", "A table to show")

	fs.Parse(args)

	if *tblName == "" {
		fmt.Fprintln(os.Stderr, "Missing required parameter \"-table\"")
		return 1
	}

	l1, l2, r1, r2, verr := getRoots(fs.Args(), cliEnv)
	fmt.Printf("diffing %s vs %s\n\n", color.CyanString(l1), color.BlueString(l2))

	if verr == nil {
		verr = diffRoots(r1, r2, []string{*tblName}, cliEnv)
	}

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	return 0
}

func getRoots(args []string, cliEnv *env.DoltCLIEnv) (l1, l2 string, r1, r2 *doltdb.RootValue, verr errhand.VerboseError) {
	if len(args) > 2 {
		bdr := errhand.BuildDError("")
		return "", "", nil, nil, bdr.Build()
	}

	l1 = "working"
	l2 = "staged"
	if len(args) == 0 {
		var err error
		r1, err = cliEnv.WorkingRoot()

		if err != nil {
			verr = errhand.BuildDError("Unable to get working.").AddCause(err).Build()
		} else {
			r2, err = cliEnv.StagedRoot()

			if err != nil {
				verr = errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
			}
		}
	} else if len(args) == 1 {
		var err error
		r1, err = cliEnv.WorkingRoot()

		if err != nil {
			verr = errhand.BuildDError("Unable to get working").AddCause(err).Build()
		} else {
			l2, r2, verr = getRootForCommitSpecStr(args[0], cliEnv)
		}
	} else {
		l1, r1, verr = getRootForCommitSpecStr(args[0], cliEnv)

		if verr == nil {
			l2, r2, verr = getRootForCommitSpecStr(args[1], cliEnv)
		}
	}

	if verr != nil {
		return "", "", nil, nil, verr
	}

	return l1, l2, r1, r2, nil
}

func getRootForCommitSpecStr(csStr string, cliEnv *env.DoltCLIEnv) (string, *doltdb.RootValue, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(csStr, cliEnv.RepoState.Branch)

	if err != nil {
		bdr := errhand.BuildDError(`"%s" is not a validly formatted branch, or commit reference.`, csStr)
		return "", nil, bdr.AddCause(err).Build()
	}

	cm, err := cliEnv.DoltDB.Resolve(cs)

	if err != nil {
		return "", nil, errhand.BuildDError(`Unable to resolve "%s"`, csStr).AddCause(err).Build()
	}

	r := cm.GetRootValue()

	return cm.HashOf().String(), r, nil
}

func diffRoots(r1, r2 *doltdb.RootValue, tblNames []string, cliEnv *env.DoltCLIEnv) errhand.VerboseError {
	for _, tblName := range tblNames {
		tbl1, ok1 := r1.GetTable(tblName)
		tbl2, ok2 := r2.GetTable(tblName)

		if !ok1 && !ok2 {
			return errhand.BuildDError("").Build()
		}

		var sch1 *schema.Schema
		var sch2 *schema.Schema
		rowData1 := types.NewMap(cliEnv.DoltDB.ValueReadWriter())
		rowData2 := types.NewMap(cliEnv.DoltDB.ValueReadWriter())

		if ok1 {
			sch1 = tbl1.GetSchema(cliEnv.DoltDB.ValueReadWriter())
			rowData1 = tbl1.GetRowData()
		}

		if ok2 {
			sch2 = tbl2.GetSchema(cliEnv.DoltDB.ValueReadWriter())
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
	transforms := []table.TransformFunc{fwtTr.TransformToFWT, colorTr}

	wr := doltdb.NewColorDiffWriter(os.Stdout, unionedSch, " | ")
	defer wr.Close()

	var verr errhand.VerboseError
	badRowCB := func(transfName string, row *table.Row, errDetails string) (quit bool) {
		verr = errhand.BuildDError("Failed transforming row").AddDetails(transfName).AddDetails(errDetails).Build()
		return true
	}

	pipeline := table.StartAsyncPipeline(rd, transforms, wr, badRowCB)
	pipeline.Wait()

	return verr
}
