package diff

import (
	"errors"
	"strings"
	"context"

	"github.com/fatih/color"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

type tableDiff struct {
	adds    []string
	drops   []string
	renames map[string]string
	same    []string
}

func SQLTableDIffs(ctx context.Context, r1, r2 *doltdb.RootValue) error {
	tblDiff, err := tableDiffs(ctx, r1, r2)

	if err != nil {
		return errors.New("error: unable to read tables")
	}

	//rename tables
	for k, v := range tblDiff.renames {
		cli.Println("RENAME TABLE",sql.QuoteIdentifier(k),"TO",sql.QuoteIdentifier(v))
	}

	// drop tables
	for _, tblName := range tblDiff.drops {
		cli.Println("DROP TABLE", sql.QuoteIdentifier(tblName))
	}

	// add tables
	for _, tblName := range tblDiff.adds {
		if tbl, ok, err := r1.GetTable(ctx, tblName); err != nil {
			return errors.New("error: unable to write SQL diff output for new table")
		} else if !ok {
			continue
		} else {
			if sch, err := tbl.GetSchema(ctx); err != nil {
				return errors.New("error unable to get schema for table " + tblName)
			} else {
				var b strings.Builder
				b.WriteString("CREATE TABLE")
				b.WriteString(sql.QuoteIdentifier(tblName))
				b.WriteString("(\n")
				for _, col := range sch.GetAllCols().GetColumns() {
					b.WriteString(sql.FmtCol(4, 0, 0, col))
					b.WriteString(",\n")
				}
				seenOne := false
				b.WriteString("\tPRIMARY KEY (")
				for _, col := range sch.GetAllCols().GetColumns() {
					if seenOne {
						b.WriteString(",")
					}
					if col.IsPartOfPK {
						b.WriteString(sql.QuoteIdentifier(col.Name))
					}
				}
				b.WriteString(")")
				b.WriteString("\n  );")
				cli.Println(b.String())

				// Insert all rows
				transforms := pipeline.NewTransformCollection()
				nullPrinter := nullprinter.NewNullPrinter(sch)
				transforms.AppendTransforms(
					pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow),
				)
				sink, err := NewSQLDiffSink(iohelp.NopWrCloser(cli.CliOut), sch, tblName)
				if err != nil {
					return errors.New("error: unable to create SQL diff sink")
				}

				rowData, err := tbl.GetRowData(ctx)

				if err != nil {
					return errors.New("error: unable to get row data")
				}

				rd, err := noms.NewNomsMapReader(ctx, rowData, sch)

				if err != nil {
					return errors.New("error: unable to create map reader")
				}

				badRowCallback := func(tff *pipeline.TransformRowFailure) (quit bool) {
					cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(ctx, tff.Row, sch)))
					return true
				}

				rdProcFunc := pipeline.ProcFuncForReader(ctx, rd)

				sinkProcFunc := pipeline.ProcFuncForSinkFunc(sink.ProcRowForExport)
				p := pipeline.NewAsyncPipeline(rdProcFunc, sinkProcFunc, transforms, badRowCallback)
				p.Start()
			}
		}
	}
	return nil
}

func tableDiffs(ctx context.Context, r1, r2 *doltdb.RootValue) (*tableDiff, error) {

	hashToName := func(ctx context.Context, r *doltdb.RootValue) (map[hash.Hash]string, []hash.Hash, error) {
		tblNames, err := r.GetTableNames(ctx)

		if err != nil {
			return nil, nil, err
		}

		hashToName := make(map[hash.Hash]string)
		hashes := make([]hash.Hash, 0)
		for _, name := range tblNames {
			tblHash, found, err := r.GetTableHash(ctx, name)

			if err != nil {
				return nil, nil, err
			}

			if !found {
				return nil, nil, errors.New("can't find table name in root, even tho that's where we got it")
			}

			hashes = append(hashes, tblHash)
			hashToName[tblHash] = name
		}

		return hashToName, hashes, nil
	}

	hashToOldName, oldTblHashes, err := hashToName(ctx, r2)

	if err != nil {
		return nil, err
	}

	hashToNewName, newTblHashes, err := hashToName(ctx, r1)

	if err != nil {
		return nil, err
	}

	same := make([]string, 0)
	renames := make(map[string]string)
	for _, newHash := range newTblHashes {
		for _, oldHash := range oldTblHashes {
			if hashToNewName[newHash] == hashToOldName[oldHash] {
				// assume it's the same table
				// DROP TABLE, ADD TABLE with the same name will
				// be interpreted as a schema change for the table
				same = append(same, hashToNewName[newHash])
				// mark names as consumed
				hashToNewName[newHash] = ""
				hashToOldName[oldHash] = ""
				break
			}
			// This only works if tables are not changed. Renaming
			// tables with changes will result in a DROP and ADD
			if newHash.Equal(oldHash) && hashToNewName[newHash] != hashToOldName[oldHash] {
				renames[hashToOldName[oldHash]] = hashToNewName[newHash]
				// mark names as consumed
				hashToNewName[newHash] = ""
				hashToOldName[oldHash] = ""
				break
			}
		}
	}

	drops := make([]string, 0)
	for _, oldHash := range oldTblHashes {
		if hashToOldName[oldHash] != "" {
			drops = append(drops, hashToOldName[oldHash])
		}
	}

	adds := make([]string, 0)
	for _, newHash := range newTblHashes {
		if hashToNewName[newHash] != "" {
			adds = append(adds, hashToNewName[newHash])
		}
	}

	return &tableDiff{ adds, drops, renames, same}, nil
}