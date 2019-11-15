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

package diff

import (
	"context"
	"errors"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

type tableDiff struct {
	adds    []string
	drops   []string
	renames map[string]string
	same    []string
}

func SQLTableDIffs(ctx context.Context, r1, r2 *doltdb.RootValue) error {
	adds, _, drops, err := r1.TableDiff(ctx, r2)

	if err != nil {
		return err
	}

	adds, drops, renames, err := findRenames(ctx, r1, r2, adds, drops)

	if err != nil {
		return err
	}

	// rename tables
	for k, v := range renames {
		cli.Println("RENAME TABLE", sql.QuoteIdentifier(k), "TO", sql.QuoteIdentifier(v))
	}

	// drop tables
	for _, tblName := range drops {
		cli.Println("DROP TABLE", sql.QuoteIdentifier(tblName), ";")
	}

	// add tables
	for _, tblName := range adds {
		if tbl, ok, err := r1.GetTable(ctx, tblName); err != nil {
			return errors.New("error: unable to write SQL diff output for new table")
		} else if !ok {
			continue
		} else {
			if sch, err := tbl.GetSchema(ctx); err != nil {
				return errors.New("error unable to get schema for table " + tblName)
			} else {
				var b strings.Builder
				b.WriteString("CREATE TABLE ")
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
				err = p.Wait()
			}
		}
	}
	return err
}

func findRenames(ctx context.Context, r1, r2 *doltdb.RootValue, adds []string, drops []string) ([]string, []string, map[string]string, error) {

	renames := make(map[string]string, 0)
	for i, add := range adds {
		for j, drop := range drops {
			addHash, foundAdd, err := r1.GetTableHash(ctx, add)

			if err != nil {
				return nil, nil, nil, err
			}

			dropHash, foundDrop, err := r2.GetTableHash(ctx, drop)

			if err != nil {
				return nil, nil, nil, err
			}

			if foundAdd && foundDrop {
				if addHash.Equal(dropHash) {
					renames[drop] = add
					// mark tables as consumed
					adds[i] = ""
					drops[j] = ""
				}
			}
		}
	}

	outAdds := make([]string, 0)
	for _, tbl := range adds {
		if tbl != "" {
			outAdds = append(outAdds, tbl)
		}
	}

	outDrops := make([]string, 0)
	for _, tbl := range drops {
		if tbl != "" {
			outDrops = append(outDrops, tbl)
		}
	}

	return outAdds, outDrops, renames, nil
}
