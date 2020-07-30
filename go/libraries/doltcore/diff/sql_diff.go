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
	"io"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type SQLDiffSink struct {
	wr        io.WriteCloser
	sch       schema.Schema
	tableName string
}

// NewSQLDiffSink creates a SQLDiffSink for a diff pipeline.
func NewSQLDiffSink(wr io.WriteCloser, sch schema.Schema, tableName string) (*SQLDiffSink, error) {
	return &SQLDiffSink{wr, sch, tableName}, nil
}

// GetSchema gets the schema that the SQLDiffSink was created with.
func (sds *SQLDiffSink) GetSchema() schema.Schema {
	return sds.sch
}

// ProcRowWithProps satisfies pipeline.SinkFunc; it writes SQL diff statements to output.
func (sds *SQLDiffSink) ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error {

	taggedVals := make(row.TaggedValues)
	allCols := sds.sch.GetAllCols()
	colDiffs := make(map[string]DiffChType)

	if prop, ok := props.Get(CollChangesProp); ok {
		if convertedVal, convertedOK := prop.(map[string]DiffChType); convertedOK {
			colDiffs = convertedVal
		}
	}

	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if val, ok := r.GetColVal(tag); ok {
			taggedVals[tag] = val
		}
		return false, nil
	})

	if err != nil {
		return err
	}

	r, err = row.New(r.Format(), sds.sch, taggedVals)

	if err != nil {
		return err
	}

	taggedVals[diffColTag] = types.String("   ")
	if prop, ok := props.Get(DiffTypeProp); ok {
		if dt, convertedOK := prop.(DiffChType); convertedOK {
			switch dt {
			case DiffAdded:
				stmt, err := sqlfmt.RowAsInsertStmt(r, sds.tableName, sds.sch)

				if err != nil {
					return err
				}

				return iohelp.WriteLine(sds.wr, stmt)
			case DiffRemoved:
				stmt, err := sqlfmt.RowAsDeleteStmt(r, sds.tableName, sds.sch)

				if err != nil {
					return err
				}

				return iohelp.WriteLine(sds.wr, stmt)
			case DiffModifiedOld:
				return nil
			case DiffModifiedNew:
				// TODO: minimize update statement to modified rows
				stmt, err := sqlfmt.RowAsUpdateStmt(r, sds.tableName, sds.sch)

				if err != nil {
					return err
				}

				return iohelp.WriteLine(sds.wr, stmt)
			}
			// Treat the diff indicator string as a diff of the same type
			colDiffs[diffColName] = dt
		}
	}

	return err
}

// ProcRowWithProps satisfies pipeline.SinkFunc; it writes rows as SQL statements.
func (sds *SQLDiffSink) ProcRowForExport(r row.Row, _ pipeline.ReadableMap) error {
	stmt, err := sqlfmt.RowAsInsertStmt(r, sds.tableName, sds.sch)

	if err != nil {
		return err
	}

	return iohelp.WriteLine(sds.wr, stmt)
}

// Close should release resources being held
func (sds *SQLDiffSink) Close() error {
	if sds.wr != nil {
		err := sds.wr.Close()
		if err != nil {
			return err
		}
		sds.wr = nil
		return nil
	} else {
		return errors.New("Already closed.")
	}
}

// PrintSqlTableDiffs writes diffs of table definitions to output.
func PrintSqlTableDiffs(ctx context.Context, r1, r2 *doltdb.RootValue, wr io.WriteCloser) error {
	creates, _, drops, err := r1.TableDiff(ctx, r2)

	if err != nil {
		return err
	}

	creates, drops, renames, err := findRenames(ctx, r1, r2, creates, drops)

	if err != nil {
		return err
	}

	for k, v := range renames {
		if err = iohelp.WriteLine(wr, sqlfmt.RenameTableStmt(k, v)); err != nil {
			return err
		}
	}

	for _, tblName := range drops {
		if err = iohelp.WriteLine(wr, sqlfmt.DropTableStmt(tblName)); err != nil {
			return err
		}
	}

	// create tables and insert rows
	for _, tblName := range creates {
		if tbl, ok, err := r1.GetTable(ctx, tblName); err != nil {
			return errors.New("error: unable to write SQL diff output for new table")
		} else if !ok {
			continue
		} else {
			if sch, err := tbl.GetSchema(ctx); err != nil {
				return errors.New("error unable to get schema for table " + tblName)
			} else {
				fkc, err := r1.GetForeignKeyCollection(ctx)
				if err != nil {
					return errhand.BuildDError("error: failed to read foreign key struct").AddCause(err).Build()
				}
				declaresFk, _ := fkc.KeysForTable(tblName)
				stmt := sqlfmt.CreateTableStmtWithTags(tblName, sch, declaresFk, nil)
				if err = iohelp.WriteLine(wr, stmt); err != nil {
					return err
				}

				// Insert all rows
				transforms := pipeline.NewTransformCollection()
				nullPrinter := nullprinter.NewNullPrinter(sch)
				transforms.AppendTransforms(
					pipeline.NewNamedTransform(nullprinter.NullPrintingStage, nullPrinter.ProcessRow),
				)
				sink, err := NewSQLDiffSink(wr, sch, tblName)
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
					_ = iohelp.WriteLine(wr, color.RedString("error: failed to transform row %s.", row.Fmt(ctx, tff.Row, sch)))
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

func findRenames(ctx context.Context, r1, r2 *doltdb.RootValue, adds []string, drops []string) (added, dropped []string, renamed map[string]string, err error) {

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
