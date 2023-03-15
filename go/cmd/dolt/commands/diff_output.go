// Copyright 2022 Dolthub, Inc.
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
	ejson "encoding/json"
	"fmt"
	"io"

	textdiff "github.com/andreyvit/diff"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/atomicerr"
)

// diffWriter is an interface that lets us write diffs in a variety of output formats
type diffWriter interface {
	// BeginTable is called when a new table is about to be written, before any schema or row diffs are written
	BeginTable(ctx context.Context, td diff.TableDelta) error
	// WriteTableSchemaDiff is called to write a schema diff for the table given (if requested by args)
	WriteTableSchemaDiff(ctx context.Context, toRoot *doltdb.RootValue, td diff.TableDelta) error
	// WriteTriggerDiff is called to write a trigger diff
	WriteTriggerDiff(ctx context.Context, triggerName, oldDefn, newDefn string) error
	// WriteViewDiff is called to write a view diff
	WriteViewDiff(ctx context.Context, viewName, oldDefn, newDefn string) error
	// RowWriter returns a row writer for the table delta provided, which will have Close() called on it when rows are
	// done being written.
	RowWriter(ctx context.Context, td diff.TableDelta, unionSch sql.Schema) (diff.SqlRowDiffWriter, error)
	// Close finalizes the work of the writer
	Close(ctx context.Context) error
}

// newDiffWriter returns a diffWriter for the output format given
func newDiffWriter(diffOutput diffOutput) (diffWriter, error) {
	switch diffOutput {
	case TabularDiffOutput:
		return tabularDiffWriter{}, nil
	case SQLDiffOutput:
		return sqlDiffWriter{}, nil
	case JsonDiffOutput:
		return newJsonDiffWriter(iohelp.NopWrCloser(cli.CliOut))
	default:
		panic(fmt.Sprintf("unexpected diff output: %v", diffOutput))
	}
}

func printDiffStat(ctx context.Context, td diff.TableDelta, oldColLen, newColLen int) errhand.VerboseError {
	// todo: use errgroup.Group
	ae := atomicerr.New()
	ch := make(chan diff.DiffStatProgress)
	go func() {
		defer close(ch)
		err := diff.StatForTableDelta(ctx, ch, td)

		ae.SetIfError(err)
	}()

	acc := diff.DiffStatProgress{}
	var count int64
	var pos int
	eP := cli.NewEphemeralPrinter()
	for p := range ch {
		if ae.IsSet() {
			break
		}

		acc.Adds += p.Adds
		acc.Removes += p.Removes
		acc.Changes += p.Changes
		acc.CellChanges += p.CellChanges
		acc.NewRowSize += p.NewRowSize
		acc.OldRowSize += p.OldRowSize
		acc.NewCellSize += p.NewCellSize
		acc.OldCellSize += p.OldCellSize

		if count%10000 == 0 {
			eP.Printf("prev size: %d, new size: %d, adds: %d, deletes: %d, modifications: %d\n", acc.OldRowSize, acc.NewRowSize, acc.Adds, acc.Removes, acc.Changes)
			eP.Display()
		}

		count++
	}

	pos = cli.DeleteAndPrint(pos, "")

	if err := ae.Get(); err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	keyless, err := td.IsKeyless(ctx)
	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	if (acc.Adds+acc.Removes+acc.Changes) == 0 && (acc.OldCellSize-acc.NewCellSize) == 0 {
		cli.Println("No data changes. See schema changes by using -s or --schema.")
		return nil
	}

	if keyless {
		printKeylessStat(acc)
	} else {
		printStat(acc, oldColLen, newColLen)
	}

	return nil
}

func printStat(acc diff.DiffStatProgress, oldColLen, newColLen int) {
	numCellInserts, numCellDeletes := sqle.GetCellsAddedAndDeleted(acc, newColLen)
	rowsUnmodified := uint64(acc.OldRowSize - acc.Changes - acc.Removes)
	unmodified := pluralize("Row Unmodified", "Rows Unmodified", rowsUnmodified)
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)
	changes := pluralize("Row Modified", "Rows Modified", acc.Changes)
	cellInsertions := pluralize("Cell Added", "Cells Added", numCellInserts)
	cellDeletions := pluralize("Cell Deleted", "Cells Deleted", numCellDeletes)
	cellChanges := pluralize("Cell Modified", "Cells Modified", acc.CellChanges)

	oldValues := pluralize("Row Entry", "Row Entries", acc.OldRowSize)
	newValues := pluralize("Row Entry", "Row Entries", acc.NewRowSize)

	percentCellsChanged := float64(100*acc.CellChanges) / (float64(acc.OldRowSize) * float64(oldColLen))

	safePercent := func(num, dom uint64) float64 {
		// returns +Inf for x/0 where x > 0
		if num == 0 {
			return float64(0)
		}
		return float64(100*num) / (float64(dom))
	}

	cli.Printf("%s (%.2f%%)\n", unmodified, safePercent(rowsUnmodified, acc.OldRowSize))
	cli.Printf("%s (%.2f%%)\n", insertions, safePercent(acc.Adds, acc.OldRowSize))
	cli.Printf("%s (%.2f%%)\n", deletions, safePercent(acc.Removes, acc.OldRowSize))
	cli.Printf("%s (%.2f%%)\n", changes, safePercent(acc.Changes, acc.OldRowSize))
	cli.Printf("%s (%.2f%%)\n", cellInsertions, safePercent(numCellInserts, acc.OldCellSize))
	cli.Printf("%s (%.2f%%)\n", cellDeletions, safePercent(numCellDeletes, acc.OldCellSize))
	cli.Printf("%s (%.2f%%)\n", cellChanges, percentCellsChanged)
	cli.Printf("(%s vs %s)\n\n", oldValues, newValues)
}

func printKeylessStat(acc diff.DiffStatProgress) {
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)

	cli.Printf("%s\n", insertions)
	cli.Printf("%s\n", deletions)
}

func pluralize(singular, plural string, n uint64) string {
	var noun string
	if n != 1 {
		noun = plural
	} else {
		noun = singular
	}
	return fmt.Sprintf("%s %s", humanize.Comma(int64(n)), noun)
}

type tabularDiffWriter struct{}

var _ diffWriter = (*tabularDiffWriter)(nil)

func (t tabularDiffWriter) Close(ctx context.Context) error {
	return nil
}

func (t tabularDiffWriter) BeginTable(ctx context.Context, td diff.TableDelta) error {
	bold := color.New(color.Bold)
	if td.IsDrop() {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", td.FromName, td.FromName)
		_, _ = bold.Println("deleted table")
	} else if td.IsAdd() {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", td.ToName, td.ToName)
		_, _ = bold.Println("added table")
	} else {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", td.FromName, td.ToName)
		h1, err := td.FromTable.HashOf()

		if err != nil {
			panic(err)
		}

		_, _ = bold.Printf("--- a/%s @ %s\n", td.FromName, h1.String())

		h2, err := td.ToTable.HashOf()

		if err != nil {
			panic(err)
		}

		_, _ = bold.Printf("+++ b/%s @ %s\n", td.ToName, h2.String())
	}
	return nil
}

func (t tabularDiffWriter) WriteTableSchemaDiff(ctx context.Context, toRoot *doltdb.RootValue, td diff.TableDelta) error {
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	var fromCreateStmt = ""
	if td.FromTable != nil {
		// TODO: use UserSpaceDatabase for these, no reason for this separate database implementation
		sqlDb := sqle.NewSingleTableDatabase(td.FromName, fromSch, td.FromFks, td.FromFksParentSch)
		sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)
		fromCreateStmt, err = sqle.GetCreateTableStmt(sqlCtx, engine, td.FromName)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	var toCreateStmt = ""
	if td.ToTable != nil {
		sqlDb := sqle.NewSingleTableDatabase(td.ToName, toSch, td.ToFks, td.ToFksParentSch)
		sqlCtx, engine, _ := sqle.PrepareCreateTableStmt(ctx, sqlDb)
		toCreateStmt, err = sqle.GetCreateTableStmt(sqlCtx, engine, td.ToName)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	}

	if fromCreateStmt != toCreateStmt {
		cli.Println(textdiff.LineDiff(fromCreateStmt, toCreateStmt))
	}

	resolvedFromFks := map[string]struct{}{}
	for _, fk := range td.FromFks {
		if len(fk.ReferencedTableColumns) > 0 {
			resolvedFromFks[fk.Name] = struct{}{}
		}
	}

	for _, fk := range td.ToFks {
		if _, ok := resolvedFromFks[fk.Name]; ok {
			continue
		}
		if len(fk.ReferencedTableColumns) > 0 {
			cli.Println(fmt.Sprintf("resolved foreign key `%s` on table `%s`", fk.Name, fk.TableName))
		}
	}

	return nil
}

func (t tabularDiffWriter) WriteTriggerDiff(ctx context.Context, triggerName, oldDefn, newDefn string) error {
	// identical implementation
	return t.WriteViewDiff(ctx, triggerName, oldDefn, newDefn)
}

func (t tabularDiffWriter) WriteViewDiff(ctx context.Context, viewName, oldDefn, newDefn string) error {
	diffString := textdiff.LineDiff(oldDefn, newDefn)
	cli.Println(diffString)
	return nil
}

func (t tabularDiffWriter) RowWriter(ctx context.Context, td diff.TableDelta, unionSch sql.Schema) (diff.SqlRowDiffWriter, error) {
	return tabular.NewFixedWidthDiffTableWriter(unionSch, iohelp.NopWrCloser(cli.CliOut), 100), nil
}

type sqlDiffWriter struct{}

var _ diffWriter = (*tabularDiffWriter)(nil)

func (s sqlDiffWriter) Close(ctx context.Context) error {
	return nil
}

func (s sqlDiffWriter) BeginTable(ctx context.Context, td diff.TableDelta) error {
	return nil
}

func (s sqlDiffWriter) WriteTableSchemaDiff(ctx context.Context, toRoot *doltdb.RootValue, td diff.TableDelta) error {
	toSchemas, err := toRoot.GetAllSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("could not read schemas from toRoot").AddCause(err).Build()
	}

	ddlStatements, err := diff.SqlSchemaDiff(ctx, td, toSchemas)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	for _, stmt := range ddlStatements {
		cli.Println(stmt)
	}

	return nil
}

func (s sqlDiffWriter) WriteTriggerDiff(ctx context.Context, triggerName, oldDefn, newDefn string) error {
	// definitions will already be semicolon terminated, no need to add additional ones
	if oldDefn == "" {
		cli.Println(newDefn)
	} else if newDefn == "" {
		cli.Println(fmt.Sprintf("DROP TRIGGER %s;", sql.QuoteIdentifier(triggerName)))
	} else {
		cli.Println(fmt.Sprintf("DROP TRIGGER %s;", sql.QuoteIdentifier(triggerName)))
		cli.Println(newDefn)
	}

	return nil
}

func (s sqlDiffWriter) WriteViewDiff(ctx context.Context, viewName, oldDefn, newDefn string) error {
	// definitions will already be semicolon terminated, no need to add additional ones
	if oldDefn == "" {
		cli.Println(newDefn)
	} else if newDefn == "" {
		cli.Println(fmt.Sprintf("DROP VIEW %s;", sql.QuoteIdentifier(viewName)))
	} else {
		cli.Println(fmt.Sprintf("DROP VIEW %s;", sql.QuoteIdentifier(viewName)))
		cli.Println(newDefn)
	}

	return nil
}

func (s sqlDiffWriter) RowWriter(ctx context.Context, td diff.TableDelta, unionSch sql.Schema) (diff.SqlRowDiffWriter, error) {
	targetSch := td.ToSch
	if targetSch == nil {
		targetSch = td.FromSch
	}

	return sqlexport.NewSqlDiffWriter(td.ToName, targetSch, iohelp.NopWrCloser(cli.CliOut)), nil
}

type jsonDiffWriter struct {
	wr               io.WriteCloser
	schemaDiffWriter diff.SchemaDiffWriter
	rowDiffWriter    diff.SqlRowDiffWriter
	tablesWritten    int
	triggersWritten  int
	viewsWritten     int
}

var _ diffWriter = (*tabularDiffWriter)(nil)

func newJsonDiffWriter(wr io.WriteCloser) (*jsonDiffWriter, error) {
	return &jsonDiffWriter{
		wr: wr,
	}, nil
}

const tablesHeader = `"tables":[`
const jsonDiffTableHeader = `{"name":"%s","schema_diff":`
const jsonDiffDataDiffHeader = `],"data_diff":[`
const jsonDataDiffFooter = `}]`

func (j *jsonDiffWriter) beginDocumentIfNecessary() error {
	if j.tablesWritten == 0 && j.triggersWritten == 0 && j.viewsWritten == 0 {
		_, err := j.wr.Write([]byte("{"))
		return err
	}
	return nil
}

func (j *jsonDiffWriter) BeginTable(ctx context.Context, td diff.TableDelta) error {
	err := j.beginDocumentIfNecessary()
	if err != nil {
		return err
	}

	if j.tablesWritten == 0 {
		err := iohelp.WriteAll(j.wr, []byte(tablesHeader))
		if err != nil {
			return err
		}
	} else {
		err := iohelp.WriteAll(j.wr, []byte(`},`))
		if err != nil {
			return err
		}
	}

	tableName := td.FromName
	if len(tableName) == 0 {
		tableName = td.ToName
	}

	err = iohelp.WriteAll(j.wr, []byte(fmt.Sprintf(jsonDiffTableHeader, tableName)))
	if err != nil {
		return err
	}

	j.tablesWritten++

	j.schemaDiffWriter, err = json.NewSchemaDiffWriter(iohelp.NopWrCloser(j.wr))
	return err
}

func (j *jsonDiffWriter) WriteTableSchemaDiff(ctx context.Context, toRoot *doltdb.RootValue, td diff.TableDelta) error {
	toSchemas, err := toRoot.GetAllSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("could not read schemas from toRoot").AddCause(err).Build()
	}

	stmts, err := diff.SqlSchemaDiff(ctx, td, toSchemas)
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		err := j.schemaDiffWriter.WriteSchemaDiff(ctx, stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *jsonDiffWriter) RowWriter(ctx context.Context, td diff.TableDelta, unionSch sql.Schema) (diff.SqlRowDiffWriter, error) {
	// close off the schema diff block, start the data block
	err := iohelp.WriteAll(j.wr, []byte(jsonDiffDataDiffHeader))
	if err != nil {
		return nil, err
	}

	// Translate the union schema to its dolt version
	cols := schema.NewColCollection()
	for i, col := range unionSch {
		doltCol, err := sqlutil.ToDoltCol(uint64(i), col)
		if err != nil {
			return nil, err
		}
		cols = cols.Append(doltCol)
	}

	sch, err := schema.SchemaFromCols(cols)
	if err != nil {
		return nil, err
	}

	j.rowDiffWriter, err = json.NewJsonDiffWriter(iohelp.NopWrCloser(cli.CliOut), sch)
	return j.rowDiffWriter, err
}

func (j *jsonDiffWriter) WriteTriggerDiff(ctx context.Context, triggerName, oldDefn, newDefn string) error {
	err := j.beginDocumentIfNecessary()
	if err != nil {
		return err
	}

	if j.triggersWritten == 0 {
		// end the table if necessary
		if j.tablesWritten > 0 {
			_, err := j.wr.Write([]byte(jsonDataDiffFooter + ","))
			if err != nil {
				return err
			}
		}

		_, err := j.wr.Write([]byte(`"triggers":[`))
		if err != nil {
			return err
		}
	} else {
		_, err := j.wr.Write([]byte(","))
		if err != nil {
			return err
		}
	}

	triggerNameBytes, err := ejson.Marshal(triggerName)
	if err != nil {
		return err
	}

	oldDefnBytes, err := ejson.Marshal(oldDefn)
	if err != nil {
		return err
	}

	newDefnBytes, err := ejson.Marshal(newDefn)
	if err != nil {
		return err
	}

	_, err = j.wr.Write([]byte(fmt.Sprintf(`{"name":%s,"from_definition":%s,"to_definition":%s}`,
		triggerNameBytes, oldDefnBytes, newDefnBytes)))
	if err != nil {
		return err
	}

	j.triggersWritten++
	return nil
}

func (j *jsonDiffWriter) WriteViewDiff(ctx context.Context, viewName, oldDefn, newDefn string) error {
	err := j.beginDocumentIfNecessary()
	if err != nil {
		return err
	}

	if j.viewsWritten == 0 {
		// end the previous block if necessary
		if j.tablesWritten > 0 && j.triggersWritten == 0 {
			_, err := j.wr.Write([]byte(jsonDataDiffFooter + ","))
			if err != nil {
				return err
			}
		} else if j.triggersWritten > 0 {
			_, err := j.wr.Write([]byte("],"))
			if err != nil {
				return err
			}
		}
	}

	if j.viewsWritten == 0 {
		_, err := j.wr.Write([]byte(`"views":[`))
		if err != nil {
			return err
		}
	} else {
		_, err := j.wr.Write([]byte(","))
		if err != nil {
			return err
		}
	}

	viewNameBytes, err := ejson.Marshal(viewName)
	if err != nil {
		return err
	}

	oldDefnBytes, err := ejson.Marshal(oldDefn)
	if err != nil {
		return err
	}

	newDefnBytes, err := ejson.Marshal(newDefn)
	if err != nil {
		return err
	}

	_, err = j.wr.Write([]byte(fmt.Sprintf(`{"name":%s,"from_definition":%s,"to_definition":%s}`,
		viewNameBytes, oldDefnBytes, newDefnBytes)))
	if err != nil {
		return err
	}

	j.viewsWritten++
	return nil
}

func (j *jsonDiffWriter) Close(ctx context.Context) error {
	if j.tablesWritten > 0 || j.triggersWritten > 0 || j.viewsWritten > 0 {
		// We only need to close off the "tables" array if we didn't also write a view / trigger
		// (which also closes that array)
		if j.triggersWritten == 0 && j.viewsWritten == 0 {
			_, err := j.wr.Write([]byte(jsonDataDiffFooter))
			if err != nil {
				return err
			}
		} else {
			// if we did write a trigger or view, we need to close off that array
			_, err := j.wr.Write([]byte("]"))
			if err != nil {
				return err
			}
		}

		err := iohelp.WriteLine(j.wr, "}")
		if err != nil {
			return err
		}
	} else {
		err := iohelp.WriteLine(j.wr, "")
		if err != nil {
			return err
		}
	}

	// Writer has already been closed here during row iteration, no need to close it here
	return nil
}
