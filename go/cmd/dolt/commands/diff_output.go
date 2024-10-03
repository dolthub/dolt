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
	"errors"
	"fmt"
	"io"

	textdiff "github.com/andreyvit/diff"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

// diffWriter is an interface that lets us write diffs in a variety of output formats
type diffWriter interface {
	// BeginTable is called when a new table is about to be written, before any schema or row diffs are written
	BeginTable(fromTableName, toTableName string, isAdd, isDrop bool) error
	// WriteTableSchemaDiff is called to write a schema diff for the table given (if requested by args)
	WriteTableSchemaDiff(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary) error
	// WriteEventDiff is called to write an event diff
	WriteEventDiff(ctx context.Context, eventName, oldDefn, newDefn string) error
	// WriteTriggerDiff is called to write a trigger diff
	WriteTriggerDiff(ctx context.Context, triggerName, oldDefn, newDefn string) error
	// WriteViewDiff is called to write a view diff
	WriteViewDiff(ctx context.Context, viewName, oldDefn, newDefn string) error
	// WriteTableDiffStats is called to write the diff stats for the table given
	WriteTableDiffStats(diffStats []diffStatistics, oldColLen, newColLen int, areTablesKeyless bool) error
	// RowWriter returns a row writer for the table delta provided, which will have Close() called on it when rows are
	// done being written.
	RowWriter(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary, unionSch sql.Schema) (diff.SqlRowDiffWriter, error)
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

func (t tabularDiffWriter) BeginTable(fromTableName, toTableName string, isAdd, isDrop bool) error {
	bold := color.New(color.Bold)
	if isDrop {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", fromTableName, fromTableName)
		_, _ = bold.Println("deleted table")
	} else if isAdd {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", toTableName, toTableName)
		_, _ = bold.Println("added table")
	} else {
		_, _ = bold.Printf("diff --dolt a/%s b/%s\n", fromTableName, toTableName)
		_, _ = bold.Printf("--- a/%s\n", fromTableName)
		_, _ = bold.Printf("+++ b/%s\n", toTableName)
	}
	return nil
}

func (t tabularDiffWriter) WriteTableSchemaDiff(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary) error {
	var fromCreateStmt = ""
	if fromTableInfo != nil {
		fromCreateStmt = fromTableInfo.CreateStmt
	}

	var toCreateStmt = ""
	if toTableInfo != nil {
		toCreateStmt = toTableInfo.CreateStmt
	}

	if fromCreateStmt != toCreateStmt {
		cli.Println(textdiff.LineDiff(fromCreateStmt, toCreateStmt))
	}

	return nil
}

func (t tabularDiffWriter) WriteEventDiff(ctx context.Context, eventName, oldDefn, newDefn string) error {
	// identical implementation
	return t.WriteViewDiff(ctx, eventName, oldDefn, newDefn)
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

func (t tabularDiffWriter) WriteTableDiffStats(diffStats []diffStatistics, oldColLen, newColLen int, areTablesKeyless bool) error {
	acc := diff.DiffStatProgress{}
	eP := cli.NewEphemeralPrinter()
	var pos int
	for i, diffStat := range diffStats {
		acc.Adds += diffStat.RowsAdded
		acc.Removes += diffStat.RowsDeleted
		acc.Changes += diffStat.RowsModified
		acc.CellChanges += diffStat.CellsModified
		acc.NewRowSize += diffStat.NewRowCount
		acc.OldRowSize += diffStat.OldRowCount
		acc.NewCellSize += diffStat.NewCellCount
		acc.OldCellSize += diffStat.OldCellCount

		if i != 0 && i%10000 == 0 {
			msg := fmt.Sprintf("prev size: %d, new size: %d, adds: %d, deletes: %d, modifications: %d\n", acc.OldRowSize, acc.NewRowSize, acc.Adds, acc.Removes, acc.Changes)
			eP.Printf(msg)
			eP.Display()
			pos += len(msg)
		}
	}

	cli.DeleteAndPrint(pos, "")

	if (acc.Adds+acc.Removes+acc.Changes) == 0 && (acc.OldCellSize-acc.NewCellSize) == 0 {
		cli.Println("No data changes. See schema changes by using -s or --schema.")
		return nil
	}

	if areTablesKeyless {
		t.printKeylessStat(acc)
	} else {
		t.printStat(acc, oldColLen, newColLen)
	}

	return nil
}

func (t tabularDiffWriter) printStat(acc diff.DiffStatProgress, oldColLen, newColLen int) {
	numCellInserts, numCellDeletes := dtablefunctions.GetCellsAddedAndDeleted(acc, newColLen)
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

func (t tabularDiffWriter) printKeylessStat(acc diff.DiffStatProgress) {
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)

	cli.Printf("%s\n", insertions)
	cli.Printf("%s\n", deletions)
}

func (t tabularDiffWriter) RowWriter(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary, unionSch sql.Schema) (diff.SqlRowDiffWriter, error) {
	return tabular.NewFixedWidthDiffTableWriter(unionSch, iohelp.NopWrCloser(cli.CliOut), 100), nil
}

type sqlDiffWriter struct{}

var _ diffWriter = (*tabularDiffWriter)(nil)

func (s sqlDiffWriter) Close(ctx context.Context) error {
	return nil
}

func (s sqlDiffWriter) BeginTable(fromTableName, toTableName string, isAdd, isDrop bool) error {
	return nil
}

func (s sqlDiffWriter) WriteTableSchemaDiff(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary) error {
	stmts := tds.AlterStmts
	if tds.IsAdd() {
		stmts = []string{toTableInfo.CreateStmt}
	} else if tds.IsDrop() {
		stmts = []string{sqlfmt.DropTableStmt(fromTableInfo.Name)}
	}
	for _, stmt := range stmts {
		if len(stmt) == 0 {
			continue
		}
		cli.Println(stmt)
	}

	return nil
}

func (s sqlDiffWriter) WriteEventDiff(ctx context.Context, eventName, oldDefn, newDefn string) error {
	// definitions will already be semicolon terminated, no need to add additional ones
	if oldDefn == "" {
		cli.Println(newDefn)
	} else if newDefn == "" {
		cli.Println(fmt.Sprintf("DROP EVENT %s;", sql.QuoteIdentifier(eventName)))
	} else {
		cli.Println(fmt.Sprintf("DROP EVENT %s;", sql.QuoteIdentifier(eventName)))
		cli.Println(newDefn)
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

func (s sqlDiffWriter) WriteTableDiffStats(diffStats []diffStatistics, oldColLen, newColLen int, areTablesKeyless bool) error {
	// TODO: implement this
	return errors.New("diff stats are not supported for sql output")
}

func (s sqlDiffWriter) RowWriter(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary, unionSch sql.Schema) (diff.SqlRowDiffWriter, error) {
	var targetSch schema.Schema
	if toTableInfo != nil {
		targetSch = toTableInfo.Sch
	}
	if targetSch == nil {
		targetSch = fromTableInfo.Sch
	}

	// TODO: schema names
	return sqlexport.NewSqlDiffWriter(tds.ToTableName.Name, targetSch, iohelp.NopWrCloser(cli.CliOut)), nil
}

type jsonDiffWriter struct {
	wr              io.WriteCloser
	tablesWritten   int
	triggersWritten int
	viewsWritten    int
	eventsWritten   int
}

var _ diffWriter = (*tabularDiffWriter)(nil)

func newJsonDiffWriter(wr io.WriteCloser) (*jsonDiffWriter, error) {
	return &jsonDiffWriter{
		wr: wr,
	}, nil
}

const jsonDiffHeader = `"tables":[`
const jsonDiffFooter = `]`
const jsonDiffSep = `},`
const jsonDiffTableHeader = `{"name":"%s",`
const jsonDiffTableFooter = `}`
const jsonDiffDataDiffHeader = `"data_diff":[`
const jsonDiffDataDiffFooter = `]`

func (j *jsonDiffWriter) beginDocumentIfNecessary() error {
	if j.tablesWritten == 0 && j.triggersWritten == 0 && j.viewsWritten == 0 {
		_, err := j.wr.Write([]byte("{"))
		return err
	}
	return nil
}

func (j *jsonDiffWriter) BeginTable(fromTableName, toTableName string, isAdd, isDrop bool) error {
	err := j.beginDocumentIfNecessary()
	if err != nil {
		return err
	}

	if j.tablesWritten == 0 {
		err = iohelp.WriteAll(j.wr, []byte(jsonDiffHeader))
	} else {
		// close previous table object, and start new one
		err = iohelp.WriteAll(j.wr, []byte(jsonDiffSep))
	}
	if err != nil {
		return err
	}

	tableName := fromTableName
	if len(tableName) == 0 {
		tableName = toTableName
	}

	err = iohelp.WriteAll(j.wr, []byte(fmt.Sprintf(jsonDiffTableHeader, tableName)))
	if err != nil {
		return err
	}

	j.tablesWritten++
	return err
}

func (j *jsonDiffWriter) WriteTableSchemaDiff(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary) error {
	jsonSchDiffWriter, err := json.NewSchemaDiffWriter(iohelp.NopWrCloser(j.wr))
	if err != nil {
		return err
	}

	stmts := tds.AlterStmts
	if tds.IsAdd() {
		stmts = []string{toTableInfo.CreateStmt}
	} else if tds.IsDrop() {
		stmts = []string{sqlfmt.DropTableStmt(fromTableInfo.Name)}
	}

	for _, stmt := range stmts {
		if len(stmt) == 0 {
			continue
		}
		err = jsonSchDiffWriter.WriteSchemaDiff(stmt)
		if err != nil {
			return err
		}
	}

	return jsonSchDiffWriter.Close()
}

func (j *jsonDiffWriter) RowWriter(fromTableInfo, toTableInfo *diff.TableInfo, tds diff.TableDeltaSummary, unionSch sql.Schema) (diff.SqlRowDiffWriter, error) {
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

	jsonRowDiffWriter, err := json.NewJSONRowDiffWriter(iohelp.NopWrCloser(cli.CliOut), sch)
	if err != nil {
		return nil, err
	}

	return jsonRowDiffWriter, nil
}

const jsonDiffEventsHeader = `"events":[`

func (j *jsonDiffWriter) WriteEventDiff(ctx context.Context, eventName, oldDefn, newDefn string) error {
	err := j.beginDocumentIfNecessary()
	if err != nil {
		return err
	}

	if j.eventsWritten == 0 {
		// end the table if necessary
		if j.tablesWritten > 0 {
			// close off table object and tables array, and indicate start of views array
			_, err = j.wr.Write([]byte(jsonDiffTableFooter + jsonDiffFooter + ","))
			if err != nil {
				return err
			}
		}
		_, err = j.wr.Write([]byte(jsonDiffEventsHeader))
	} else {
		_, err = j.wr.Write([]byte(","))
	}

	if err != nil {
		return err
	}

	eventNameBytes, err := ejson.Marshal(eventName)
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
		eventNameBytes, oldDefnBytes, newDefnBytes)))
	if err != nil {
		return err
	}

	j.eventsWritten++
	return nil
}

const jsonDiffTriggersHeader = `"triggers":[`

func (j *jsonDiffWriter) WriteTriggerDiff(ctx context.Context, triggerName, oldDefn, newDefn string) error {
	err := j.beginDocumentIfNecessary()
	if err != nil {
		return err
	}

	if j.triggersWritten == 0 {
		// end the previous block if necessary
		if j.tablesWritten > 0 && j.eventsWritten == 0 {
			// close off table object and tables array, and indicate start of views array
			_, err = j.wr.Write([]byte(jsonDiffTableFooter + jsonDiffFooter + ","))
		} else if j.eventsWritten > 0 {
			_, err = j.wr.Write([]byte("],"))
		}
		if err != nil {
			return err
		}
		_, err = j.wr.Write([]byte(jsonDiffTriggersHeader))
	} else {
		_, err = j.wr.Write([]byte(","))
	}

	if err != nil {
		return err
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

const jsonDiffViewsHeader = `"views":[`

func (j *jsonDiffWriter) WriteViewDiff(ctx context.Context, viewName, oldDefn, newDefn string) error {
	err := j.beginDocumentIfNecessary()
	if err != nil {
		return err
	}

	if j.viewsWritten == 0 {
		// end the previous block if necessary
		if j.tablesWritten > 0 && j.eventsWritten == 0 && j.triggersWritten == 0 {
			// close off table object and tables array, and indicate start of views array
			_, err = j.wr.Write([]byte(jsonDiffTableFooter + jsonDiffFooter + ","))
		} else if j.eventsWritten > 0 || j.triggersWritten > 0 {
			_, err = j.wr.Write([]byte("],"))
		}
		if err != nil {
			return err
		}
		_, err = j.wr.Write([]byte(jsonDiffViewsHeader))
	} else {
		_, err = j.wr.Write([]byte(","))
	}

	if err != nil {
		return err
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

	viewStmt := fmt.Sprintf(`{"name":%s,"from_definition":%s,"to_definition":%s}`, viewNameBytes, oldDefnBytes, newDefnBytes)
	_, err = j.wr.Write([]byte(viewStmt))
	if err != nil {
		return err
	}

	j.viewsWritten++
	return nil
}

const jsonDiffStatsHeader = `"stats":{`
const jsonDiffStatsFooter = `}`

func (j *jsonDiffWriter) WriteTableDiffStats(diffStats []diffStatistics, oldColLen, newColLen int, areTablesKeyless bool) error {
	acc := diff.DiffStatProgress{}
	for _, diffStat := range diffStats {
		acc.Adds += diffStat.RowsAdded
		acc.Removes += diffStat.RowsDeleted
		acc.Changes += diffStat.RowsModified
		acc.CellChanges += diffStat.CellsModified
		acc.NewRowSize += diffStat.NewRowCount
		acc.OldRowSize += diffStat.OldRowCount
		acc.NewCellSize += diffStat.NewCellCount
		acc.OldCellSize += diffStat.OldCellCount
	}
	j.wr.Write([]byte(jsonDiffStatsHeader))
	if (acc.Adds+acc.Removes+acc.Changes) == 0 && (acc.OldCellSize-acc.NewCellSize) == 0 {
		j.wr.Write([]byte(jsonDiffStatsFooter))
		return nil
	}

	j.wr.Write([]byte(fmt.Sprintf(`"rows_added":%d,`, acc.Adds)))
	j.wr.Write([]byte(fmt.Sprintf(`"rows_deleted":%d,`, acc.Removes)))
	j.wr.Write([]byte(fmt.Sprintf(`"rows_modified":%d,`, acc.Changes)))
	rowsUnmodified := acc.OldRowSize - acc.Changes - acc.Removes
	j.wr.Write([]byte(fmt.Sprintf(`"rows_unmodified":%d,`, rowsUnmodified)))

	cellAdds, cellDeletes := dtablefunctions.GetCellsAddedAndDeleted(acc, newColLen)
	j.wr.Write([]byte(fmt.Sprintf(`"cells_added":%d,`, cellAdds)))
	j.wr.Write([]byte(fmt.Sprintf(`"cells_deleted":%d,`, cellDeletes)))
	j.wr.Write([]byte(fmt.Sprintf(`"cells_modified":%d`, acc.CellChanges)))

	j.wr.Write([]byte(jsonDiffStatsFooter))
	return nil
}

func (j *jsonDiffWriter) Close(ctx context.Context) error {
	if j.tablesWritten > 0 || j.triggersWritten > 0 || j.viewsWritten > 0 || j.eventsWritten > 0 {
		// close off tables object
		if j.triggersWritten == 0 && j.viewsWritten == 0 && j.eventsWritten == 0 {
			_, err := j.wr.Write([]byte(jsonDiffTableFooter))
			if err != nil {
				return err
			}
		}

		// close off last block
		_, err := j.wr.Write([]byte(jsonDiffFooter))
		if err != nil {
			return err
		}

		// end document
		_, err = j.wr.Write([]byte("}"))
		if err != nil {
			return err
		}
	}

	// Writer has already been closed here during row iteration, no need to close it here
	return nil
}
