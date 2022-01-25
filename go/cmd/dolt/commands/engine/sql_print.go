// Copyright 2020 Dolthub, Inc.
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

package engine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/utils/pipeline"
)

type PrintResultFormat byte

const (
	FormatTabular PrintResultFormat = iota
	FormatCsv
	FormatJson
	FormatNull // used for profiling
	FormatVertical
)

const (
	readBatchSize  = 10
	writeBatchSize = 1
)

// PrettyPrintResults prints the result of a query (schema + row iter).
func PrettyPrintResults(ctx *sql.Context, resultFormat PrintResultFormat, sqlSch sql.Schema, rowIter sql.RowIter, hasTopLevelOrderBy bool) (rerr error) {
	defer func() {
		closeErr := rowIter.Close(ctx)
		if rerr == nil && closeErr != nil {
			rerr = closeErr
		}
	}()

	if isOkResult(sqlSch) {
		return printOKResult(ctx, rowIter)
	}

	// For some output formats, we want to convert everything to strings to be processed by the pipeline. For others,
	// we want to leave types alone and let the writer figure out how to format it for output.
	var p *pipeline.Pipeline
	switch resultFormat {
	case FormatCsv:
		p = createCSVPipeline(ctx, sqlSch, rowIter, hasTopLevelOrderBy)
	case FormatJson:
		p = createJSONPipeline(ctx, sqlSch, rowIter, hasTopLevelOrderBy)
	case FormatTabular:
		p = createTabularPipeline(ctx, sqlSch, rowIter)
	case FormatNull:
		p = createNullPipeline(ctx, sqlSch, rowIter)
	case FormatVertical:
		p = createVerticalPipeline(ctx, sqlSch, rowIter)
	}

	p.Start(ctx)
	rerr = p.Wait()

	return rerr
}

func printOKResult(ctx *sql.Context, iter sql.RowIter) (returnErr error) {
	row, err := iter.Next(ctx)
	if err != nil {
		return err
	}

	if okResult, ok := row[0].(sql.OkResult); ok {
		rowNoun := "row"
		if okResult.RowsAffected != 1 {
			rowNoun = "rows"
		}

		cli.Printf("Query OK, %d %s affected\n", okResult.RowsAffected, rowNoun)

		if okResult.Info != nil {
			cli.Printf("%s\n", okResult.Info)
		}
	}

	return nil
}

func isOkResult(sch sql.Schema) bool {
	return sch.Equals(sql.OkResultSchema)
}

// noParallelizationInitFunc only exists to validate the routine wasn't parallelized
func noParallelizationInitFunc(ctx context.Context, index int) error {
	if index != 0 {
		panic("cannot parallelize this routine")
	}

	return nil
}

// getReadStageFunc is a general purpose stage func used by multiple pipelines to read the rows into batches
func getReadStageFunc(ctx *sql.Context, iter sql.RowIter, batchSize int) pipeline.StageFunc {
	isDone := false
	return func(_ context.Context, _ []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if isDone {
			return nil, io.EOF
		}

		items := make([]pipeline.ItemWithProps, 0, batchSize)
		for i := 0; i < 10; i++ {
			r, err := iter.Next(ctx)

			if err == io.EOF {
				isDone = true
				break
			} else if err != nil {
				return nil, err
			}

			items = append(items, pipeline.NewItemWithNoProps(r))
		}

		if len(items) == 0 {
			return nil, io.EOF
		}

		return items, nil
	}
}

// writeToCliOutStageFunc is a general purpose stage func to write the output of a pipeline to stdout
func writeToCliOutStageFunc(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	for _, item := range items {
		str := *item.GetItem().(*string)
		cli.Print(str)
	}

	return nil, nil
}

// Null pipeline creation and stage functions

func createNullPipeline(ctx *sql.Context, sch sql.Schema, iter sql.RowIter) *pipeline.Pipeline {
	return pipeline.NewPipeline(
		pipeline.NewStage("read", noParallelizationInitFunc, getReadStageFunc(ctx, iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("drop", nil, dropOnFloor, 0, 100, writeBatchSize),
	)
}

func dropOnFloor(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	return nil, nil
}

// CSV Pipeline creation and stage functions

func createCSVPipeline(ctx *sql.Context, sch sql.Schema, iter sql.RowIter, hasTopLevelOrderBy bool) *pipeline.Pipeline {
	parallelism := 2

	// On order by clauses do not turn on parallelism so results are processed in the correct order.
	if hasTopLevelOrderBy {
		parallelism = 0
	}

	p := pipeline.NewPipeline(
		pipeline.NewStage("read", noParallelizationInitFunc, getReadStageFunc(ctx, iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("process", nil, csvProcessStageFunc, parallelism, 1000, readBatchSize),
		pipeline.NewStage("write", noParallelizationInitFunc, writeToCliOutStageFunc, 0, 100, writeBatchSize),
	)

	writeIn, _ := p.GetInputChannel("write")
	sb := strings.Builder{}
	for i, col := range sch {
		if i != 0 {
			sb.WriteRune(',')
		}

		sb.WriteString(col.Name)
	}
	sb.WriteRune('\n')

	str := sb.String()
	writeIn <- []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}

	return p
}

func csvProcessStageFunc(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	var b bytes.Buffer
	wr := bufio.NewWriter(&b)

	for _, item := range items {
		r := item.GetItem().(sql.Row)
		colValStrs := make([]*string, len(r))

		for colNum, col := range r {
			if col != nil {
				str := sqlutil.SqlColToStr(ctx, col)
				colValStrs[colNum] = &str
			} else {
				colValStrs[colNum] = nil
			}
		}

		err := csv.WriteCSVRow(wr, colValStrs, ",", false)

		if err != nil {
			return nil, err
		}
	}

	wr.Flush()

	str := b.String()
	return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}, nil
}

// JSON pipeline creation and stage functions
func createJSONPipeline(ctx *sql.Context, sch sql.Schema, iter sql.RowIter, hasTopLevelOrderBy bool) *pipeline.Pipeline {
	parallelism := 2

	// On order by clauses do not turn on parallelism so results are processed in the correct order.
	if hasTopLevelOrderBy {
		parallelism = 0
	}

	p := pipeline.NewPipeline(
		pipeline.NewStage("read", noParallelizationInitFunc, getReadStageFunc(ctx, iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("process", nil, getJSONProcessFunc(sch), parallelism, 1000, readBatchSize),
		pipeline.NewStage("write", noParallelizationInitFunc, writeJSONToCliOutStageFunc, 0, 100, writeBatchSize),
	)

	return p
}

func getJSONProcessFunc(sch sql.Schema) pipeline.StageFunc {
	formats := make([]string, len(sch))
	for i, col := range sch {
		switch col.Type.(type) {
		case sql.StringType, sql.DatetimeType, sql.EnumType, sql.TimeType:
			formats[i] = fmt.Sprintf(`"%s":"%%s"`, col.Name)
		default:
			formats[i] = fmt.Sprintf(`"%s":%%s`, col.Name)
		}
	}

	return func(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if items == nil {
			return nil, nil
		}

		sb := &strings.Builder{}
		sb.Grow(2048)
		for i, item := range items {
			r := item.GetItem().(sql.Row)

			if i != 0 {
				sb.WriteString(",{")
			} else {
				sb.WriteString("{")
			}

			validCols := 0
			for colNum, col := range r {
				if col != nil {
					if validCols != 0 {
						sb.WriteString(",")
					}

					validCols++
					colStr := sqlutil.SqlColToStr(ctx, col)

					if _, ok := col.(sql.JSONValue); !ok {
						// don't escape for JSONValue literals
						colStr = strings.Replace(colStr, "\"", "\\\"", -1)
					}

					str := fmt.Sprintf(formats[colNum], colStr)
					sb.WriteString(str)
				}
			}

			sb.WriteRune('}')
		}

		str := sb.String()
		return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}, nil
	}
}

func writeJSONToCliOutStageFunc(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	const hasRunKey = 0

	ls := pipeline.GetLocalStorage(ctx)
	_, hasRun := ls.Get(hasRunKey)
	ls.Put(hasRunKey, true)

	if items == nil {
		if hasRun {
			cli.Print("]}")
		} else {
			cli.Print("{\"rows\":[]}")
		}
	} else {
		for _, item := range items {
			if hasRun {
				cli.Print(",")
			} else {
				cli.Print("{\"rows\": [")
			}

			str := *item.GetItem().(*string)
			cli.Print(str)

			hasRun = true
		}
	}

	return nil, nil
}

// tabular pipeline creation and pipeline functions
func createTabularPipeline(ctx *sql.Context, sch sql.Schema, iter sql.RowIter) *pipeline.Pipeline {
	const samplesForAutoSizing = 10000
	tps := &tabularPipelineStages{}

	p := pipeline.NewPipeline(
		pipeline.NewStage("read", nil, getReadStageFunc(ctx, iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("stringify", nil, rowsToStringSlices, 0, 1000, 1000),
		pipeline.NewStage("fix_width", noParallelizationInitFunc, tps.getFixWidthStageFunc(samplesForAutoSizing), 0, 1000, readBatchSize),
		pipeline.NewStage("cell_borders", noParallelizationInitFunc, tps.getBorderFunc(), 0, 1000, readBatchSize),
		pipeline.NewStage("write", noParallelizationInitFunc, writeToCliOutStageFunc, 0, 100, writeBatchSize),
	)

	writeIn, _ := p.GetInputChannel("fix_width")
	headers := make([]string, len(sch))
	for i, col := range sch {
		headers[i] = col.Name
	}

	writeIn <- []pipeline.ItemWithProps{
		pipeline.NewItemWithProps(headers, pipeline.NewImmutableProps(map[string]interface{}{"headers": true})),
	}

	return p
}

func rowsToStringSlices(ctx context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	rows := make([]pipeline.ItemWithProps, len(items))
	for i, item := range items {
		r := item.GetItem().(sql.Row)

		cols := make([]string, len(r))
		for colNum, col := range r {
			isNull := col == nil

			if !isNull {
				sqlTypeInst, isType := col.(sql.Type)

				if isType && sqlTypeInst.Type() == sqltypes.Null {
					isNull = true
				}
			}

			if !isNull {
				cols[colNum] = sqlutil.SqlColToStr(ctx, col)
			} else {
				cols[colNum] = "NULL"
			}
		}

		rows[i] = pipeline.NewItemWithNoProps(cols)
	}

	return rows, nil
}

type tabularPipelineStages struct {
	rowSep string
}

func (tps *tabularPipelineStages) getFixWidthStageFunc(samples int) func(context.Context, []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	bufferring := true
	buffer := make([]pipeline.ItemWithProps, 0, samples)
	idxToMaxWidth := make(map[int]int)
	idxToMaxNumRunes := make(map[int]int)
	var fwf fwt.FixedWidthFormatter
	return func(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if items == nil {
			bufferring = false
			fwf = fwt.NewFixedWidthFormatter(fwt.HashFillWhenTooLong, idxMapToSlice(idxToMaxWidth), idxMapToSlice(idxToMaxNumRunes))
			tps.rowSep = genRowSepString(fwf)
			return tps.formatItems(fwf, buffer)
		}

		if bufferring {
			for _, item := range items {
				cols := item.GetItem().([]string)

				for colIdx, colStr := range cols {
					strWidth := fwt.StringWidth(colStr)
					if strWidth > idxToMaxWidth[colIdx] {
						idxToMaxWidth[colIdx] = strWidth
					}

					numRunes := len([]rune(colStr))
					if numRunes > idxToMaxNumRunes[colIdx] {
						idxToMaxNumRunes[colIdx] = numRunes
					}
				}

				buffer = append(buffer, item)
			}

			if len(buffer) > samples {
				bufferring = false
				fwf = fwt.NewFixedWidthFormatter(fwt.HashFillWhenTooLong, idxMapToSlice(idxToMaxWidth), idxMapToSlice(idxToMaxNumRunes))
				tps.rowSep = genRowSepString(fwf)
				ret, err := tps.formatItems(fwf, buffer)

				if err != nil {
					return nil, err
				}

				// clear the buffer
				buffer = buffer[:0]
				return ret, nil
			}

			return nil, nil
		}

		return tps.formatItems(fwf, items)
	}
}

func (tps *tabularPipelineStages) formatItems(fwf fwt.FixedWidthFormatter, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	results := make([]pipeline.ItemWithProps, len(items))
	for i, item := range items {
		cols := item.GetItem().([]string)
		formatted, err := fwf.Format(cols)

		if err != nil {
			return nil, err
		}

		results[i] = pipeline.NewItemWithProps(formatted, item.GetProperties())
	}

	return results, nil
}

func (tps *tabularPipelineStages) getBorderFunc() func(context.Context, []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	return func(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if items == nil {
			return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&tps.rowSep)}, nil
		}

		sb := &strings.Builder{}
		sb.Grow(2048)
		for _, item := range items {
			props := item.GetProperties()
			headers := false
			if _, ok := props.Get("headers"); ok {
				headers = true
				sb.WriteString(tps.rowSep)
			}

			cols := item.GetItem().([]string)

			for _, str := range cols {
				sb.WriteString("| ")
				sb.WriteString(str)
				sb.WriteRune(' ')
			}

			sb.WriteString("|\n")

			if headers {
				sb.WriteString(tps.rowSep)
			}
		}

		str := sb.String()
		return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}, nil
	}
}

func idxMapToSlice(idxMap map[int]int) []int {
	sl := make([]int, len(idxMap))
	for idx, val := range idxMap {
		sl[idx] = val
	}

	return sl
}

func genRowSepString(fwf fwt.FixedWidthFormatter) string {
	rowSepRunes := make([]rune, fwf.TotalWidth+(3*len(fwf.Widths))+2)
	for i := 0; i < len(rowSepRunes); i++ {
		rowSepRunes[i] = '-'
	}

	var pos int
	for _, width := range fwf.Widths {
		rowSepRunes[pos] = '+'
		pos += width + 3
	}

	rowSepRunes[pos] = '+'
	rowSepRunes[pos+1] = '\n'

	return string(rowSepRunes)
}

// vertical format pipeline creation and pipeline functions
func createVerticalPipeline(ctx *sql.Context, sch sql.Schema, iter sql.RowIter) *pipeline.Pipeline {
	const samplesForAutoSizing = 10000
	vps := &verticalPipelineStages{}

	p := pipeline.NewPipeline(
		pipeline.NewStage("read", nil, getReadStageFunc(ctx, iter, readBatchSize), 0, 0, 0),
		pipeline.NewStage("stringify", nil, rowsToStringSlices, 0, 1000, 1000),
		pipeline.NewStage("fix_width", noParallelizationInitFunc, vps.getFixWidthStageFunc(samplesForAutoSizing), 0, 1000, readBatchSize),
		pipeline.NewStage("cell_borders", noParallelizationInitFunc, vps.getSeparatorFunc(), 0, 1000, readBatchSize),
		pipeline.NewStage("write", noParallelizationInitFunc, writeToCliOutStageFunc, 0, 100, writeBatchSize),
	)

	writeIn, _ := p.GetInputChannel("fix_width")
	headers := make([]string, len(sch))
	for i, col := range sch {
		headers[i] = col.Name
	}

	writeIn <- []pipeline.ItemWithProps{
		pipeline.NewItemWithProps(headers, pipeline.NewImmutableProps(map[string]interface{}{"headers": true})),
	}

	return p
}

type verticalPipelineStages struct {
	heads  []string
	rowIdx int
}

func (vps *verticalPipelineStages) getFixWidthStageFunc(samples int) func(context.Context, []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	bufferring := true
	buffer := make([]pipeline.ItemWithProps, 0, samples)
	var maxWidth int
	return func(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		if items == nil {
			bufferring = false
			return buffer, nil
		}
		if bufferring {
			for _, item := range items {
				props := item.GetProperties()
				if _, ok := props.Get("headers"); ok {
					head := item.GetItem().([]string)

					for _, headStr := range head {
						strWidth := fwt.StringWidth(headStr)
						if strWidth > maxWidth {
							maxWidth = strWidth
						}
					}
					vps.heads, _ = vps.formatHeads(maxWidth, head)
					vps.rowIdx = 0
				} else {
					buffer = append(buffer, item)
				}

			}

			if len(buffer) > samples {
				bufferring = false
				ret := buffer

				// clear the buffer
				buffer = buffer[:0]
				return ret, nil
			}
			return nil, nil
		}
		return items, nil
	}
}

func (vps *verticalPipelineStages) formatHeads(width int, items []string) ([]string, error) {
	results := make([]string, len(items))
	for i, item := range items {
		diff := width - len(item)
		if diff < 0 {
			return nil, errors.New("column width exceeded maximum width for column")
		}
		results[i] = fmt.Sprintf("%*s", width, item)
	}
	return results, nil
}

func (vps *verticalPipelineStages) getSeparatorFunc() func(context.Context, []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
	return func(_ context.Context, items []pipeline.ItemWithProps) ([]pipeline.ItemWithProps, error) {
		empty := ""
		if items == nil {
			return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&empty)}, nil
		}

		sb := &strings.Builder{}
		sb.Grow(2048)
		var sep string
		for _, item := range items {
			vps.rowIdx += 1
			sep = fmt.Sprintf("*************************** %d. row ***************************\n", vps.rowIdx)
			sb.WriteString(sep)

			cols := item.GetItem().([]string)

			for i, str := range cols {
				sb.WriteString(vps.heads[i])
				sb.WriteString(": ")
				sb.WriteString(str)
				sb.WriteString("\n")
			}
		}
		str := sb.String()
		return []pipeline.ItemWithProps{pipeline.NewItemWithNoProps(&str)}, nil
	}
}
