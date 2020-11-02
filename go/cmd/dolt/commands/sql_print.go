package commands

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	pipeline2 "github.com/dolthub/dolt/go/libraries/utils/pipeline"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	readBatchSize  = 10
	writeBatchSize = 1
)

// sqlColToStr is a utility function for converting a sql column of type interface{} to a string
func sqlColToStr(col interface{}) string {
	if col != nil {
		switch typedCol := col.(type) {
		case int:
			return strconv.FormatInt(int64(typedCol), 10)
		case int32:
			return strconv.FormatInt(int64(typedCol), 10)
		case int64:
			return strconv.FormatInt(int64(typedCol), 10)
		case int16:
			return strconv.FormatInt(int64(typedCol), 10)
		case int8:
			return strconv.FormatInt(int64(typedCol), 10)
		case uint:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint32:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint64:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint16:
			return strconv.FormatUint(uint64(typedCol), 10)
		case uint8:
			return strconv.FormatUint(uint64(typedCol), 10)
		case float64:
			return strconv.FormatFloat(float64(typedCol), 'g', -1, 64)
		case float32:
			return strconv.FormatFloat(float64(typedCol), 'g', -1, 32)
		case string:
			return typedCol
		case bool:
			if typedCol {
				return "true"
			} else {
				return "false"
			}
		case time.Time:
			return typedCol.Format("2006-01-02 15:04:05 -0700 MST")
		}
	}

	return ""
}

// getReadStageFunc is a general purpose stage func used by multiple pipelines to read the rows into batches
func getReadStageFunc(iter sql.RowIter, batchSize int) pipeline2.StageFunc {
	return func(ctx context.Context, _ []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
		items := make([]pipeline2.ItemWithProps, 0, batchSize)
		for i := 0; i < 10; i++ {
			r, err := iter.Next()

			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			items = append(items, pipeline2.NewItemWithNoProps(r))
		}

		if len(items) == 0 {
			return nil, io.EOF
		}

		return items, nil
	}
}

// writeToCliOutStageFunc is a general purpose stage func to write the output of a pipeline to stdout
func writeToCliOutStageFunc(ctx context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	for _, item := range items {
		str := *item.GetItem().(*string)
		cli.Printf(str)
	}

	return nil, nil
}


// Null pipeline creation and stage functions

func createNullPipeline(_ context.Context, sch sql.Schema, iter sql.RowIter) *pipeline2.Pipeline {
	return pipeline2.NewPipeline(
		pipeline2.NewStage("read", nil, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline2.NewStage("drop", nil, dropOnFloor, 0, 100, writeBatchSize),
	)
}

func dropOnFloor(ctx context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	return nil, nil
}


// CSV Pipeline creation and stage functions


func createCSVPipeline(_ context.Context, sch sql.Schema, iter sql.RowIter) *pipeline2.Pipeline {
	p := pipeline2.NewPipeline(
		pipeline2.NewStage("read", nil, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline2.NewStage("process", nil, csvProcessStageFunc, 2, 1000, readBatchSize),
		pipeline2.NewStage("write", nil, writeToCliOutStageFunc, 0, 100, writeBatchSize),
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
	writeIn <- []pipeline2.ItemWithProps{pipeline2.NewItemWithNoProps(&str)}

	return p
}

func csvProcessStageFunc(ctx context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	sb := &strings.Builder{}
	sb.Grow(2048)
	for _, item := range items {
		r := item.GetItem().(sql.Row)

		for colNum, col := range r {
			if col != nil {
				str := sqlColToStr(col)

				if len(str) == 0 {
					str = "\"\""
				}

				sb.WriteString(str)
			}

			if colNum != len(r)-1 {
				sb.WriteRune(',')
			}
		}

		sb.WriteRune('\n')
	}

	str := sb.String()
	return []pipeline2.ItemWithProps{pipeline2.NewItemWithNoProps(&str)}, nil
}



// JSON pipeline creation and stage functions


func createJSONPipeline(_ context.Context, sch sql.Schema, iter sql.RowIter) *pipeline2.Pipeline {
	p := pipeline2.NewPipeline(
		pipeline2.NewStage("read", nil, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline2.NewStage("process", nil, getJSONProcessFunc(sch), 2, 1000, readBatchSize),
		pipeline2.NewStage("write", writeJSONInitRoutineFunc, writeJSONToCliOutStageFunc, 0, 100, writeBatchSize),
	)

	return p
}

func getJSONProcessFunc(sch sql.Schema) pipeline2.StageFunc {
	formats := make([]string, len(sch))
	for i, col := range sch {
		switch col.Type.(type) {
		case sql.StringType, sql.DatetimeType, sql.EnumType, sql.TimeType:
			formats[i] = fmt.Sprintf(`"%s":"%%s"`, col.Name)
		default:
			formats[i] = fmt.Sprintf(`"%s":%%s`, col.Name)
		}
	}

	return func(ctx context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
		if items == nil {
			return nil, nil
		}

		sb := &strings.Builder{}
		sb.Grow(2048)
		for i, item := range items {
			r := item.GetItem().(sql.Row)

			if i != 0 {
				sb.WriteString(",\n\t{")
			} else {
				sb.WriteString("\t{")
			}

			validCols := 0
			for colNum, col := range r {
				if col != nil {
					if validCols != 0 {
						sb.WriteString(", ")
					}

					validCols++
					str := fmt.Sprintf(formats[colNum], sqlColToStr(col))
					sb.WriteString(str)
				}
			}

			sb.WriteRune('}')
		}

		str := sb.String()
		return []pipeline2.ItemWithProps{pipeline2.NewItemWithNoProps(&str)}, nil
	}
}

// this function only exists to validate the write routine wasn't parallelized
func writeJSONInitRoutineFunc(ctx context.Context, index int) error {
	if index != 0 {
		// writeJSONToCliOut assumes that it is not being run in parallel.
		panic("cannot parallelize this routine")
	}

	return nil
}

func writeJSONToCliOutStageFunc(ctx context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	const hasRunKey = 0

	ls := pipeline2.GetLocalStorage(ctx)
	_, hasRun := ls.Get(hasRunKey)
	ls.Put(hasRunKey, true)

	if items == nil {
		if hasRun {
			cli.Printf("\n]}\n")
		} else {
			cli.Printf("{\"data\":[]}\n")
		}
	} else {
		for _, item := range items {
			if hasRun {
				cli.Printf(",\n")
			} else {
				cli.Printf("{\"data\": [\n")
			}

			str := *item.GetItem().(*string)
			cli.Printf(str)

			hasRun = true
		}
	}

	return nil, nil
}


// tabular pipeline creation and pipeline functions

func createTabularPipeline(_ context.Context, sch sql.Schema, iter sql.RowIter) *pipeline2.Pipeline {
	const samplesForAutoSizing = 1000

	p := pipeline2.NewPipeline(
		pipeline2.NewStage("read", nil, getReadStageFunc(iter, readBatchSize), 0, 0, 0),
		pipeline2.NewStage("stringify", nil, rowsToStringSlices, 0, 1000,  1000),
		pipeline2.NewStage("fix_width", nil , getFixWidthStageFunc(samplesForAutoSizing), 0, 1000, readBatchSize),
		pipeline2.NewStage("cell_borders", nil, getBorderFunc(), 0, 1000, readBatchSize),
		pipeline2.NewStage("write", nil, writeToCliOutStageFunc, 0, 100, writeBatchSize),
	)

	writeIn, _ := p.GetInputChannel("fix_width")
	headers := make([]string, len(sch))
	for i, col := range sch {
		headers[i] = col.Name
	}

	writeIn <- []pipeline2.ItemWithProps{
		pipeline2.NewItemWithProps(headers, pipeline2.NewImmutableProps(map[string]interface{}{"headers": true})),
	}

	return p
}

func rowsToStringSlices(_ context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	if items == nil {
		return nil, nil
	}

	rows := make([]pipeline2.ItemWithProps, len(items))
	for i, item := range items {
		r := item.GetItem().(sql.Row)

		cols := make([]string, len(r))
		for colNum, col := range r {
			if col != nil {
				cols[colNum] = sqlColToStr(col)
			}
		}

		rows[i] = pipeline2.NewItemWithNoProps(cols)
	}

	return rows, nil
}

var testSepStr = ""

func getFixWidthStageFunc(samples int) func(context.Context, []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	bufferring := true
	buffer := make([]pipeline2.ItemWithProps, 0, samples)
	idxToMaxWidth := make(map[int]int)
	idxToMaxNumRunes := make(map[int]int)
	var fwf fwt.FixedWidthFormatter
	return func(_ context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
		if items == nil {
			bufferring = false
			fwf = fwt.NewFixedWidthFormatter(fwt.HashFillWhenTooLong, idxMapToSlice(idxToMaxWidth), idxMapToSlice(idxToMaxNumRunes))
			testSepStr = genRowSepString(fwf)
			return formatItems(fwf, buffer)
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
				testSepStr = genRowSepString(fwf)
				return formatItems(fwf, buffer)
			}

			return nil, nil
		}

		return formatItems(fwf, buffer)
	}
}

func formatItems(fwf fwt.FixedWidthFormatter, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	results := make([]pipeline2.ItemWithProps, len(items))
	for i, item := range items {
		cols := item.GetItem().([]string)
		formatted, err := fwf.Format(cols)

		if err != nil {
			return nil, err
		}

		results[i] = pipeline2.NewItemWithProps(formatted, item.GetProperties())
	}

	return results, nil
}

func idxMapToSlice(idxMap map[int]int) []int {
	sl := make([]int, len(idxMap))
	for idx, val := range idxMap {
		sl[idx] = val
	}

	return sl
}

func getBorderFunc() func(context.Context, []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
	return func(_ context.Context, items []pipeline2.ItemWithProps) ([]pipeline2.ItemWithProps, error) {
		if items == nil {
			return []pipeline2.ItemWithProps{pipeline2.NewItemWithNoProps(&testSepStr)}, nil
		}

		sb := &strings.Builder{}
		sb.Grow(2048)
		for _, item := range items {
			props := item.GetProperties()
			headers := false
			if _, ok := props.Get("headers"); ok {
				headers = true
				sb.WriteString(testSepStr)
			}

			cols := item.GetItem().([]string)

			sb.WriteString("| ")
			for _, str := range cols {
				sb.WriteString(str)
				sb.WriteString( " | ")
			}

			sb.WriteRune('\n')

			if headers {
				sb.WriteString(testSepStr)
			}
		}

		str := sb.String()
		return []pipeline2.ItemWithProps{pipeline2.NewItemWithNoProps(&str)}, nil
	}
}


func genRowSepString(fwf fwt.FixedWidthFormatter) string {
	rowSepRunes := make([]rune, fwf.TotalWidth + 5)
	for i := 0; i < len(rowSepRunes); i++ {
		rowSepRunes[i] = '-'
	}

	rowSepRunes[0] = '+'
	rowSepRunes[fwf.TotalWidth + 4] = '\n'
	pos := 2
	for _, width := range fwf.Widths {
		pos += width
		rowSepRunes[pos+1] = '+'
	}

	return string(rowSepRunes)
}
