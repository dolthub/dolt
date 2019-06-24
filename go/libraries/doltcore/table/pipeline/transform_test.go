package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var inCSV = `first,last,film or show,year
Tim,Allen,The Santa Clause,1994
Tim,Allen,The Santa Clause 2,2002
Tim,Allen,The Santa Clause 3: The Escape Clause,2006
Ed,Asner,Elf,2003
Ed,Asner,Christmas on the Bayou,2013
Ed,Asner,Elf: Buddy's Musical Christmas,2014
Fred,Astaire,The Man in the Santa Claus Suit,1979
Richard,Attenborough,Miracle on 34th Street,1994
Steve,Bacic,Deck the Halls,2005
Alec,Baldwin,Rise of the Guardians,2012
Don,Beddoe,Bewitched (episode Humbug Not to Be Spoken Here - Season 4),1967
`

var outCSV = `first,last,film or show,year,pre2000,index
Tim,Allen,The Santa Clause,1994,true,0
Tim,Allen,The Santa Clause,1994,true,1
Tim,Allen,The Santa Clause 2,2002,false,0
Tim,Allen,The Santa Clause 2,2002,false,1
Tim,Allen,The Santa Clause 3: The Escape Clause,2006,false,0
Tim,Allen,The Santa Clause 3: The Escape Clause,2006,false,1
Ed,Asner,Elf,2003,false,0
Ed,Asner,Elf,2003,false,1
Ed,Asner,Christmas on the Bayou,2013,false,0
Ed,Asner,Christmas on the Bayou,2013,false,1
Ed,Asner,Elf: Buddy's Musical Christmas,2014,false,0
Ed,Asner,Elf: Buddy's Musical Christmas,2014,false,1
Fred,Astaire,The Man in the Santa Claus Suit,1979,true,0
Fred,Astaire,The Man in the Santa Claus Suit,1979,true,1
Richard,Attenborough,Miracle on 34th Street,1994,true,0
Richard,Attenborough,Miracle on 34th Street,1994,true,1
Steve,Bacic,Deck the Halls,2005,false,0
Steve,Bacic,Deck the Halls,2005,false,1
Alec,Baldwin,Rise of the Guardians,2012,false,0
Alec,Baldwin,Rise of the Guardians,2012,false,1
Don,Beddoe,Bewitched (episode Humbug Not to Be Spoken Here - Season 4),1967,true,0
Don,Beddoe,Bewitched (episode Humbug Not to Be Spoken Here - Season 4),1967,true,1`

var _, schIn = untyped.NewUntypedSchema("first", "last", "film or show", "year")
var nameToTag, schOut = untyped.NewUntypedSchema("first", "last", "film or show", "year", "pre2000", "index")

func TestPipeline(t *testing.T) {
	buf := bytes.NewBuffer([]byte(inCSV))
	outBuf := bytes.NewBuffer([]byte{})

	afterFinishCalled := false
	afterFinishFunc := func() {
		afterFinishCalled = true
	}

	func() {
		csvInfo := &csv.CSVFileInfo{Delim: ',', HasHeaderLine: true, Columns: nil, EscapeQuotes: true}
		rd, _ := csv.NewCSVReader(ioutil.NopCloser(buf), csvInfo)
		wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(outBuf), schOut, csvInfo)

		tc := NewTransformCollection(
			NewNamedTransform("identity", identityTransFunc),
			NewNamedTransform("label", labelTransFunc),
			NewNamedTransform("dupe", dupeTransFunc),
			NewNamedTransform("append", appendColumnPre2000TransFunc),
		)

		inProcFunc := ProcFuncForReader(context.Background(), rd)
		outProcFunc := ProcFuncForWriter(context.Background(), wr)
		p := NewAsyncPipeline(inProcFunc, outProcFunc, tc, nil)

		p.RunAfter(func() { rd.Close(context.Background()) })
		p.RunAfter(func() { wr.Close(context.Background()) })
		p.RunAfter(afterFinishFunc)

		p.Start()
		p.Wait()
	}()

	assert.True(t, afterFinishCalled, "afterFinish func not called when pipeline ended")

	assert.Equal(t, strings.TrimSpace(outCSV), strings.TrimSpace(outBuf.String()), "output doesn't match expectation")
}

func TestAddingStages(t *testing.T) {
	buf := bytes.NewBuffer([]byte(inCSV))
	outBuf := bytes.NewBuffer([]byte{})

	afterFinishCalled := false
	afterFinishFunc := func() {
		afterFinishCalled = true
	}

	func() {
		csvInfo := &csv.CSVFileInfo{Delim: ',', HasHeaderLine: true, Columns: nil, EscapeQuotes: true}
		rd, _ := csv.NewCSVReader(ioutil.NopCloser(buf), csvInfo)
		wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(outBuf), schOut, csvInfo)

		tc := NewTransformCollection(
			NewNamedTransform("identity", identityTransFunc),
			NewNamedTransform("label", labelTransFunc),
		)

		addedStages := []NamedTransform{
			NewNamedTransform("dupe", dupeTransFunc),
			NewNamedTransform("append", appendColumnPre2000TransFunc),
		}

		inProcFunc := ProcFuncForReader(context.Background(), rd)
		outProcFunc := ProcFuncForWriter(context.Background(), wr)
		p := NewAsyncPipeline(inProcFunc, outProcFunc, tc, nil)
		for _, stage := range addedStages {
			p.AddStage(stage)
		}

		p.RunAfter(func() { rd.Close(context.Background()) })
		p.RunAfter(func() { wr.Close(context.Background()) })
		p.RunAfter(afterFinishFunc)

		p.Start()
		p.Wait()
	}()

	assert.True(t, afterFinishCalled, "afterFinish func not called when pipeline ended")

	assert.Equal(t, strings.TrimSpace(outCSV), strings.TrimSpace(outBuf.String()), "output doesn't match expectation")
}

func TestPartialPipeline(t *testing.T) {
	buf := bytes.NewBuffer([]byte(inCSV))
	outBuf := bytes.NewBuffer([]byte{})

	afterFinishCalled := false
	afterFinishFunc := func() {
		afterFinishCalled = true
	}

	var newOutCsv = `first,last,film or show,year,pre2000,index
New,Row,InAppendStage,2999,true,0
AnotherNew,Row,InAppendStage,3000,true,1
Tim,Allen,The Santa Clause,1994,true,0
Tim,Allen,The Santa Clause,1994,true,1
Tim,Allen,The Santa Clause 2,2002,false,0
Tim,Allen,The Santa Clause 2,2002,false,1
Tim,Allen,The Santa Clause 3: The Escape Clause,2006,false,0
Tim,Allen,The Santa Clause 3: The Escape Clause,2006,false,1
Ed,Asner,Elf,2003,false,0
Ed,Asner,Elf,2003,false,1
Ed,Asner,Christmas on the Bayou,2013,false,0
Ed,Asner,Christmas on the Bayou,2013,false,1
Ed,Asner,Elf: Buddy's Musical Christmas,2014,false,0
Ed,Asner,Elf: Buddy's Musical Christmas,2014,false,1
Fred,Astaire,The Man in the Santa Claus Suit,1979,true,0
Fred,Astaire,The Man in the Santa Claus Suit,1979,true,1
Richard,Attenborough,Miracle on 34th Street,1994,true,0
Richard,Attenborough,Miracle on 34th Street,1994,true,1
Steve,Bacic,Deck the Halls,2005,false,0
Steve,Bacic,Deck the Halls,2005,false,1
Alec,Baldwin,Rise of the Guardians,2012,false,0
Alec,Baldwin,Rise of the Guardians,2012,false,1
Don,Beddoe,Bewitched (episode Humbug Not to Be Spoken Here - Season 4),1967,true,0
Don,Beddoe,Bewitched (episode Humbug Not to Be Spoken Here - Season 4),1967,true,1`

	func() {
		csvInfo := &csv.CSVFileInfo{Delim: ',', HasHeaderLine: true, Columns: nil, EscapeQuotes: true}
		rd, _ := csv.NewCSVReader(ioutil.NopCloser(buf), csvInfo)
		wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(outBuf), schOut, csvInfo)

		addedStages := []NamedTransform {
			NewNamedTransform("identity", identityTransFunc),
			NewNamedTransform("label", labelTransFunc),
			NewNamedTransform("dupe", dupeTransFunc),
			NewNamedTransform("append", appendColumnPre2000TransFunc),
		}

		inProcFunc := ProcFuncForReader(context.Background(), rd)
		outProcFunc := ProcFuncForWriter(context.Background(), wr)

		p := NewPartialPipeline(inProcFunc)
		for _, stage := range addedStages {
			p.AddStage(stage)
		}

		// Can't start the pipeline until setting a sink
		assert.Panics(t, func() {
			p.Start()
		})

		p.SetOutput(outProcFunc)

		//New,Row,InAppendStage,2999,true,0
		var injectedColumns = map[uint64]string{
			0: "New",
			1: "Row",
			2: "InAppendStage",
			3: "2999",
			4: "true",
			5: "0",
		}
		injectedRow := untyped.NewRowFromTaggedStrings(schOut, injectedColumns)
		p.InjectRow("append", injectedRow)

		//AnotherNew,Row,InAppendStage,3000,true,1
		injectedColumns = map[uint64]string{
			0: "AnotherNew",
			1: "Row",
			2: "InAppendStage",
			3: "3000",
			4: "true",
			5: "1",
		}
		injectedRow = untyped.NewRowFromTaggedStrings(schOut, injectedColumns)
		p.InjectRow("append", injectedRow)

		p.RunAfter(func() { rd.Close(context.Background()) })
		p.RunAfter(func() { wr.Close(context.Background()) })
		p.RunAfter(afterFinishFunc)

		p.Start()

		// Now that the pipeline is started, other calls to set it up should panic
		assert.Panics(t, func() {
			p.SetOutput(func(p *Pipeline, ch <-chan RowWithProps, badRowChan chan<- *TransformRowFailure) {
			})
		})
		assert.Panics(t, func() {
			p.AddStage(NewNamedTransform("identity2", identityTransFunc))
		})
		assert.Panics(t, func() {
			p.InjectRow("identity", injectedRow)
		})

		p.Wait()
	}()

	assert.True(t, afterFinishCalled, "afterFinish func not called when pipeline ended")

	assert.Equal(t, strings.TrimSpace(newOutCsv), strings.TrimSpace(outBuf.String()), "output does not match expectation")
}

func TestAbort(t *testing.T) {
	buf := bytes.NewBuffer([]byte(inCSV))
	outBuf := bytes.NewBuffer([]byte{})

	afterFinishCalled := false
	afterFinishFunc := func() {
		afterFinishCalled = true
	}

	func() {
		csvInfo := &csv.CSVFileInfo{Delim: ',', HasHeaderLine: true, Columns: nil, EscapeQuotes: true}
		rd, _ := csv.NewCSVReader(ioutil.NopCloser(buf), csvInfo)
		wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(outBuf), schOut, csvInfo)

		var wg = sync.WaitGroup{}

		tc := NewTransformCollection(
			NewNamedTransform("identity", identityTransFunc),
			NewNamedTransform("dies", hangs(&wg)),
		)

		inProcFunc := ProcFuncForReader(context.Background(), rd)
		outProcFunc := ProcFuncForWriter(context.Background(), wr)
		p := NewAsyncPipeline(inProcFunc, outProcFunc, tc, nil)

		p.RunAfter(func() { rd.Close(context.Background()) })
		p.RunAfter(func() { wr.Close(context.Background()) })
		p.RunAfter(afterFinishFunc)

		p.Start()
		wg.Wait()
		p.Abort()
	}()

	assert.True(t, afterFinishCalled, "afterFinish func not called when pipeline ended")
}

// Returns a function that hangs right after signalling the given WaitGroup that it's done
func hangs(wg *sync.WaitGroup) func(inRow row.Row, props ReadableMap) ([]*TransformedRowResult, string) {
	wg.Add(1)
	return func(inRow row.Row, props ReadableMap) (results []*TransformedRowResult, s string) {
		i := 0
		fmt.Println("about to call done()")
		wg.Done()
		for {
			i++
		}
	}
}

func identityTransFunc(inRow row.Row, props ReadableMap) ([]*TransformedRowResult, string) {
	return []*TransformedRowResult{{inRow, nil}}, ""
}

func labelTransFunc(inRow row.Row, props ReadableMap) ([]*TransformedRowResult, string) {
	val, _ := inRow.GetColVal(nameToTag["year"])
	year, _ := strconv.ParseInt(string(val.(types.String)), 10, 32)
	return []*TransformedRowResult{
		{inRow, map[string]interface{}{"pre2000": year < 2000}},
	}, ""
}

func dupeTransFunc(inRow row.Row, props ReadableMap) ([]*TransformedRowResult, string) {
	r1, _ := inRow.SetColVal(nameToTag["index"], types.String("0"), schOut)
	r2, _ := inRow.SetColVal(nameToTag["index"], types.String("1"), schOut)
	return []*TransformedRowResult{
		{r1, map[string]interface{}{"dupe_index": 1}},
		{r2, map[string]interface{}{"dupe_index": 2}},
	}, ""
}

func appendColumnPre2000TransFunc(inRow row.Row, props ReadableMap) (rowData []*TransformedRowResult, badRowDetails string) {
	labelval, _ := props.Get("pre2000")

	isPre2000Str := "false"
	if boolVal, ok := labelval.(bool); ok && boolVal {
		isPre2000Str = "true"
	}

	// Update the column value if it's not already present
	var r1 row.Row = inRow
	if _, ok := inRow.GetColVal(nameToTag["pre2000"]); !ok {
		r1, _ = inRow.SetColVal(nameToTag["pre2000"], types.String(isPre2000Str), schOut)
	}
	return []*TransformedRowResult{
		{r1, nil},
	}, ""
}
