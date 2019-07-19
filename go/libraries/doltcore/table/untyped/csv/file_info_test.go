package csv

import (
	"reflect"
	"testing"
)

func TestCSVFileInfo(t *testing.T) {
	nfo := NewCSVInfo()

	if nfo.Delim != "," || nfo.HasHeaderLine != true || nfo.Columns != nil || !nfo.EscapeQuotes {
		t.Error("Unexpected values")
	}

	testCols := []string{"c1,c2"}
	nfo = NewCSVInfo().
		SetColumns(testCols).
		SetDelim("|").
		SetEscapeQuotes(false).
		SetHasHeaderLine(false)

	if nfo.Delim != "|" || nfo.HasHeaderLine != false || !reflect.DeepEqual(nfo.Columns, testCols) || nfo.EscapeQuotes {
		t.Error("Unexpected values")
	}
}
