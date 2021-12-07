// Copyright 2019 Dolthub, Inc.
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

package csv

import (
	"bufio"
	"io"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func toPointer(input []string) []*string {
	output := make([]*string, len(input))
	for i := range input {
		var pointer *string
		pointer = &input[i]
		output[i] = pointer
	}
	return output
}

func fromPointer(input []*string) []string {
	output := make([]string, len(input))
	for i := range input {
		var value string
		value = *input[i]
		output[i] = value
	}
	return output
}

func addr(s string) *string {
	return &s
}

func csvReaderRead(s string, delim string) ([]*string, error) {
	r := strings.NewReader(s)
	br := bufio.NewReaderSize(r, 1)

	csvr := CSVReader{
		closer:          nil,
		bRd:             br,
		sch:             sql.PrimaryKeySchema{},
		isDone:          false,
		nbf:             nil,
		delim:           []byte(delim),
		fieldsPerRecord: 0,
	}
	strs, err := csvr.csvReadRecords(nil)
	if err == io.EOF {
		err = nil
	}
	return strs, err
}

func TestCSVSplitLine(t *testing.T) {
	splitTests := []struct {
		ToSplit        string
		Delim          string
		expectedTokens []*string
		escapeQuotes   bool
		expectErr      bool
	}{
		{", ,\t,\t ,\t, \t", ",", []*string{nil, nil, nil, nil, nil, nil}, true, false},
		{"\"\",\" \",\"\t\",\"\r\",\"\n\",\" \r\n\t\"", ",", toPointer([]string{"", " ", "\t", "\r", "\n", " \n\t"}), true, false},
		{`"", one, ""`, ",", toPointer([]string{``, `one`, ``}), true, false},
		{`"", "", ""`, ",", toPointer([]string{``, ``, ``}), true, false},
		{`"", "one", ""`, ",", toPointer([]string{``, `one`, ``}), true, false},
		{`"""""","one"`, ",", toPointer([]string{`""`, `one`}), true, false},
		{`,,`, ",", []*string{nil, nil, nil}, true, false},
		{`one`, ",", toPointer([]string{"one"}), true, false},
		{`one,`, ",", []*string{addr("one"), nil}, true, false},
		{`one,two, three`, ",", toPointer([]string{"one", "two", "three"}), true, false},
		{`one,"two", three`, ",", toPointer([]string{"one", "two", "three"}), true, false},
		{`one," two", three`, ",", toPointer([]string{"one", " two", "three"}), true, false},
		{`one,"two, three"`, ",", toPointer([]string{"one", "two, three"}), true, false},
		{`one,"""two three"""`, ",", toPointer([]string{"one", `"two three"`}), true, false},
		{`one,"two, ""three"""`, ",", toPointer([]string{"one", `two, "three"`}), true, false},
		{`"brian ""the great"" hendriks",mr.,1.7526`, ",", toPointer([]string{`brian "the great" hendriks`, "mr.", "1.7526"}), true, false},
		{`col1,"Industriepark ""De Bruwaan""",col3`, ",", toPointer([]string{"col1", `Industriepark "De Bruwaan"`, "col3"}), true, false},
		{`|a|`, "|", []*string{nil, addr("a"), nil}, true, false},
		{`72470|30|0|40|0||||`, "|", []*string{addr("72470"), addr("30"), addr("0"), addr("40"), addr("0"), nil, nil, nil, nil}, true, false},
		{`"one","two"`, ",", toPointer([]string{`one`, `two`}), true, false},
		{`one,  two`, ",", toPointer([]string{`one`, `two`}), true, false},
		{`one,"  two"`, ",", toPointer([]string{`one`, `  two`}), true, false},
		{
			`23660|1300|"Beef, brisket, flat half, separable lean and fat, trimmed to 1/8"""`,
			"|",
			toPointer([]string{"23660", "1300", `Beef, brisket, flat half, separable lean and fat, trimmed to 1/8"`}),
			true,
			false,
		},
		{`72470<delim>30<delim>0<delim>40<delim>0<delim>"<delim>"<delim><delim><delim>`, "<delim>", []*string{addr("72470"), addr("30"), addr("0"), addr("40"), addr("0"), addr("<delim>"), nil, nil, nil}, true, false},
		{`72470<delim>30<delim>0<delim>40<delim>0<delim>"""<delim>"""<delim><delim><delim>`, "<delim>", []*string{addr("72470"), addr("30"), addr("0"), addr("40"), addr("0"), addr(`"<delim>"`), nil, nil, nil}, true, false},
		{`"the ""word"" is true","a ""quoted-field"""`, ",", toPointer([]string{`the "word" is true`, `a "quoted-field"`}), true, false},
		{`"not closed,`, ",", toPointer([]string{}), true, true},
		{`"closed", "not closed,`, ",", toPointer([]string{"closed"}), true, true},
	}

	for _, test := range splitTests {
		results, err := csvReaderRead(test.ToSplit, test.Delim)

		if (err != nil) != test.expectErr {
			if test.expectErr {
				t.Error("Expected an error that didn't occur.")
			} else {
				t.Error("Unexpected error: " + err.Error())
			}
		}

		if err != nil {
			continue
		}

		if len(results) != len(test.expectedTokens) {
			t.Error(test.ToSplit + " split test failure")
			continue
		}

		for i, token := range results {
			if token == nil && test.expectedTokens[i] == nil {
				break
			}
			if token != nil && test.expectedTokens[i] == nil {
				t.Errorf("%s split test failure. expected: %v, actual: %v\n", test.ToSplit, test.expectedTokens, results)
				break
			}
			if token == nil && test.expectedTokens[i] != nil {
				t.Errorf("%s split test failure. expected: %v, actual: %v\n", test.ToSplit, test.expectedTokens, results)
				break
			}
			if *token != *test.expectedTokens[i] {
				t.Errorf("%s split test failure. expected: %v, actual: %v\n", test.ToSplit, fromPointer(test.expectedTokens), fromPointer(results))
				break
			}
		}
	}
}
