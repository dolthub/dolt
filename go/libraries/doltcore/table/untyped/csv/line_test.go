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

package csv

import "testing"

func TestCSVSplitLine(t *testing.T) {
	splitTests := []struct {
		ToSplit        string
		Delim          string
		expectedTokens []string
		escapeQuotes   bool
		expectErr      bool
	}{
		{`"", one, ""`, ",", []string{`""`, `one`, `""`}, false, false},
		{`"", one, ""`, ",", []string{`""`, `one`, `""`}, true, false},
		{`"", "one", ""`, ",", []string{`""`, `"one"`, `""`}, false, false},
		{`"", "one", ""`, ",", []string{`""`, "one", `""`}, true, false},
		{``, ",", []string{""}, true, false},
		{`one`, ",", []string{"one"}, true, false},
		{`one,`, ",", []string{"one", ""}, true, false},
		{`one,two, three`, ",", []string{"one", "two", "three"}, true, false},
		{`one,"two", three`, ",", []string{"one", "two", "three"}, true, false},
		{`one," two", three`, ",", []string{"one", " two", "three"}, true, false},
		{`one," two", three`, ",", []string{"one", `" two"`, "three"}, false, false},
		{`one,"two, three"`, ",", []string{"one", "two, three"}, true, false},
		{`one,""two three""`, ",", []string{"one", `"two three"`}, true, false},
		{`one,"two, ""three""`, ",", []string{"one", `two, "three"`}, true, false},
		{`brian ""the great"" hendriks,mr.,1.7526`, ",", []string{`brian "the great" hendriks`, "mr.", "1.7526"}, true, false},
		{`col1,"Industriepark ""De Bruwaan""",col3`, ",", []string{"col1", `Industriepark "De Bruwaan"`, "col3"}, true, false},
		{`|a|`, "|", []string{"", "a", ""}, true, false},
		{`72470|30|0|40|0||||`, "|", []string{"72470", "30", "0", "40", "0", "", "", "", ""}, true, false},
		{`"one","two"`, ",", []string{`"one"`, `"two"`}, false, false},
		{`"one","two"`, ",", []string{`one`, `two`}, true, false},
		{`one,  two`, ",", []string{`one`, `two`}, true, false},
		{`one,"  two"`, ",", []string{`one`, `  two`}, true, false},
		{
			`23660|1300|"Beef, brisket, flat half, separable lean and fat, trimmed to 1/8"""`,
			"|",
			[]string{"23660", "1300", `Beef, brisket, flat half, separable lean and fat, trimmed to 1/8"`},
			true,
			false,
		},
		{`72470<delim>30<delim>0<delim>40<delim>0<delim>"<delim>"<delim><delim><delim>`, "<delim>", []string{"72470", "30", "0", "40", "0", "<delim>", "", "", ""}, true, false},
		{`72470<delim>30<delim>0<delim>40<delim>0<delim>"""<delim>"""<delim><delim><delim>`, "<delim>", []string{"72470", "30", "0", "40", "0", `"<delim>"`, "", "", ""}, true, false},
		{`"the ""word"" is true","a ""quoted-field"""`, ",", []string{`the "word" is true`, `a "quoted-field"`}, true, false},
		{`"not closed,`, ",", []string{}, true, true},
		{`"closed", "not closed,`, ",", []string{"closed"}, true, true},
	}

	for _, test := range splitTests {
		results, err := csvSplitLine(test.ToSplit, test.Delim, test.escapeQuotes)

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
			if token != test.expectedTokens[i] {
				t.Errorf("%s split test failure. expected: %v, actual: %v", test.ToSplit, test.expectedTokens, results)
				break
			}
		}
	}
}
