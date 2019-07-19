package csv

import "testing"

func TestCSVSplitLine(t *testing.T) {
	splitTests := []struct {
		ToSplit        string
		Delim          string
		expectedTokens []string
		escapeQuotes   bool
	}{
		{``, ",", []string{""}, true},
		{`one`, ",", []string{"one"}, true},
		{`one,`, ",", []string{"one", ""}, true},
		{`one,two, three`, ",", []string{"one", "two", "three"}, true},
		{`one,"two", three`, ",", []string{"one", "two", "three"}, true},
		{`one," two", three`, ",", []string{"one", " two", "three"}, true},
		{`one," two", three`, ",", []string{"one", `" two"`, "three"}, false},
		{`one,"two, three"`, ",", []string{"one", "two, three"}, true},
		{`one,""two three""`, ",", []string{"one", `"two three"`}, true},
		{`one,"two, ""three""`, ",", []string{"one", `two, "three"`}, true},
		{`brian ""the great"" hendriks,mr.,1.7526`, ",", []string{`brian "the great" hendriks`, "mr.", "1.7526"}, true},
		{`col1,"Industriepark ""De Bruwaan""",col3`, ",", []string{"col1", `Industriepark "De Bruwaan"`, "col3"}, true},
		{`|a|`, "|", []string{"", "a", ""}, true},
		{`72470|30|0|40|0||||`, "|", []string{"72470", "30", "0", "40", "0", "", "", "", ""}, true},
		{`"one","two"`, ",", []string{`"one"`, `"two"`}, false},
		{`"one","two"`, ",", []string{`one`, `two`}, true},
		{`one,  two`, ",", []string{`one`, `two`}, true},
		{`one,"  two"`, ",", []string{`one`, `  two`}, true},
		{
			`23660|1300|"Beef, brisket, flat half, separable lean and fat, trimmed to 1/8"""`,
			"|",
			[]string{"23660", "1300", `Beef, brisket, flat half, separable lean and fat, trimmed to 1/8"`},
			true,
		},
		{`72470<delim>30<delim>0<delim>40<delim>0<delim>"<delim>"<delim><delim><delim>`, "<delim>", []string{"72470", "30", "0", "40", "0", "<delim>", "", "", ""}, true},
		{`72470<delim>30<delim>0<delim>40<delim>0<delim>"""<delim>"""<delim><delim><delim>`, "<delim>", []string{"72470", "30", "0", "40", "0", `"<delim>"`, "", "", ""}, true},
	}

	for _, test := range splitTests {
		results := csvSplitLine(test.ToSplit, test.Delim, test.escapeQuotes)

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
