package cli

import (
	"testing"
)

func TestToIndentedParagraph(t *testing.T) {
	tests := map[string]string{
		"":                      "",
		"Short test":            "  Short test",
		"Shows the commit logs": "  Shows the commit\n  logs",
		"Sample long line which should get split": `  Sample long line
  which should get
  split`,
	}

	for testInput, expectedOut := range tests {
		out := ToIndentedParagraph(testInput, "  ", 20)

		if out != expectedOut {
			t.Errorf("\nFor %s\nexpect:\n%s\nreceived:\n%s", testInput, expectedOut, out)
		}
	}
}

func TestEmbolden(t *testing.T) {
	tests := map[string]string{
		"":                       "",
		"no boldness":            "no boldness",
		"has\nnewline":           "has\nnewline",
		"has<b>start":            "has<b>start",
		"has<b>end":              "has<b>end",
		"end</b>before<b>start":  "end</b>before<b>start",
		"<b>one</b> end":         bold.Sprint("one") + " end",
		"<b>both ends</b>":       bold.Sprint("both ends"),
		"other <b>end</b>":       "other " + bold.Sprint("end"),
		"extra</b> <b>close</b>": "extra</b> " + bold.Sprint("close"),
		"<b>multiple</b><b>bold</b> <b>sections</b>": bold.Sprint("multiple") + bold.Sprint("bold") + " " + bold.Sprint("sections"),
	}

	for inStr, expectedOut := range tests {
		actualOut := embolden(inStr)

		if actualOut != expectedOut {
			t.Error("in:", inStr, "out:", actualOut, "expected:", expectedOut)
		}
	}
}
