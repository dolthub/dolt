package editor

import (
	"reflect"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/osutil"
)

func TestGetCmdNameAndArgsForEditor(t *testing.T) {
	tests := []struct {
		inStr        string
		expectedCmd  string
		expectedArgs []string
	}{
		{"vim", "vim", []string{}},
		{"subl -n -w", "subl", []string{"-n", "-w"}},
		{`'C:\Program Files\notepadpp\notepadpp.exe' -arg1 -arg2`, `C:\Program Files\notepadpp\notepadpp.exe`, []string{"-arg1", "-arg2"}},
		{`"C:\Program Files\notepadpp\notepadpp.exe" -arg1 -arg2`, `C:\Program Files\notepadpp\notepadpp.exe`, []string{"-arg1", "-arg2"}},
	}
	for _, test := range tests {
		actualCmd, actualArgs := getCmdNameAndArgsForEditor(test.inStr)

		if actualCmd != test.expectedCmd {
			t.Error(actualCmd, "!=", test.expectedCmd)
		}

		if !reflect.DeepEqual(actualArgs, test.expectedArgs) {
			t.Error(actualArgs, "!=", test.expectedArgs)
		}
	}
}

func TestOpenCommitEditor(t *testing.T) {
	if osutil.IsWindows {
		t.Skip("Invalid test on Windows as /bin/sh does not exist")
	}
	tests := []struct {
		editorStr       string
		initialContents string
		expected        string
	}{
		{`/bin/sh -c 'printf "this is a test" > $1' -- `, "", "this is a test"},
		{`/bin/sh -c 'printf "this is a test" > $1' -- `, "Initial contents: ", "this is a test"},
	}

	for _, test := range tests {
		val, err := OpenCommitEditor(test.editorStr, test.initialContents)

		if err != nil {
			t.Error(err)
		}

		if val != test.expected {
			t.Error(val, "!=", test.expected)
		}
	}
}
