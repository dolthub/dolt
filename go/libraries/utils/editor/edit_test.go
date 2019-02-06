package editor

import (
	"reflect"
	"testing"
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
	tests := []struct {
		editorStr       string
		initialContents string
		expected        string
	}{
		{`python -c 'import sys f = open(sys.argv[1], "w+") f.write("this is a test") f.close()'`, "", "this is a test"},
		{`python -c 'import sys f = open(sys.argv[1], "w+") f.write("this is a test") f.close()'`, "Initial contents: ", "Initial contents: this is a test"},
	}

	for _, test := range tests {
		val, err := OpenCommitEditor(test.editorStr, test.initialContents)

		if err != nil {
			t.Error(err)
		}

		if val != test.initialContents {
			t.Error(val, "!=", test.initialContents)
		}
	}
}
