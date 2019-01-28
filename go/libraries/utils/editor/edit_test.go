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
