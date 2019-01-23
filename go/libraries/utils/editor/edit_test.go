package editor

import (
	"reflect"
	"testing"
)

func TestGetEditorString(t *testing.T) {
	tests := []struct {
		inStr        string
		expectedCmd  string
		expectedArgs []string
	}{
		{"", "", []string{}},
		{"vim", "vim", []string{}},
		{"subl -n -w", "subl", []string{"-n", "-w"}},
	}

	for _, test := range tests {
		actualCmd, actualArgs := getEditorString(test.inStr)

		if actualCmd != test.expectedCmd {
			t.Error(actualCmd, "!=", test.expectedCmd)
		}

		if !reflect.DeepEqual(actualArgs, test.expectedArgs) {
			t.Error(actualCmd, "!=", test.expectedCmd)
		}
	}
}
