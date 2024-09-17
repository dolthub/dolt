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

package editor

import (
	"reflect"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/osutil"
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
		val, err := OpenTempEditor(test.editorStr, test.initialContents, "")

		if err != nil {
			t.Error(err)
		}

		if val != test.expected {
			t.Error(val, "!=", test.expected)
		}
	}
}
