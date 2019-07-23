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

package errhand

import (
	"errors"
	"testing"
)

func TestIndent(t *testing.T) {
	tests := []struct {
		toIndent string
		expected string
	}{
		{"", "\t"},
		{"one", "\tone"},
		{"one\ntwo\nthree", "\tone\n\ttwo\n\tthree"},
		{"one\n\n\tthree", "\tone\n\t\n\t\tthree"},
	}

	for _, test := range tests {
		result := indent(test.toIndent, "\t")

		if result != test.expected {
			t.Error(test.toIndent, "returned:", result, "expected:", test.expected)
		}
	}
}

func TestDError(t *testing.T) {
	rootCause := BuildDError("This is test %d of %f", 1, 1.0).
		AddDetails("The %[1]s that happened happened because of %[1]s", "stuff").
		AddDetails("details 3").
		Build()

	derr := BuildDError("More Badness occurred.").AddDetails("details 1").AddDetails("details 2").AddCause(rootCause).Build()
	derr = BuildDError("Badness occurred.").AddDetails("details 0").AddCause(derr).Build()

	t.Log(derr.Error())
	t.Log(derr.Verbose())
}

func TestBuildIf(t *testing.T) {
	derr := BuildIf(nil, "doesn't matter %s", "something").
		AddDetails("details").
		AddCause(nil).
		Build()

	if derr != nil {
		t.Fatal("Should not build a display error if err is nil")
	}

	derr2 := BuildIf(errors.New("valid"), "doesn't matter %s", "something").
		AddDetails("details").
		AddCause(nil).
		Build()

	if derr2 == nil {
		t.Fatal("Should have built a valid display error.")
	}
}
