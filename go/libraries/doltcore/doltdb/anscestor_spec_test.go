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

package doltdb

import (
	"reflect"
	"testing"
)

func TestParseInstructions(t *testing.T) {
	tests := []struct {
		inputStr  string
		expected  []int
		expectErr bool
	}{
		{"", []int{}, false},
		{"^", []int{0}, false},
		{"^1", []int{0}, false},
		{"~", []int{0}, false},
		{"~1", []int{0}, false},
		{"^10", []int{9}, false},
		{"~3", []int{0, 0, 0}, false},
		{"^^", []int{0, 0}, false},
		{"^2~3^5", []int{1, 0, 0, 0, 4}, false},
		{"invalid", nil, true},
	}

	for _, test := range tests {
		actual, actualErr := parseInstructions(test.inputStr)

		if actualErr != nil {
			if !test.expectErr {
				t.Error(test.inputStr, "- unexpected err")
			}
		} else if !reflect.DeepEqual(test.expected, actual) {
			t.Error("Error parsing", test.inputStr)
		}
	}
}

func TestSplitAncestorSpec(t *testing.T) {
	tests := []struct {
		inputStr         string
		expectedCSpecStr string
		expectedASpecStr string
		expectErr        bool
	}{
		{"master", "master", "", false},
		{"MASTER^1", "MASTER", "^1", false},
		{"head~3^^", "head", "~3^^", false},
		{"HEAD~3^^", "HEAD", "~3^^", false},
		{"branch^invalid", "", "", true},
	}

	for _, test := range tests {
		actualCSpecStr, actualASpec, actualErr := SplitAncestorSpec(test.inputStr)

		if actualErr != nil {
			if !test.expectErr {
				t.Error(test.inputStr, "- unexpected err")
			}
		} else if actualCSpecStr != test.expectedCSpecStr {
			t.Error(test.inputStr, "- actual commit spcec:", actualCSpecStr, "expected commit spec:", test.expectedCSpecStr)
		} else if actualASpec.SpecStr != test.expectedASpecStr {
			t.Error(test.inputStr, "- actual commit spcec:", actualCSpecStr, "expected commit spec:", test.expectedCSpecStr)
		}
	}
}
