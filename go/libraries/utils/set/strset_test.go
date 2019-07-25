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

package set

import (
	"reflect"
	"sort"
	"testing"
)

func TestStrSet(t *testing.T) {
	strSet := NewStrSet([]string{"a", "b", "c", "c", "d", "d", "d"})

	if !isAsExpected(strSet, []string{"a", "b", "c", "d"}) {
		t.Error("Set doesn't match expectation after creation", strSet.AsSlice())
	}

	strSet.Add("a")
	strSet.Add("e")

	if !isAsExpected(strSet, []string{"a", "b", "c", "d", "e"}) {
		t.Error("Set doesn't match expectation after adds", strSet.AsSlice())
	}

	joinedStr := strSet.JoinStrings(",")

	if joinedStr != "a,b,c,d,e" {
		t.Error("JoinStrings failed to yield correct result:", joinedStr)
	}
}

// tests Size(), ContainsAll, Contains(), and AsSlice()
func isAsExpected(strSet *StrSet, expected []string) bool {
	if strSet.Size() != len(expected) {
		return false
	}

	if !strSet.ContainsAll(expected) {
		return false
	}

	if strSet.Contains("This should fail as it shouldn't be in the set") {
		return false
	}

	actual := strSet.AsSlice()

	sort.Strings(expected)
	sort.Strings(actual)

	return reflect.DeepEqual(actual, expected)
}

func TestUnique(t *testing.T) {
	uStrs := Unique([]string{"a", "b", "b", "c", "c", "c"})

	sort.Strings(uStrs)

	if !reflect.DeepEqual(uStrs, []string{"a", "b", "c"}) {
		t.Error(`Unique failed. expected: ["a", "b", "c"] actual:`, uStrs)
	}
}

func TestIterateDifferent(t *testing.T) {
	strSet1 := NewStrSet([]string{"a", "b", "c", "d"})
	strSet2 := NewStrSet([]string{"e", "f", "g"})

	strSet1.Iterate(func(s string) (cont bool) {
		if strSet2.Contains(s) {
			t.Error(s, " should not be in strSet2")
		}

		return true
	})

	if strSet1.ContainsAll(strSet2.AsSlice()) {
		t.Error("strSet1 does not contain all or any of strSet2")
	}
}
