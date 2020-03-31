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
	"sort"
	"strings"
)

var emptyInstance = struct{}{}

// StrSet is a simple set implementation providing standard set operations for strings.
type StrSet struct {
	items map[string]interface{}
}

// NewStrSet creates a set from a list of strings
func NewStrSet(items []string) *StrSet {
	s := &StrSet{make(map[string]interface{}, len(items))}

	if items != nil {
		for _, item := range items {
			s.items[item] = emptyInstance
		}
	}

	return s
}

// Add adds new items to the set
func (s *StrSet) Add(items ...string) {
	for _, item := range items {
		s.items[item] = emptyInstance
	}
}


// Remove removes existing items from the set
func (s *StrSet) Remove(items ...string) {
	for _, item := range items {
		delete(s.items, item)
	}
}

// Contains returns true if the item being checked is already in the set.
func (s *StrSet) Contains(item string) bool {
	_, present := s.items[item]
	return present
}

// ContainsAll returns true if all the items being checked are already in the set.
func (s *StrSet) ContainsAll(items []string) bool {
	for _, item := range items {
		if _, present := s.items[item]; !present {
			return false
		}
	}

	return true
}

func (s *StrSet) Equals(other *StrSet) bool {
	ss := s.AsSlice()
	os := other.AsSlice()
	sort.Strings(ss)
	sort.Strings(os)

	if len(ss) != len(os) {
		return false
	}

	for i := range ss {
		if ss[i] != os[i] {
			return false
		}
	}
	return true
}

// Size returns the number of unique elements in the set
func (s *StrSet) Size() int {
	return len(s.items)
}

// AsSlice converts the set to a slice of strings
func (s *StrSet) AsSlice() []string {
	size := len(s.items)
	sl := make([]string, size)

	i := 0
	for k := range s.items {
		sl[i] = k
		i++
	}

	return sl
}

// Iterate accepts a callback which will be called once for each element in the set until all items have been
// exhausted or callback returns false.
func (s *StrSet) Iterate(callBack func(string) (cont bool)) {
	for k := range s.items {
		if !callBack(k) {
			break
		}
	}
}

// IntersectionAndMissing takes a slice of strings and returns a slice of strings containing the intersection with the
// set, and a slice of strings for the ones missing from the set.
func (s *StrSet) IntersectAndMissing(other []string) (intersection []string, missing []string) {
	for _, str := range other {
		if s.Contains(str) {
			intersection = append(intersection, str)
		} else {
			missing = append(missing, str)
		}
	}

	return intersection, missing
}

// JoinStrings returns the sorted values from the set concatenated with a given sep
func (s *StrSet) JoinStrings(sep string) string {
	strSl := s.AsSlice()
	sort.Strings(strSl)
	return strings.Join(strSl, sep)
}

// Unique will return a slice of unique strings given an input slice
func Unique(strs []string) []string {
	return NewStrSet(strs).AsSlice()
}
