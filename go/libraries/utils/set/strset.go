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

package set

import (
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/funcitr"
)

// StrSet is a simple set implementation providing standard set operations for strings.
type StrSet struct {
	items         map[string]bool
	caseSensitive bool
}

// NewStrSet creates a set from a list of strings
func newStrSet(items []string, caseSensitive bool) *StrSet {
	s := &StrSet{make(map[string]bool, len(items)), caseSensitive}

	for _, item := range items {
		s.items[item] = true
	}

	return s
}

func NewEmptyStrSet() *StrSet {
	return newStrSet(nil, true)
}

func NewStrSet(items []string) *StrSet {
	return newStrSet(items, true)
}

func NewCaseInsensitiveStrSet(items []string) *StrSet {
	lwrStrs := funcitr.MapStrings(items, strings.ToLower)
	return newStrSet(lwrStrs, false)
}

// Add adds new items to the set
func (s *StrSet) Add(items ...string) {
	for _, item := range items {
		if !s.caseSensitive {
			item = strings.ToLower(item)
		}
		s.items[item] = true
	}
}

// Remove removes existing items from the set
func (s *StrSet) Remove(items ...string) {
	for _, item := range items {
		if !s.caseSensitive {
			item = strings.ToLower(item)
		}

		delete(s.items, item)
	}
}

// Contains returns true if the item being checked is already in the set.
func (s *StrSet) Contains(item string) bool {
	if s == nil {
		return false
	}
	if !s.caseSensitive {
		item = strings.ToLower(item)
	}

	_, present := s.items[item]
	return present
}

// ContainsAll returns true if all the items being checked are already in the set.
func (s *StrSet) ContainsAll(items []string) bool {
	if s == nil {
		return false
	}
	if !s.caseSensitive {
		items = funcitr.MapStrings(items, strings.ToLower)
	}

	for _, item := range items {
		if _, present := s.items[item]; !present {
			return false
		}
	}

	return true
}

func (s *StrSet) Equals(other *StrSet) bool {
	// two string sets can be equal even if one is sensitive and the other is insensitive as long al the items are a
	// case sensitive match.
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
	if s == nil {
		return 0
	}
	return len(s.items)
}

// AsSlice converts the set to a slice of strings. If this is an insensitive set the resulting slice will be lowercase
// regardless of the case that was used when adding the string to the set.
func (s *StrSet) AsSlice() []string {
	if s == nil {
		return nil
	}
	size := len(s.items)
	sl := make([]string, size)

	i := 0
	for k := range s.items {
		sl[i] = k
		i++
	}

	return sl
}

// AsSortedSlice converts the set to a slice of strings. If this is an insensitive set the resulting slice will be lowercase
// regardless of the case that was used when adding the string to the set. The slice is sorted in ascending order.
func (s *StrSet) AsSortedSlice() []string {
	if s == nil {
		return nil
	}
	slice := s.AsSlice()
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] < slice[j]
	})
	return slice
}

// Iterate accepts a callback which will be called once for each element in the set until all items have been
// exhausted or callback returns false.
func (s *StrSet) Iterate(callBack func(string) (cont bool)) {
	if s == nil {
		return
	}
	for k := range s.items {
		if !callBack(k) {
			break
		}
	}
}

// LeftIntersectionRight takes a slice of strings and returns a slice of strings containing the intersection with the
// set, and a slice of strings for the ones missing from the set.
func (s *StrSet) LeftIntersectionRight(other *StrSet) (left *StrSet, intersection *StrSet, right *StrSet) {
	left = NewStrSet(nil)
	intersection = NewStrSet(nil)
	right = NewStrSet(nil)

	for os := range other.items {
		if s.Contains(os) {
			intersection.Add(os)
		} else {
			right.Add(os)
		}
	}
	for ss := range s.items {
		if !intersection.Contains(ss) {
			left.Add(ss)
		}
	}

	return left, intersection, right
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
