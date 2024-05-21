// Copyright 2024 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// TableNameSet is a simple set implementation providing standard set operations for table names.
type TableNameSet struct {
	items         map[doltdb.TableName]bool
	caseSensitive bool
}

// NewStrSet creates a set from a list of strings
func newTableNameSet(items []doltdb.TableName, caseSensitive bool) *TableNameSet {
	s := &TableNameSet{make(map[doltdb.TableName]bool, len(items)), caseSensitive}

	for _, item := range items {
		s.items[item] = true
	}

	return s
}

func NewTableNameSet() *TableNameSet {
	return newTableNameSet(nil, true)
}

func NewCaseInsensitiveTableNameSet(items []doltdb.TableName) *TableNameSet {
	// lower case the table names
	newItems := make([]doltdb.TableName, len(items))
	for i, item := range items {
		newItems[i] = item.ToLower()
	}
	return newTableNameSet(newItems, false)
}

// Add adds new items to the set
func (s *TableNameSet) Add(items ...doltdb.TableName) {
	for _, item := range items {
		if !s.caseSensitive {
			item = item.ToLower()
		}
		s.items[item] = true
	}
}

// Remove removes existing items from the set
func (s *TableNameSet) Remove(items ...doltdb.TableName) {
	for _, item := range items {
		if !s.caseSensitive {
			item = item.ToLower()
		}

		delete(s.items, item)
	}
}

// Contains returns true if the item being checked is already in the set.
func (s *TableNameSet) Contains(item doltdb.TableName) bool {
	if s == nil {
		return false
	}
	if !s.caseSensitive {
		item = item.ToLower()
	}

	_, present := s.items[item]
	return present
}

// ContainsAll returns true if all the items being checked are already in the set.
func (s *TableNameSet) ContainsAll(items []doltdb.TableName) bool {
	if s == nil {
		return false
	}

	for _, item := range items {
		if !s.caseSensitive {
			item = item.ToLower()
		}
		if _, present := s.items[item]; !present {
			return false
		}
	}

	return true
}

func (s *TableNameSet) Equals(other *TableNameSet) bool {
	// two string sets can be equal even if one is sensitive and the other is insensitive as long al the items are a
	// case sensitive match.
	ss := s.AsSlice()
	os := other.AsSlice()

	if len(ss) != len(os) {
		return false
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Less(ss[j])
	})
	sort.Slice(os, func(i, j int) bool {
		return os[i].Less(os[j])
	})
	
	for i := range ss {
		if ss[i] != os[i] {
			return false
		}
	}
	return true
}

// Size returns the number of unique elements in the set
func (s *TableNameSet) Size() int {
	if s == nil {
		return 0
	}
	return len(s.items)
}

// AsSlice converts the set to a slice of strings. If this is an insensitive set the resulting slice will be lowercase
// regardless of the case that was used when adding the string to the set.
func (s *TableNameSet) AsSlice() []doltdb.TableName {
	if s == nil {
		return nil
	}
	size := len(s.items)
	sl := make([]doltdb.TableName, size)

	i := 0
	for k := range s.items {
		sl[i] = k
		i++
	}

	return sl
}

// AsSortedSlice converts the set to a slice of strings. If this is an insensitive set the resulting slice will be lowercase
// regardless of the case that was used when adding the string to the set. The slice is sorted in ascending order.
func (s *TableNameSet) AsSortedSlice() []doltdb.TableName {
	if s == nil {
		return nil
	}
	slice := s.AsSlice()
	sort.Slice(slice, func(i, j int) bool {
		return slice[i].Less(slice[j])
	})
	return slice
}

// Iterate accepts a callback which will be called once for each element in the set until all items have been
// exhausted or callback returns false.
func (s *TableNameSet) Iterate(callBack func(doltdb.TableName) (cont bool)) {
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
func (s *TableNameSet) LeftIntersectionRight(other *TableNameSet) (left *TableNameSet, intersection *TableNameSet, right *TableNameSet) {
	left = NewTableNameSet()
	intersection = NewTableNameSet()
	right = NewTableNameSet()

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
func (s *TableNameSet) JoinStrings(sep string) string {
	slice := s.AsSlice()
	sort.Slice(slice, func(i, j int) bool {
		return slice[i].Less(slice[j])
	})
	ss := make([]string, len(slice))
	for i, v := range slice {
		ss[i] = v.String()
	}
	return strings.Join(ss, sep)
}
