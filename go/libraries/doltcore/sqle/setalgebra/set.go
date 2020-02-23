// Copyright 2020 Liquidata, Inc.
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

package setalgebra

// Set is a well-defined collection of distint objects
type Set interface {
	// Union takes the current set and another set and returns a set containing all values from both.
	Union(other Set) (Set, error)
	// Interset takes the current set and another set and returns a set containing the values that are in both
	Intersect(other Set) (Set, error)
}

// EmptySet is a Set implementation that has no values
type EmptySet struct{}

// Union takes the current set and another set and returns a set containing all values from both. When EmptySet is
// unioned against any other set X, X will be the result.
func (es EmptySet) Union(other Set) (Set, error) {
	return other, nil
}

// Intersect takes the current set and another set and returns a set containing the values that are in both. When
// EmptySet is intersected with any other set X, EmptySet will be returned.
func (es EmptySet) Intersect(other Set) (Set, error) {
	return es, nil
}

// UniversalSet is the set containing all values
type UniversalSet struct{}

// Union takes the current set and another set and returns a set containing all values from both. When
// UniversalSet is unioned with any other set X, UniversalSet will be returned.
func (us UniversalSet) Union(other Set) (Set, error) {
	return us, nil
}

// Interset takes the current set and another set and returns a set containing the values that are in both. When
// UniversalSet is intersected with any other set X, X will be the result.
func (us UniversalSet) Intersect(other Set) (Set, error) {
	return other, nil
}
