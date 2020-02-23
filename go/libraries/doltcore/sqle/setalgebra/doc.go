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

/*
Package setalgebra provides the ability to perform algebraic set operations on mathematical sets built directly on noms
types. Unlike standard sets in computer science, which define a finitely sized collection of unordered unique values,
sets in mathematics are defined as a well-defined collection of distint objects. This can include infinitely sized
groupings such as the set of all real numbers greater than 0.

See https://en.wikipedia.org/wiki/Set_(mathematics)

There are 3 types of sets defined in this package: FiniteSet, Interval, and CompositeSet.

FiniteSet is your typical computer science set representing a finite number of unique objects stored in a map. An
example would be the set of strings {"red","blue","green"}, or the set of numbers {5, 73, 127}.

Interval is a set which can be written as an inequality such as {n | n > 0} (set of all numbers n such that n > 0) or a
chained comparison {n | 0.0 <= n <= 1.0 } (set of all floating point values between 0.0 and 1.0)

CompositeSet is a set which is made up of a FiniteSet and one or more non overlapping intervals such as
{n | n < 0 or n > 100} (set of all numbers n below 0 or greater than 100) this set contains 2 non overlapping intervals
and an empty finite set. Alternatively {n | n < 0 or {5,10,15}} (set of all numbers n below 0 or n equal to 5, 10 or 15)
which would be represented by one Interval and a FiniteSet containing 5,10, and 15.

There are 2 special sets also defined in this package: EmptySet, UniversalSet.

The EmptySet is a set that has no values in it.  It has the property that when unioned with any set X, X will be the
result, and if intersected with any set X, EmptySet will be returned.

The UniversalSet is the set containing all values.  It has the property that when unioned with any set X, UniversalSet is
returned and when intersected with any set X, X will be returned.
*/
package setalgebra
