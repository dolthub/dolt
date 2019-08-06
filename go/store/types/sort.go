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

package types

import "sort"

type SortData interface {
	Len() int
	Less(i, j int) (bool, error)
	Swap(i, j int)
}

type sortWrapper struct {
	data SortData
	err  error
}

func (sw *sortWrapper) Len() int {
	return sw.data.Len()
}

func (sw *sortWrapper) Less(i, j int) bool {
	if sw.err != nil {
		return false
	}

	isLess, err := sw.data.Less(i, j)

	if err != nil {
		sw.err = err
	}

	return isLess
}

func (sw *sortWrapper) Swap(i, j int) {
	sw.data.Swap(i, j)
}

func SortWithErroringLess(data SortData) error {
	sw := &sortWrapper{data, nil}
	sort.Stable(sw)

	return sw.err
}

func SearchWithErroringLess(n int, f func(i int) (bool, error)) (int, error) {
	var err error
	fWrapper := func(i int) bool {
		if err != nil {
			return false
		}

		var isLess bool
		isLess, err = f(i)

		return isLess
	}

	idx := sort.Search(n, fWrapper)

	return idx, err
}
