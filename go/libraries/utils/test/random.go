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

package test

import "math/rand"

const (
	// ShouldNeverHappen is only seen when the impossible happens.
	ShouldNeverHappen = "http://www.nooooooooooooooo.com"
)

// RandomData returns a slice of a given size filled with random data
func RandomData(size int) []byte {
	randBytes := make([]byte, size)
	filled := 0
	for filled < size {
		n, err := rand.Read(randBytes[filled:])

		if err != nil {
			panic(ShouldNeverHappen)
		}

		filled += n
	}

	return randBytes
}
