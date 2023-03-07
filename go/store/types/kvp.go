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

package types

import "context"

// KVP is a simple key value pair
type KVP struct {
	// Key is the key
	Key LesserValuable

	// Val is the value
	Val Valuable
}

// KVPSlice is a slice of KVPs that implements sort.Interface
type KVPSlice []KVP

type KVPSort struct {
	Values []KVP
}

// Len returns the size of the slice
func (kvps KVPSort) Len() int {
	return len(kvps.Values)
}

// Less returns a bool representing whether the key at index i is less than the key at index j
func (kvps KVPSort) Less(ctx context.Context, nbf *NomsBinFormat, i, j int) (bool, error) {
	return kvps.Values[i].Key.Less(ctx, nbf, kvps.Values[j].Key)
}

// Swap swaps the KVP at index i with the KVP at index j
func (kvps KVPSort) Swap(i, j int) {
	kvps.Values[i], kvps.Values[j] = kvps.Values[j], kvps.Values[i]
}
