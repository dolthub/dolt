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

package schema

import (
	"crypto/sha512"
	"encoding/binary"
	"github.com/liquidata-inc/dolt/go/store/types"
	"math/rand"
)

const (
	// TODO: increase ReservedTagMin to 1 << 63 once numeric marshalling is fixed
	// ReservedTagMin is the start of a range of tags which the user should not be able to use in their schemas.
	ReservedTagMin uint64 = 1 << 50
)

// AutoGenerateTag generates a random tag that doesn't exist in the provided SuperSchema
func AutoGenerateTag(rootSS *SuperSchema, schemaKinds []types.NomsKind) uint64 {
	// use the schema to deterministically seed tag generation
	var bb []byte
	for _, k := range schemaKinds {
		bb = append(bb, uint8(k))
	}
	h := sha512.Sum512(bb)
	randGen := rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(h[:]))))

	var maxTagVal uint64 = 128 * 128

	for maxTagVal/2 < uint64(rootSS.Size()) {
		if maxTagVal == ReservedTagMin-1 {
			panic("There is no way anyone should ever have this many columns.  You are a bad person if you hit this panic.")
		} else if maxTagVal*128 < maxTagVal {
			maxTagVal = ReservedTagMin - 1
		} else {
			maxTagVal = maxTagVal * 128
		}
	}

	var randTag uint64
	for {
		randTag = uint64(randGen.Int63n(int64(maxTagVal)))

		if _, found := rootSS.GetColumn(randTag); !found {
			break
		}
	}

	return randTag
}
