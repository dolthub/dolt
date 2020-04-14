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
	"fmt"
	"math/rand"
	"regexp"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	// ReservedTagMin is the start of a range of tags which the user should not be able to use in their schemas.
	ReservedTagMin uint64 = 1 << 50
)

func ErrTagPrevUsed(tag uint64, newColName, tableName string) error {
	return fmt.Errorf("Cannot create column %s, the tag %d was already used in table %s", newColName, tag, tableName)
}

// AutoGenerateTag generates a random tag that doesn't exist in the provided SuperSchema.
// It uses a deterministic random number generator that is seeded with the NomsKinds of any existing columns in the
// schema and the NomsKind of the column being added to the schema. Deterministic tag generation means that branches
// and repositories that perform the same sequence of mutations to a database will get equivalent databases as a result.
// DETERMINISTIC MUTATION IS A CRITICAL INVARIANT TO MAINTAINING COMPATIBILITY BETWEEN REPOSITORIES.
// DO NOT ALTER THIS METHOD.
func AutoGenerateTag(existingTags *set.Uint64Set, tableName string, existingColKinds []types.NomsKind, newColName string, newColKind types.NomsKind) uint64 {
	// DO NOT ALTER THIS METHOD (see above)
	var maxTagVal uint64 = 128 * 128

	for maxTagVal/2 < uint64(existingTags.Size()) {
		if maxTagVal >= ReservedTagMin-1 {
			panic("There is no way anyone should ever have this many columns.  You are a bad person if you hit this panic.")
		} else if maxTagVal*128 < maxTagVal {
			maxTagVal = ReservedTagMin - 1
			break
		} else {
			maxTagVal = maxTagVal * 128
		}
	}

	randGen := deterministicRandomTagGenerator(tableName, newColName, existingColKinds, newColKind)
	var randTag uint64
	for {
		randTag = uint64(randGen.Int63n(int64(maxTagVal)))

		if !existingTags.Contains(randTag) {
			break
		}
	}

	return randTag
}

// randomTagGeneratorFromKinds creates a deterministic random number generator that is seeded with the NomsKinds of any
// existing columns in the schema and the NomsKind of the column being added to the schema. Deterministic tag generation
// means that branches and repositories that perform the same sequence of mutations to a database will get equivalent
// databases as a result.
// DETERMINISTIC MUTATION IS A CRITICAL INVARIANT TO MAINTAINING COMPATIBILITY BETWEEN REPOSITORIES.
// DO NOT ALTER THIS METHOD.
func deterministicRandomTagGenerator(tableName string, newColName string, existingColKinds []types.NomsKind, newColKind types.NomsKind) *rand.Rand {
	// DO NOT ALTER THIS METHOD (see Above)

	var bb []byte
	for _, k := range existingColKinds {
		bb = append(bb, uint8(k))
	}

	bb = append(bb, uint8(newColKind))

	// transform these strings to increase the likelihood of tag collisions for similarly specified tables. eg:
	// Alice: "CREATE TABLE `My Table` (c0 INT NOT NULL PRIMARY KEY);"
	// Bob:   "CREATE TABLE my_table (C0 INT NOT NULL PRIMARY KEY);"
	tableName = simpleString(tableName)
	newColName = simpleString(newColName)
	bb = append(bb, []byte(tableName)...)
	bb = append(bb, []byte(newColName)...)

	h := sha512.Sum512(bb)
	return rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(h[:]))))
}

// simpleString converts s to lower case and removes non-alphanumeric characters
func simpleString(s string) string {
	reg := regexp.MustCompile("[^a-zA-Z0-9]+")
	return strings.ToLower(reg.ReplaceAllString(s, ""))
}

func NomsKindsFromSchema(sch Schema) []types.NomsKind {
	var nks []types.NomsKind
	_ = sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		nks = append(nks, col.Kind)
		return false, nil
	})
	return nks
}
