// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/types"
)

const (
	// ReservedTagMin is the start of a range of tags which the user should not be able to use in their schemas.
	ReservedTagMin uint64 = 1 << 50
)

type ErrTagPrevUsed struct {
	Tag          uint64
	NewColName   string
	NewTableName string
	OldTableName string
}

var _ error = ErrTagPrevUsed{}

func (e ErrTagPrevUsed) Error() string {
	return fmt.Sprintf("cannot create column %s on table %s, the tag %d was already used in table %s", e.NewColName, e.NewTableName, e.Tag, e.OldTableName)
}

func NewErrTagPrevUsed(tag uint64, newColName, newTableName, oldTableName string) ErrTagPrevUsed {
	return ErrTagPrevUsed{
		Tag:          tag,
		NewColName:   newColName,
		NewTableName: newTableName,
		OldTableName: oldTableName,
	}
}

type TagMapping map[uint64]string

func (tm TagMapping) Contains(tag uint64) (ok bool) {
	_, ok = tm[tag]
	return
}

func (tm TagMapping) Get(tag uint64) (table string, ok bool) {
	table, ok = tm[tag]
	return
}

func (tm TagMapping) Add(tag uint64, table string) {
	tm[tag] = table
}

func (tm TagMapping) Remove(tag uint64) {
	delete(tm, tag)
}

func (tm TagMapping) Size() int {
	return len(tm)
}

// AutoGenerateTag generates a random tag that doesn't exist in the provided SuperSchema.
// It uses a deterministic random number generator that is seeded with the NomsKinds of any existing columns in the
// schema and the NomsKind of the column being added to the schema. Deterministic tag generation means that branches
// and repositories that perform the same sequence of mutations to a database will get equivalent databases as a result.
// DETERMINISTIC MUTATION IS A CRITICAL INVARIANT TO MAINTAINING COMPATIBILITY BETWEEN REPOSITORIES.
// DO NOT ALTER THIS METHOD.
func AutoGenerateTag(existingTags TagMapping, tableName string, existingColKinds []types.NomsKind, newColName string, newColKind types.NomsKind) uint64 {
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
