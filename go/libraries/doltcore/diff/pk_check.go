// Copyright 2026 Dolthub, Inc.
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

package diff

import (
	"context"
	"strings"

	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

var ErrPrimaryKeySetChanged = goerrors.NewKind("cannot diff table %s: primary key set changed, and %s")

const reasonKeyColumnMissing = "a column of the new primary key is missing from the old rows or has another type"
const reasonCrossedTags = "a column tag moved to another column"

// RekeyForDiff returns |from|, whose rows are stored under |fromSch|, and its schema, keyed so that
// [prolly.DiffMaps] pairs its rows with those of a map stored under |toSch|. When the primary keys differ, the rows
// are rewritten to the primary key of |toSch| in |ns|. It returns ErrPrimaryKeySetChanged when they cannot be.
//
// Keyless tables are returned unchanged.
func RekeyForDiff(ctx context.Context, tblName string, ns tree.NodeStore, from prolly.Map, fromSch, toSch schema.Schema) (prolly.Map, schema.Schema, error) {
	if !NeedsRekeyForDiff(fromSch, toSch, ns) {
		return from, fromSch, nil
	}
	if hasCrossedTags(fromSch, toSch) {
		return prolly.Map{}, nil, ErrPrimaryKeySetChanged.New(tblName, reasonCrossedTags)
	}
	rekeyedSch, ok, err := schema.RekeyedSchema(fromSch, toSch)
	if err != nil {
		return prolly.Map{}, nil, err
	}
	if !ok {
		return prolly.Map{}, nil, ErrPrimaryKeySetChanged.New(tblName, reasonKeyColumnMissing)
	}
	rekeyed, ok, err := durable.RekeyRows(ctx, ns, from, fromSch, rekeyedSch)
	if err == durable.ErrRekeyCollision || err == durable.ErrRekeyNullKey {
		return prolly.Map{}, nil, ErrPrimaryKeySetChanged.New(tblName, err.Error())
	} else if err != nil {
		return prolly.Map{}, nil, err
	}
	if !ok {
		return prolly.Map{}, nil, ErrPrimaryKeySetChanged.New(tblName, reasonKeyColumnMissing)
	}
	return rekeyed, rekeyedSch, nil
}

// NeedsRekeyForDiff returns whether rows stored under |fromSch| must be rekeyed by [RekeyForDiff] before they can be
// diffed against rows stored under |toSch|.
func NeedsRekeyForDiff(fromSch, toSch schema.Schema, ns tree.NodeStore) bool {
	if !isKeyedSchema(fromSch) || !isKeyedSchema(toSch) {
		return false
	}
	if schema.ComparePrimaryKeys(fromSch, toSch) != schema.PrimaryKeysDiffer {
		return false
	}
	return hasCrossedTags(fromSch, toSch) || !haveSameKeyEncoding(fromSch, toSch, ns)
}

// hasCrossedTags returns whether a column of |toSch| holds the tag of a differently named column of |fromSch| that
// still exists under another tag, so that matching columns by tag would pair different columns. A renamed column
// keeps its tag and is not crossed.
func hasCrossedTags(fromSch, toSch schema.Schema) bool {
	fromCols := fromSch.GetAllCols()
	for _, toCol := range toSch.GetAllCols().GetColumns() {
		byTag, ok := fromCols.GetByTag(toCol.Tag)
		if !ok || strings.EqualFold(byTag.Name, toCol.Name) {
			continue
		}
		if byName, ok := fromCols.GetByNameCaseInsensitive(toCol.Name); ok && byName.Tag != toCol.Tag {
			return true
		}
	}
	return false
}

// haveSameKeyEncoding returns whether rows stored under |fromSch| already sort and compare like rows stored under
// |toSch|, with each key column matched by tag or name, as when only a key column's length changed.
func haveSameKeyEncoding(fromSch, toSch schema.Schema, ns tree.NodeStore) bool {
	fromPK, toPK := fromSch.GetPKCols(), toSch.GetPKCols()
	if fromPK.Size() != toPK.Size() {
		return false
	}
	fromKD, _ := fromSch.GetMapDescriptors(ns)
	toKD, _ := toSch.GetMapDescriptors(ns)
	if fromKD == nil || toKD == nil || !fromKD.Equals(toKD) {
		return false
	}
	for i := 0; i < fromPK.Size(); i++ {
		fc, tc := fromPK.GetByIndex(i), toPK.GetByIndex(i)
		if fc.Tag != tc.Tag && !strings.EqualFold(fc.Name, tc.Name) {
			return false
		}
	}
	return true
}

func isKeyedSchema(sch schema.Schema) bool {
	return sch != nil && sch.GetAllCols().Size() > 0 && !schema.IsKeyless(sch)
}
