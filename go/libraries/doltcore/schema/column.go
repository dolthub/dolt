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

package schema

import (
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dolttypes"
	"math"
	"strings"

	"github.com/liquidata-inc/dolt/go/store/types"
)

// FullKind is the combination of both a DoltKind and NomsKind, represents the absolute kind
type FullKind struct {
	NomsKind types.NomsKind
	DoltKind dolttypes.DoltKind
}

// KindToLwrStr maps a full kind to the kinds lowercased name
var KindToLwrStr = make(map[FullKind]string)

// LwrStrToKind maps a lowercase string to the full kind it is referring to
var LwrStrToKind = make(map[string]FullKind)

func init() {
	for t, s := range types.KindToString {
		if t != types.UnderlyingArrayKind {
			s := strings.ToLower(s)
			KindToLwrStr[FullKind{t, dolttypes.UnknownKind}] = s
			LwrStrToKind[s] = FullKind{t, dolttypes.UnknownKind}
		}
	}
	for t, s := range dolttypes.KindName {
		if t != dolttypes.UnknownKind {
			s := strings.ToLower(s)
			KindToLwrStr[FullKind{types.UnderlyingArrayKind, t}] = s
			LwrStrToKind[s] = FullKind{types.UnderlyingArrayKind, t}
		}
	}
}

// InvalidTag is used as an invalid tag
var InvalidTag uint64 = math.MaxUint64

// ReservedTagMin is the start of a range of tags which the user should not be able to use in their schemas.
const ReservedTagMin uint64 = 1 << 63

// InvalidCol is a Column instance that is returned when there is nothing to return and can be tested against.
var InvalidCol = NewColumn("invalid", InvalidTag, types.NullKind, dolttypes.UnknownKind, false)

// Column is a structure containing information about a column in a row in a table.
type Column struct {
	// Name is the name of the column
	Name string

	// Tag should be unique per versioned schema and allows
	Tag uint64

	// Kind is the types.NomsKind that values of this column will be
	Kind types.NomsKind

	// DoltKind is the Dolt type that values of this column will be (with Kind == types.UnderlyingArrayKind)
	DoltKind dolttypes.DoltKind

	// IsPartOfPK says whether this column is part of the primary key
	IsPartOfPK bool

	// Constraints are rules that can be checked on each column to say if the columns value is valid
	Constraints []ColConstraint
}

// NewColumn creates a Column instance
func NewColumn(name string, tag uint64, kind types.NomsKind, dKind dolttypes.DoltKind, partOfPK bool, constraints ...ColConstraint) Column {
	for _, c := range constraints {
		if c == nil {
			panic("nil passed as a constraint")
		}
	}

	return Column{
		name,
		tag,
		kind,
		dKind,
		partOfPK,
		constraints,
	}
}

// IsNullable returns whether the column can be set to a null value.
func (c Column) IsNullable() bool {
	for _, cnst := range c.Constraints {
		if cnst.GetConstraintType() == NotNullConstraintType {
			return false
		}
	}
	return true
}

// Equals tests equality between two columns.
func (c Column) Equals(other Column) bool {
	return c.Name == other.Name &&
		c.Tag == other.Tag &&
		c.Kind == other.Kind &&
		c.DoltKind == c.DoltKind &&
		c.IsPartOfPK == other.IsPartOfPK &&
		ColConstraintsAreEqual(c.Constraints, other.Constraints)
}

// KindString returns the string representation of the NomsKind stored in the column.
func (c Column) KindString() string {
	return KindToLwrStr[FullKind{c.Kind, c.DoltKind}]
}
