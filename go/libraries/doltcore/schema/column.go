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
	"errors"
	"math"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// InvalidTag is used as an invalid tag
const InvalidTag uint64 = math.MaxUint64

var (
	// KindToLwrStr maps a noms kind to the kinds lowercased name
	KindToLwrStr = make(map[types.NomsKind]string)

	// LwrStrToKind maps a lowercase string to the noms kind it is referring to
	LwrStrToKind = make(map[string]types.NomsKind)
)

var (
	// InvalidCol is a Column instance that is returned when there is nothing to return and can be tested against.
	InvalidCol = Column{
		"invalid",
		InvalidTag,
		types.NullKind,
		false,
		typeinfo.UnknownType,
		"",
		"",
		nil,
	}
)

func init() {
	for t, s := range types.KindToString {
		KindToLwrStr[t] = strings.ToLower(s)
		LwrStrToKind[strings.ToLower(s)] = t
	}
}

// Column is a structure containing information about a column in a row in a table.
type Column struct {
	// Name is the name of the column
	Name string

	// Tag should be unique per versioned schema and allows
	Tag uint64

	// Kind is the types.NomsKind that values of this column will be
	Kind types.NomsKind

	// IsPartOfPK says whether this column is part of the primary key
	IsPartOfPK bool

	// TypeInfo states the type of this column.
	TypeInfo typeinfo.TypeInfo

	// Default is the default value of this column. This is the string representation of a sql.Expression.
	Default string

	// Comment is the comment for this column.
	Comment string

	// Constraints are rules that can be checked on each column to say if the columns value is valid
	Constraints []ColConstraint
}

// NewColumn creates a Column instance with the default type info for the NomsKind
func NewColumn(name string, tag uint64, kind types.NomsKind, partOfPK bool, constraints ...ColConstraint) Column {
	typeInfo := typeinfo.FromKind(kind)
	col, err := NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, "", "", constraints...)
	if err != nil {
		panic(err)
	}
	return col
}

// NewColumnWithTypeInfo creates a Column instance with the given type info.
func NewColumnWithTypeInfo(name string, tag uint64, typeInfo typeinfo.TypeInfo, partOfPK bool, defaultVal, comment string, constraints ...ColConstraint) (Column, error) {
	for _, c := range constraints {
		if c == nil {
			return Column{}, errors.New("nil passed as a constraint")
		}
	}

	if typeInfo == nil {
		return Column{}, errors.New("cannot instantiate column with nil type info")
	}

	return Column{
		name,
		tag,
		typeInfo.NomsKind(),
		partOfPK,
		typeInfo,
		defaultVal,
		comment,
		constraints,
	}, nil
}

// IsNullable returns whether the column can be set to a null value.
func (c Column) IsNullable() bool {
	if c.IsPartOfPK {
		return false
	}
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
		c.IsPartOfPK == other.IsPartOfPK &&
		c.TypeInfo.Equals(other.TypeInfo) &&
		c.Default == other.Default &&
		ColConstraintsAreEqual(c.Constraints, other.Constraints)
}

// KindString returns the string representation of the NomsKind stored in the column.
func (c Column) KindString() string {
	return KindToLwrStr[c.Kind]
}
