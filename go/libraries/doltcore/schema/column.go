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

package schema

import (
	"errors"
	"math"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
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
		Name:     "invalid",
		Tag:      InvalidTag,
		Kind:     types.NullKind,
		TypeInfo: typeinfo.UnknownType,
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

	// Kind is the types.NomsKind of values in this column. This value is mostly symbolic and this field mostly
	// vestigial with the new storage format. If you seem to need it for something, consider TypeInfo instead.
	// Its main remaining use is in determining the tag for new columns, so it must be set for that reason.
	Kind types.NomsKind

	// Encoding is the encoding of column values on disk. It's primarily derived from the column's TypeInfo,
	// but some column types have multiple possible encodings that can vary based on settings, Dolt version, etc.
	// This field is considered authoritative for the encoding of a column when reading and writing to disk.
	Encoding val.Encoding

	// IsPartOfPK says whether this column is part of the primary key
	IsPartOfPK bool

	// TypeInfo states the type of this column.
	TypeInfo typeinfo.TypeInfo

	// Default is the default value of this column. This is the string representation of a sql.Expression.
	Default string

	// Generated is the generated value of this column. This is the string representation of a sql.Expression.
	Generated string

	// OnUpdate is the on update value of this column. This is the string representation of a sql.Expression.
	OnUpdate string

	// Virtual is true if this is a virtual column.
	Virtual bool

	// AutoIncrement says whether this column auto increments.
	AutoIncrement bool

	// Comment is the comment for this column.
	Comment string

	// Constraints are rules that can be checked on each column to say if the columns value is valid
	Constraints []ColConstraint
}

// NewColumn creates a Column instance with the default type info for the NomsKind
// Deprecated. Use NewColumnWithTypeInfo instead.
func NewColumn(name string, tag uint64, kind types.NomsKind, partOfPK bool, constraints ...ColConstraint) Column {
	typeInfo := typeinfo.FromKind(kind)
	col, err := NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, "", false, "", constraints...)
	if err != nil {
		panic(err)
	}
	return col
}

// NewColumnWithTypeInfo creates a Column instance with the given type info.
// Callers are encouraged to construct schema.Column structs directly instead of using this method, then call
// ValidateColumn.
func NewColumnWithTypeInfo(name string, tag uint64, typeInfo typeinfo.TypeInfo, partOfPK bool, defaultVal string, autoIncrement bool, comment string, constraints ...ColConstraint) (Column, error) {
	c := Column{
		Name:          name,
		Tag:           tag,
		Kind:          typeInfo.NomsKind(),
		IsPartOfPK:    partOfPK,
		TypeInfo:      typeInfo,
		Default:       defaultVal,
		AutoIncrement: autoIncrement,
		Comment:       comment,
		Constraints:   constraints,
	}

	err := ValidateColumn(c)
	if err != nil {
		return InvalidCol, err
	}

	return c, nil
}

// ValidateColumn validates the given column.
func ValidateColumn(c Column) error {
	for _, c := range c.Constraints {
		if c == nil {
			return errors.New("nil passed as a constraint")
		}
	}

	if c.TypeInfo == nil {
		return errors.New("cannot instantiate column with nil type info")
	}

	return nil
}

// IsNullable returns whether the column can be set to a null value.
func (c Column) IsNullable() bool {
	if c.IsPartOfPK {
		return false
	}
	if c.AutoIncrement {
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
			c.IsPartOfPK == other.IsPartOfPK &&
			c.TypeInfo.Equals(other.TypeInfo) &&
			c.Default == other.Default &&
			ColConstraintsAreEqual(c.Constraints, other.Constraints)
}

// EqualsWithoutTag tests equality between two columns, but does not check the columns' tags.
func (c Column) EqualsWithoutTag(other Column) bool {
	return c.Name == other.Name &&
			c.IsPartOfPK == other.IsPartOfPK &&
			c.TypeInfo.Equals(other.TypeInfo) &&
			c.Default == other.Default &&
			ColConstraintsAreEqual(c.Constraints, other.Constraints)
}
