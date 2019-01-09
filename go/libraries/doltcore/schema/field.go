package schema

import (
	"strings"

	"github.com/attic-labs/noms/go/types"
)

// KindToLwrStr maps a noms kind to the kinds lowercased name
var KindToLwrStr = make(map[types.NomsKind]string)

// LwrStrToKind maps a lowercase string to the noms kind it is referring to
var LwrStrToKind = make(map[string]types.NomsKind)

func init() {
	for t, s := range types.KindToString {
		KindToLwrStr[t] = strings.ToLower(s)
		LwrStrToKind[strings.ToLower(s)] = t
	}
}

// Field represents a column within a table
type Field struct {
	// name is the name of the field
	name string

	// kind is the type of the field.  See types/noms_kind.go in the liquidata fork for valid values
	kind types.NomsKind

	// required tells whether all rows require this value to be considered valid
	required bool
}

// NewField creates a new instance of Field from a name, type, and a flag saying whether it is required
func NewField(name string, kind types.NomsKind, required bool) *Field {
	return &Field{strings.ToLower(name), kind, required}
}

// Equals returns true if all members are equal
func (fld *Field) Equals(other *Field) bool {
	return fld.name == other.name && fld.kind == other.kind && fld.required == other.required
}

// NameStr returns the name of the field
func (fld *Field) NameStr() string {
	return fld.name
}

// NomsKind returns the kind of the field
func (fld *Field) NomsKind() types.NomsKind {
	return fld.kind
}

// KindString returns the lower case friendly name of the field's type
func (fld *Field) KindString() string {
	return KindToLwrStr[fld.kind]
}

// IsRequired tells whether all rows require this value to be considered valid.
func (fld *Field) IsRequired() bool {
	return fld.required
}
