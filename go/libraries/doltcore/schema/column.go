package schema

import (
	"github.com/attic-labs/noms/go/types"
	"math"
	"strings"
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

var InvalidTag uint64 = math.MaxUint64
var ReservedTagMin uint64 = 1 << 63
var InvalidCol = NewColumn("invalid", InvalidTag, types.NullKind, false)

type Column struct {
	Name        string
	Tag         uint64
	Kind        types.NomsKind
	IsPartOfPK  bool
	Constraints []ColConstraint
}

func NewColumn(name string, tag uint64, kind types.NomsKind, partOfPK bool, constraints ...ColConstraint) Column {
	for _, c := range constraints {
		if c == nil {
			panic("nil passed as a constraint")
		}
	}

	return Column{
		name,
		tag,
		kind,
		partOfPK,
		constraints,
	}
}

func (c Column) Equals(other Column) bool {
	return c.Name == other.Name &&
		c.Tag == other.Tag &&
		c.Kind == other.Kind &&
		c.IsPartOfPK == other.IsPartOfPK &&
		ColConstraintsAreEqual(c.Constraints, other.Constraints)
}

func (c Column) KindString() string {
	return KindToLwrStr[c.Kind]
}
