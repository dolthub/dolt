package parse

import (
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
)

// ParsePackage reads a Noms package specification from r and returns a Package. Errors will be annotated with logname and thrown.
func ParsePackage(logname string, r io.Reader) Package {
	got, err := ParseReader(logname, r)
	d.Exp.NoError(err)
	return got.(Package)
}

// A Package has a map of import Aliases that are not yet really used.
// UsingDeclarations is kind of a hack to indicate specializations of Noms containers that need to be generated. These should all be one of ListKind, SetKind, MapKind or RefKind, and Desc should be a CompoundDesc instance.
// NamedTypes is a lookup table for types defined in this package. These should all be EnumKind or StructKind. When traversing the definition of a given type, you may run into a TypeRef that IsUnresolved(). In that case, look it up by name in the NamedTypes of the appropriate package.
type Package struct {
	Name              string
	Aliases           map[string]string
	UsingDeclarations []types.TypeRef
	NamedTypes        map[string]types.TypeRef
}
