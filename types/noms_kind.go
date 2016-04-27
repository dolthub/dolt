package types

// NomsKind allows a TypeDesc to indicate what kind of type is described.
type NomsKind uint8

// All supported kinds of Noms types are enumerated here.
const (
	BoolKind NomsKind = iota
	NumberKind
	StringKind
	BlobKind
	ValueKind
	ListKind
	MapKind
	RefKind
	SetKind
	StructKind
	TypeKind
	UnresolvedKind
	PackageKind
)

// IsPrimitiveKind returns true if k represents a Noms primitive type, which excludes collections (List, Map, Set), Refs, Structs, Symbolic and Unresolved types.
func IsPrimitiveKind(k NomsKind) bool {
	switch k {
	case BoolKind, NumberKind, StringKind, BlobKind, ValueKind, TypeKind, PackageKind:
		return true
	default:
		return false
	}
}
