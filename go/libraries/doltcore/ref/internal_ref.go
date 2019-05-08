package ref

import "strings"

// InternalRef is a dolt internal reference
type InternalRef struct {
	path string
}

// GetType returns InternalRefType
func (ir InternalRef) GetType() RefType {
	return InternalRefType
}

// GetPath returns the name of the internal reference
func (ir InternalRef) GetPath() string {
	return ir.path
}

// String returns the fully qualified reference e.g. refs/internal/create
func (ir InternalRef) String() string {
	return String(ir)
}

// NewInternalRef creates an internal ref
func NewInternalRef(name string) DoltRef {
	if IsRef(name) {
		prefix := PrefixForType(InternalRefType)
		if strings.HasPrefix(name, prefix) {
			name = name[len(prefix):]
		} else {
			panic(name + " is a ref that is not of type " + prefix)
		}
	}

	return InternalRef{name}
}
