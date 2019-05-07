package ref

import "strings"

type InternalRef struct {
	path string
}

func (ir InternalRef) GetType() RefType {
	return InternalRefType
}

func (ir InternalRef) GetPath() string {
	return ir.path
}

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
