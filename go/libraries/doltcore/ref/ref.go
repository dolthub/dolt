package ref

import (
	"errors"
	"path"
	"strings"
)

var ErrUnknownRefType = errors.New("unknown ref type")

const (
	refPrefix     = "refs/"
	remotesPrefix = "remotes/"
)

// IsRef returns true if the string is a reference string (meanings it starts with the prefix refs/)
func IsRef(str string) bool {
	return strings.HasPrefix(str, refPrefix)
}

// RefType is the type of the reference, and this value follows the refPrefix in a ref string.  e.g. refs/type/...
type RefType string

const (
	// InvalidRefType will be used when parsing a ref type with a RefType string that isn't known
	InvalidRefType RefType = "invalid"

	// BranchRef is a reference to a local branch in the format refs/heads/...
	BranchRef RefType = "heads"

	// RemoteRef is a reference to a local remote tracking branch
	RemoteRef RefType = "remotes"

	// InternalRef is a reference to a dolt internal commit
	InternalRef RefType = "internal"
)

// RefTypes is the set of all supported reference types.  External RefTypes can be to this map in order to add
// RefTypes for external tooling
var RefTypes = map[RefType]struct{}{BranchRef: {}, RemoteRef: {}, InternalRef: {}}

// PrefixForType returns what a reference string for a given type should start with
func PrefixForType(refType RefType) string {
	return refPrefix + string(refType) + "/"
}

// DoltRef is a reference to a commit.
type DoltRef struct {
	// Type is the RefType of this ref
	Type RefType

	// Path is the identifier for the reference
	Path string
}

// InvalidRef is an instance of an invalid reference
var InvalidRef = DoltRef{InvalidRefType, ""}

// String() converts the DoltRef to a reference string in the format refs/type/path
func (dr DoltRef) String() string {
	return PrefixForType(dr.Type) + dr.Path
}

// Equals returns true if two DoltRefs have the same Type and Path
func (dr DoltRef) Equals(other DoltRef) bool {
	return dr.Type == other.Type && dr.Path == other.Path
}

// EqualsStr compares a DoltRef to a reference string to see if they are refering to the same thing
func (dr DoltRef) EqualsStr(str string) bool {
	other, err := Parse(str)

	if err != nil {
		return false
	}

	return dr.Equals(other)
}

// MarshalJson implements the json Marshaler interface to json encode DoltRefs as their string representation
func (dr DoltRef) MarshalJSON() ([]byte, error) {
	str := dr.String()
	data := make([]byte, len(str)+2)

	data[0] = '"'
	data[len(str)+1] = '"'

	for i, b := range str {
		data[i+1] = byte(b)
	}

	return data, nil
}

// UnmarshalJSON implements the json Unmarshaler interface to decode the reference string representation of a
// DoltRef within a json file
func (dr *DoltRef) UnmarshalJSON(data []byte) error {
	dref, err := Parse(string(data[1 : len(data)-1]))

	if err != nil {
		return err
	}

	dr.Type = dref.Type
	dr.Path = dref.Path

	return nil
}

// NewBranchRef creates a reference to a local branch from a branch name or a branch ref e.g. master, or refs/heads/master
func NewBranchRef(branchName string) DoltRef {
	if IsRef(branchName) {
		prefix := PrefixForType(BranchRef)
		if strings.HasPrefix(branchName, prefix) {
			branchName = branchName[len(prefix):]
		} else {
			panic(branchName + " is a ref that is not of type " + prefix)
		}
	}

	return DoltRef{BranchRef, branchName}
}

// NewRemoteRef creates a remote ref from an origin name and a path
func NewRemoteRef(remote, name string) DoltRef {
	return DoltRef{RemoteRef, path.Join(remote, name)}
}

// NewRemoteRefFromPathString creates a DoltRef from a string in the format origin/master, or remotes/origin/master, or
// refs/remotes/origin/master
func NewRemoteRefFromPathStr(remoteAndPath string) DoltRef {
	if IsRef(remoteAndPath) {
		prefix := PrefixForType(RemoteRef)
		if strings.HasPrefix(remoteAndPath, prefix) {
			remoteAndPath = remoteAndPath[len(prefix):]
		} else {
			panic(remoteAndPath + " is a ref that is not of type " + prefix)
		}
	} else if strings.HasPrefix(remoteAndPath, remotesPrefix) {
		remoteAndPath = remoteAndPath[len(remotesPrefix):]
	}

	return DoltRef{RemoteRef, remoteAndPath}
}

// NewInternalRef creates an internal ref
func NewInternalRef(name string) DoltRef {
	if IsRef(name) {
		prefix := PrefixForType(InternalRef)
		if strings.HasPrefix(name, prefix) {
			name = name[len(prefix):]
		} else {
			panic(name + " is a ref that is not of type " + prefix)
		}
	}

	return DoltRef{InternalRef, name}
}

// Parse will parse ref strings and return a DoltRef or an error for refs that can't be parsed.
func Parse(str string) (DoltRef, error) {
	if !IsRef(str) {
		if strings.HasPrefix(str, remotesPrefix) {
			return NewRemoteRefFromPathStr(str), nil
		} else {
			return NewBranchRef(str), nil
		}
	}

	for rType := range RefTypes {
		prefix := PrefixForType(rType)
		if strings.HasPrefix(str, prefix) {
			return DoltRef{
				rType,
				str[len(prefix):],
			}, nil
		}
	}

	return InvalidRef, ErrUnknownRefType
}
