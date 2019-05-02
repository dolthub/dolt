package ref

import (
	"errors"
	"strings"
)

var ErrUnknownRefType = errors.New("unknown ref type")

const (
	refPrefix = "refs/"
)

func IsRef(str string) bool {
	return strings.HasPrefix(str, refPrefix)
}

type RefType string

const (
	InvalidRefType = "invalid"
	BranchRef      = "heads"
	RemoteRef      = "remotes"
	InternalRef    = "internal"
)

var RefTypes = map[RefType]struct{}{BranchRef: {}, RemoteRef: {}, InternalRef: {}}

func PrefixForType(refType RefType) string {
	return refPrefix + string(refType)
}

type DoltRef struct {
	Type RefType
	Path string
}

var InvalidRef = DoltRef{InvalidRefType, ""}

func (dr DoltRef) String() string {
	return PrefixForType(dr.Type) + "/" + dr.Path
}

func (dr DoltRef) Equals(other DoltRef) bool {
	return dr.Type == other.Type && dr.Path == other.Path
}

func (dr DoltRef) EqualsStr(str string) bool {
	other, err := Parse(str)

	if err != nil {
		return false
	}

	return dr.Equals(other)
}

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

func (dr *DoltRef) UnmarshalJSON(data []byte) error {
	dref, err := Parse(string(data[1 : len(data)-1]))

	if err != nil {
		return err
	}

	dr.Type = dref.Type
	dr.Path = dref.Path

	return nil
}

func NewBranchRef(branchName string) DoltRef {
	if strings.HasPrefix(branchName, "ref/") {
		panic("bug")
	}

	return DoltRef{BranchRef, branchName}
}

func NewRemoteRef(origin, name string) DoltRef {
	if strings.HasPrefix(name, "ref/") {
		panic("bug")
	}

	return DoltRef{RemoteRef, origin + "/" + name}
}

func NewRemoteRefFromPathStr(path string) DoltRef {
	const remotesPrefix = "remotes/"

	if strings.HasPrefix(path, "ref/") {
		panic("bug")
	}

	if strings.HasPrefix(path, remotesPrefix) {
		path = path[len(remotesPrefix):]
	}

	return DoltRef{RemoteRef, path}
}

func NewInternalRef(name string) DoltRef {
	if strings.HasPrefix(name, "ref/") {
		panic("bug")
	}

	return DoltRef{InternalRef, name}
}

func Parse(str string) (DoltRef, error) {
	if !IsRef(str) {
		return NewBranchRef(str), nil
	}

	for rType := range RefTypes {
		prefix := PrefixForType(rType)
		if strings.HasPrefix(str, prefix) {
			return DoltRef{
				rType,
				str[len(prefix)+1:],
			}, nil
		}
	}

	return InvalidRef, ErrUnknownRefType
}
