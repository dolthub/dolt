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

package ref

import (
	"errors"
	"fmt"
	"strings"
)

// ErrUnknownRefType is the error returned when parsing a ref in the format refs/type/... where type is unknown
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
	// BranchRefType is a reference to a local branch in the format refs/heads/...
	BranchRefType RefType = "heads"

	// RemoteRefType is a reference to a local remote tracking branch
	RemoteRefType RefType = "remotes"

	// InternalRefType is a reference to a dolt internal commit
	InternalRefType RefType = "internal"

	// TagRefType is a reference to commit tag
	TagRefType RefType = "tags"

	// WorkspaceRefType is a reference to a workspace
	WorkspaceRefType RefType = "workspaces"

	// StashRefType is a reference to a stash list
	StashRefType RefType = "stashes"

	// StatsRefType is a reference to a statistics table
	StatsRefType RefType = "statistics"

	// TupleRefType is a reference to a statistics table
	TupleRefType RefType = "tuples"
)

// HeadRefTypes are the ref types that point to a HEAD and contain a Commit struct. These are the types that are
// returned by GetHeadRefs. Other ref types don't point to Commits necessarily, so aren't in this list and must be
// asked for explicitly.
var HeadRefTypes = map[RefType]struct{}{
	BranchRefType:    {},
	RemoteRefType:    {},
	InternalRefType:  {},
	TagRefType:       {},
	WorkspaceRefType: {},
}

var StashRefTypes = map[RefType]struct{}{
	StashRefType: {},
}

// StatsRefTypes point to a table address hash, not a commit hash.
var StatsRefTypes = map[RefType]struct{}{
	StatsRefType: {},
}

// PrefixForType returns what a reference string for a given type should start with
func PrefixForType(refType RefType) string {
	return refPrefix + string(refType) + "/"
}

type UpdateMode struct {
	Force bool
	Prune bool
}

var ForceUpdate = UpdateMode{true, false}
var FastForwardOnly = UpdateMode{false, false}

// DoltRef is a reference to a commit.
type DoltRef interface {
	fmt.Stringer

	// GetType returns the RefType of this ref
	GetType() RefType

	// GetPath returns the identifier for the reference
	GetPath() string
}

// Equals returns true if two DoltRefs have the same Type and Path
func Equals(dr, other DoltRef) bool {
	if dr == nil && other == nil {
		return true
	} else if dr == nil || other == nil {
		return false
	}

	return dr.GetType() == other.GetType() && dr.GetPath() == other.GetPath()
}

// EqualsCaseInsensitive returns true if two DoltRefs have the same Type and Path, comparing the path case-insensitive
func EqualsCaseInsensitive(dr, other DoltRef) bool {
	if dr == nil && other == nil {
		return true
	} else if dr == nil || other == nil {
		return false
	}

	return dr.GetType() == other.GetType() && strings.EqualFold(dr.GetPath(), other.GetPath())
}

// EqualsStr compares a DoltRef to a reference string to see if they are referring to the same thing
func EqualsStr(dr DoltRef, str string) bool {
	other, err := Parse(str)

	if err != nil {
		return false
	}

	return Equals(dr, other)
}

// String converts the DoltRef to a reference string in the format refs/type/path
func String(dr DoltRef) string {
	return PrefixForType(dr.GetType()) + dr.GetPath()
}

// MarshalJSON implements the json Marshaler interface to json encode DoltRefs as their string representation
func MarshalJSON(dr DoltRef) ([]byte, error) {
	str := dr.String()
	data := make([]byte, len(str)+2)

	data[0] = '"'
	data[len(str)+1] = '"'

	for i, b := range str {
		data[i+1] = byte(b)
	}

	return data, nil
}

// Parse will parse ref strings and return a DoltRef or an error for refs that can't be parsed.
// refs without a RefType prefix ("refs/heads/", "refs/tags/", etc) are assumed to be branches)
func Parse(str string) (DoltRef, error) {
	if !IsRef(str) {
		if strings.HasPrefix(str, remotesPrefix) {
			return NewRemoteRefFromPathStr(str)
		} else {
			return NewBranchRef(str), nil
		}
	}

	for rType := range HeadRefTypes {
		prefix := PrefixForType(rType)
		if strings.HasPrefix(str, prefix) {
			str = str[len(prefix):]
			switch rType {
			case BranchRefType:
				return NewBranchRef(str), nil
			case RemoteRefType:
				return NewRemoteRefFromPathStr(str)
			case InternalRefType:
				return NewInternalRef(str), nil
			case TagRefType:
				return NewTagRef(str), nil
			case WorkspaceRefType:
				return NewWorkspaceRef(str), nil
			default:
				panic("unknown type " + rType)
			}
		}
	}

	for rType := range StashRefTypes {
		prefix := PrefixForType(rType)
		if strings.HasPrefix(str, prefix) {
			str = str[len(prefix):]
			switch rType {
			case StashRefType:
				return NewStashRef(), nil
			default:
				panic("unknown type " + rType)
			}
		}
	}

	if prefix := PrefixForType(StatsRefType); strings.HasPrefix(str, prefix) {
		return NewStatsRef(str[len(prefix):]), nil
	}

	if prefix := PrefixForType(TupleRefType); strings.HasPrefix(str, prefix) {
		return NewTupleRef(str[len(prefix):]), nil
	}

	return nil, ErrUnknownRefType
}
