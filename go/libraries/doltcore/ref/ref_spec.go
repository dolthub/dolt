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
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/strhelp"
)

// ErrInvalidRefSpec is the error returned when a refspec isn't syntactically valid
var ErrInvalidRefSpec = errors.New("invalid ref spec")

// ErrInvalidMapping is the error returned when a refspec tries to do an invalid mapping, such as mapping
// refs/heads/main to refs/remotes/origin/*
var ErrInvalidMapping = errors.New("invalid ref spec mapping")

// ErrUnsupportedMapping is returned when trying to do anything other than map local branches (refs/heads/*) to
// remote tracking branches (refs/remotes/*).  As other mappings are added this code will need updating
var ErrUnsupportedMapping = errors.New("unsupported mapping")

// RefSpec is an interface for mapping a reference in one space to a reference in another space.
type RefSpec interface {
	// SrcRef will take a reference to the current working branch and return a reference to what should be used
	// for the source reference of an operation involving a reference spec
	SrcRef(cwbRef DoltRef) DoltRef

	// DestRef will take a source reference and return a reference to what should be used for the destination
	// reference of an operation involving a reference spec.
	DestRef(srcRef DoltRef) DoltRef
}

// RemoteRefSpec is an interface that embeds the RefSpec interface and provides an additional method to get the name
// of a remote.
type RemoteRefSpec interface {
	RefSpec
	GetRemote() string
	GetRemRefToLocal() branchMapper
}

// ParseRefSpec parses a RefSpec from a string.
func ParseRefSpec(refSpecStr string) (RefSpec, error) {
	return ParseRefSpecForRemote("", refSpecStr)
}

// ParseRefSpecForRemote takes the name of a remote and a refspec, and parses that refspec, verifying that it refers
// to the appropriate remote
func ParseRefSpecForRemote(remote, refSpecStr string) (RefSpec, error) {
	var fromRef DoltRef
	var toRef DoltRef
	var err error
	if len(refSpecStr) == 0 {
		return nil, ErrInvalidRefSpec
	} else if refSpecStr[0] == ':' {
		fromRef = EmptyBranchRef
		toRef, err = Parse(refSpecStr[1:])

		if err != nil {
			return nil, ErrInvalidRefSpec
		}
	} else {
		tokens := strings.Split(refSpecStr, ":")

		if len(tokens) == 0 {
			return nil, ErrInvalidRefSpec
		}

		srcStr := tokens[0]
		destStr := tokens[0]

		if len(tokens) > 2 {
			return nil, ErrInvalidRefSpec
		} else if len(tokens) == 2 {
			destStr = tokens[1]
		}

		fromRef, err = Parse(srcStr)

		if err != nil {
			return nil, ErrInvalidRefSpec
		}

		toRef, err = Parse(destStr)

		if err != nil {
			return nil, ErrInvalidRefSpec
		}
	}

	if fromRef.GetType() == BranchRefType && toRef.GetType() == RemoteRefType {
		return NewLocalToRemoteTrackingRef(remote, fromRef.(BranchRef), toRef.(RemoteRef))
	} else if fromRef.GetType() == BranchRefType && toRef.GetType() == BranchRefType {
		return NewBranchToBranchRefSpec(fromRef.(BranchRef), toRef.(BranchRef))
	} else if fromRef.GetType() == TagRefType && toRef.GetType() == TagRefType {
		return NewTagToTagRefSpec(fromRef.(TagRef), toRef.(TagRef))
	}

	return nil, ErrUnsupportedMapping
}

type branchMapper interface {
	mapBranch(string) string
}

type identityBranchMapper string

func (ibm identityBranchMapper) mapBranch(s string) string {
	return string(ibm)
}

type wcBranchMapper struct {
	prefix string
	suffix string
}

func newWildcardBranchMapper(s string) wcBranchMapper {
	tokens := strings.Split(s, "*")

	if len(tokens) != 2 {
		panic("invalid localPattern")
	}

	return wcBranchMapper{tokens[0], tokens[1]}
}

func (wcbm wcBranchMapper) mapBranch(s string) string {
	return wcbm.prefix + s + wcbm.suffix
}

// BranchToBranchRefSpec maps one branch to another.
type BranchToBranchRefSpec struct {
	srcRef  DoltRef
	destRef DoltRef
}

// NewBranchToBranchRefSpec takes a source and destination BranchRef and returns a RefSpec that maps source to dest.
func NewBranchToBranchRefSpec(srcRef, destRef BranchRef) (RefSpec, error) {
	return BranchToBranchRefSpec{
		srcRef:  srcRef,
		destRef: destRef,
	}, nil
}

// SrcRef will always determine the DoltRef specified as the source ref regardless to the cwbRef
func (rs BranchToBranchRefSpec) SrcRef(cwbRef DoltRef) DoltRef {
	return rs.srcRef
}

// DestRef verifies the localRef matches the refspecs local pattern, and then maps it to a remote tracking branch, or
// nil if it does not match the local pattern.
func (rs BranchToBranchRefSpec) DestRef(r DoltRef) DoltRef {
	if Equals(r, rs.srcRef) {
		return rs.destRef
	}

	return nil
}

type TagToTagRefSpec struct {
	srcRef  DoltRef
	destRef DoltRef
}

// NewTagToTagRefSpec takes a source and destination TagRef and returns a RefSpec that maps source to dest.
func NewTagToTagRefSpec(srcRef, destRef TagRef) (RefSpec, error) {
	return TagToTagRefSpec{
		srcRef:  srcRef,
		destRef: destRef,
	}, nil
}

// SrcRef will always determine the DoltRef specified as the source ref regardless to the cwbRef
func (rs TagToTagRefSpec) SrcRef(_ DoltRef) DoltRef {
	return rs.srcRef
}

// DestRef verifies the localRef matches the refspecs local pattern, and then maps it to a remote tracking branch, or
// nil if it does not match the local pattern.
func (rs TagToTagRefSpec) DestRef(r DoltRef) DoltRef {
	if Equals(r, rs.srcRef) {
		return rs.destRef
	}

	return nil
}

// GetRemote returns the name of the remote being operated on.
func (rs TagToTagRefSpec) GetRemote() string {
	return ""
}

// GetRemRefToLocal returns the local tracking branch.
func (rs TagToTagRefSpec) GetRemRefToLocal() branchMapper {
	return identityBranchMapper(rs.srcRef.GetPath())
}

// BranchToTrackingBranchRefSpec maps a branch to the branch that should be tracking it
type BranchToTrackingBranchRefSpec struct {
	localPattern  pattern
	remPattern    pattern
	remote        string
	localToRemRef branchMapper
	remRefToLocal branchMapper
}

func NewLocalToRemoteTrackingRef(remote string, srcRef BranchRef, destRef RemoteRef) (RefSpec, error) {
	srcWCs := strings.Count(srcRef.GetPath(), "*")
	destWCs := strings.Count(destRef.GetPath(), "*")

	if srcWCs != destWCs || srcWCs > 1 {
		return nil, ErrInvalidRefSpec
	} else {
		remoteInRef, ok := strhelp.NthToken(destRef.GetPath(), '/', 0)

		if !ok {
			return nil, ErrInvalidRefSpec
		} else if remote != "" && remote != remoteInRef {
			return nil, ErrInvalidRefSpec
		}

		if srcWCs == 0 {
			srcPattern := strPattern(srcRef.GetPath())
			destPattern := strPattern(destRef.GetPath())
			srcToDestMapper := identityBranchMapper(destRef.GetPath()[len(remoteInRef)+1:])
			destToSrcMapper := identityBranchMapper(srcRef.GetPath())

			return BranchToTrackingBranchRefSpec{
				localPattern:  srcPattern,
				remPattern:    destPattern,
				remote:        remoteInRef,
				localToRemRef: srcToDestMapper,
				remRefToLocal: destToSrcMapper,
			}, nil
		} else {
			srcPattern := newWildcardPattern(srcRef.GetPath())
			destPattern := newWildcardPattern(destRef.GetPath())
			srcToDestMapper := newWildcardBranchMapper(destRef.GetPath()[len(remoteInRef)+1:])
			destToSrcMapper := newWildcardBranchMapper(srcRef.GetPath())

			return BranchToTrackingBranchRefSpec{
				localPattern:  srcPattern,
				remPattern:    destPattern,
				remote:        remoteInRef,
				localToRemRef: srcToDestMapper,
				remRefToLocal: destToSrcMapper,
			}, nil
		}
	}
}

// SrcRef will return the current working branch reference that is passed in as long as the cwbRef matches
// the source portion of the ref spec
func (rs BranchToTrackingBranchRefSpec) SrcRef(cwbRef DoltRef) DoltRef {
	if cwbRef.GetType() == BranchRefType {
		_, matches := rs.localPattern.matches(cwbRef.GetPath())
		if matches {
			return cwbRef
		}
	}

	return nil
}

// DestRef verifies the branchRef matches the refspec's local pattern, and then maps it to a remote tracking branch, or
// to nil if it does not match the pattern.
func (rs BranchToTrackingBranchRefSpec) DestRef(branchRef DoltRef) DoltRef {
	if branchRef.GetType() == BranchRefType {
		captured, matches := rs.localPattern.matches(branchRef.GetPath())
		if matches {
			return NewRemoteRef(rs.remote, rs.localToRemRef.mapBranch(captured))
		}
	}

	return nil
}

// GetRemote returns the name of the remote being operated on.
func (rs BranchToTrackingBranchRefSpec) GetRemote() string {
	return rs.remote
}

// GetRemRefToLocal returns the local tracking branch.
func (rs BranchToTrackingBranchRefSpec) GetRemRefToLocal() branchMapper {
	return rs.remRefToLocal
}
