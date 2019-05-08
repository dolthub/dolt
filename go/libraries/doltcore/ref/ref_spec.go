package ref

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/strhelp"
	"strings"
)

// ErrInvalidRefSpec is the error returned when a refspec isn't syntactically valid
var ErrInvalidRefSpec = errors.New("invalid ref spec")

// ErrInvalidMapping is the error returned when a refspec tries to do an invalid mapping, such as mapping
// refs/heads/master to refs/remotes/origin/*
var ErrInvalidMapping = errors.New("invalid ref spec mapping")

// ErrUnsupportedMapping is returned when trying to do anything other than map local branches (refs/heads/*) to
// remote tracking branches (refs/remotes/*).  As other mappings are added this code will need updating
var ErrUnsupportedMapping = errors.New("unsupported mapping")

// RefSpec is an interface for mapping a reference in one space to a reference in another space.
type RefSpec interface {
	Map(DoltRef) DoltRef
	MapBack(DoltRef) DoltRef
}

type RemoteRefSpec interface {
	RefSpec
	GetRemote() string
}

func ParseRefSpec(refSpecStr string) (RefSpec, error) {
	return ParseRefSpecForRemote("", refSpecStr)
}

// ParseRefSpecForRemote takes the name of a remote and a refspec, and parses that refspec, verifying that it refers
// to the appropriate remote
func ParseRefSpecForRemote(remote, refSpecStr string) (RefSpec, error) {
	tokens := strings.Split(refSpecStr, ":")

	if len(tokens) != 2 {
		return nil, ErrInvalidRefSpec
	}

	fromRef, err := Parse(tokens[0])

	if err != nil {
		return nil, ErrInvalidRefSpec
	}

	toRef, err := Parse(tokens[1])

	if err != nil {
		return nil, ErrInvalidRefSpec
	}

	if fromRef.GetType() == BranchRefType && toRef.GetType() == RemoteRefType {
		return newLocalToRemoteTrackingRef(remote, fromRef, toRef)
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

type localToRemoteTrackingRef struct {
	localPattern  pattern
	remPattern    pattern
	remote        string
	localToRemRef branchMapper
	remRefToLocal branchMapper
}

func newLocalToRemoteTrackingRef(remote string, local DoltRef, remoteRef DoltRef) (RefSpec, error) {
	localWCs := strings.Count(local.GetPath(), "*")
	remoteWCs := strings.Count(remoteRef.GetPath(), "*")

	if localWCs != remoteWCs || localWCs > 1 {
		return nil, ErrInvalidRefSpec
	} else {
		remoteInRef, ok := strhelp.NthToken(remoteRef.GetPath(), '/', 0)

		if !ok {
			return nil, ErrInvalidRefSpec
		} else if remote != "" && remote != remoteInRef {
			return nil, ErrInvalidRefSpec
		}

		if localWCs == 0 {
			localPattern := strPattern(local.GetPath())
			remPattern := strPattern(remoteRef.GetPath())
			localToRemMapper := identityBranchMapper(remoteRef.GetPath()[len(remoteInRef):])
			remRefToLocalMapper := identityBranchMapper(local.GetPath())

			return localToRemoteTrackingRef{
				localPattern:  localPattern,
				remPattern:    remPattern,
				remote:        remoteInRef,
				localToRemRef: localToRemMapper,
				remRefToLocal: remRefToLocalMapper,
			}, nil
		} else {
			localPattern := newWildcardPattern(local.GetPath())
			remPattern := newWildcardPattern(remoteRef.GetPath())
			localToRemMapper := newWildcardBranchMapper(remoteRef.GetPath()[len(remoteInRef)+1:])
			remRefToLocalMapper := newWildcardBranchMapper(local.GetPath())

			return localToRemoteTrackingRef{
				localPattern:  localPattern,
				remPattern:    remPattern,
				remote:        remoteInRef,
				localToRemRef: localToRemMapper,
				remRefToLocal: remRefToLocalMapper,
			}, nil
		}
	}

	return nil, ErrInvalidRefSpec
}

// Map verifies the localRef matches the refspecs local pattern, and then maps it to a remote tracking branch.
func (rs localToRemoteTrackingRef) Map(localRef DoltRef) DoltRef {
	if localRef.GetType() == BranchRefType {
		captured, matches := rs.localPattern.matches(localRef.GetPath())
		if matches {
			return NewRemoteRef(rs.remote, rs.localToRemRef.mapBranch(captured))
		}
	}

	return nil
}

// Map verifies the remoteRef matches the refspecs remote pattern, and then maps it to a local branch
func (rs localToRemoteTrackingRef) MapBack(remRef DoltRef) DoltRef {
	if remRef.GetType() == RemoteRefType {
		captured, matches := rs.remPattern.matches(remRef.GetPath())
		if matches {
			return NewBranchRef(rs.remRefToLocal.mapBranch(captured))
		}
	}

	return nil
}

func (rs localToRemoteTrackingRef) GetRemote() string {
	return rs.remote
}
