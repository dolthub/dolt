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

	if fromRef.Type == BranchRef && toRef.Type == RemoteRef {
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
		panic("invalid pattern")
	}

	return wcBranchMapper{tokens[0], tokens[1]}
}

func (wcbm wcBranchMapper) mapBranch(s string) string {
	return wcbm.prefix + s + wcbm.suffix
}

type localToRemoteTrackingRef struct {
	pattern    pattern
	remote     string
	localToRef branchMapper
}

func newLocalToRemoteTrackingRef(remote string, local DoltRef, remoteRef DoltRef) (RefSpec, error) {
	localWCs := strings.Count(local.Path, "*")
	remoteWCs := strings.Count(remoteRef.Path, "*")

	if localWCs != remoteWCs || localWCs > 1 {
		return nil, ErrInvalidRefSpec
	} else {
		remoteInRef, ok := strhelp.NthToken(remoteRef.Path, '/', 0)

		if !ok || remote != remoteInRef {
			return nil, ErrInvalidRefSpec
		}

		if localWCs == 0 {
			localPattern := strPattern(local.Path)
			branchMapper := identityBranchMapper(remoteRef.Path[len(remote):])

			return localToRemoteTrackingRef{localPattern, remote, branchMapper}, nil
		} else {
			localPattern := newWildcardPattern(local.Path)
			branchMapper := newWildcardBranchMapper(remoteRef.Path[len(remote)+1:])

			return localToRemoteTrackingRef{localPattern, remote, branchMapper}, nil
		}
	}

	return nil, ErrInvalidRefSpec
}

// Map verifies the localRef matches the refspecs pattern, and then maps it to a remote tracking branch.
func (rs localToRemoteTrackingRef) Map(localRef DoltRef) DoltRef {
	if localRef.Type == BranchRef {
		captured, matches := rs.pattern.matches(localRef.Path)
		if matches {
			return NewRemoteRef(rs.remote, rs.localToRef.mapBranch(captured))
		}
	}

	return InvalidRef
}
