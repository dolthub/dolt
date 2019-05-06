package ref

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/strhelp"
	"strings"
)

var ErrInvalidRefSpec = errors.New("")
var ErrInvalidMapping = errors.New("")
var ErrUnsupportedMapping = errors.New("unsupported mapping")

type RefSpec interface {
	Map(DoltRef) DoltRef
}

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
		return NewLocalToRemoteTrackingRef(remote, fromRef, toRef)
	}

	return nil, ErrUnsupportedMapping
}

type branchMapper interface {
	MapBranch(string) string
}

type IdentityBranchMapper string

func (ibm IdentityBranchMapper) MapBranch(s string) string {
	return string(ibm)
}

type WildcardBranchMapper struct {
	prefix string
	suffix string
}

func NewWildcardBranchMapper(s string) WildcardBranchMapper {
	tokens := strings.Split(s, "*")

	if len(tokens) != 2 {
		panic("invalid pattern")
	}

	return WildcardBranchMapper{tokens[0], tokens[1]}
}

func (wcbm WildcardBranchMapper) MapBranch(s string) string {
	return wcbm.prefix + s + wcbm.suffix
}

type LocalToRemoteTrackingRef struct {
	pattern    Pattern
	remote     string
	localToRef branchMapper
}

func NewLocalToRemoteTrackingRef(remote string, local DoltRef, remoteRef DoltRef) (RefSpec, error) {
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
			localPattern := StringPattern(local.Path)
			branchMapper := IdentityBranchMapper(remoteRef.Path[len(remote):])

			return LocalToRemoteTrackingRef{localPattern, remote, branchMapper}, nil
		} else {
			localPattern := NewWildcardPattern(local.Path)
			branchMapper := NewWildcardBranchMapper(remoteRef.Path[len(remote)+1:])

			return LocalToRemoteTrackingRef{localPattern, remote, branchMapper}, nil
		}
	}

	return nil, ErrInvalidRefSpec
}

func (rs LocalToRemoteTrackingRef) Map(local DoltRef) DoltRef {
	if local.Type == BranchRef {
		captured, matches := rs.pattern.Matches(local.Path)
		if matches {
			return NewRemoteRef(rs.remote, rs.localToRef.MapBranch(captured))
		}
	}

	return InvalidRef
}
