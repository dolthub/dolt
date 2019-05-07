package ref

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/strhelp"
	"path"
	"strings"
)

type RemoteRef struct {
	remote string
	branch string
}

func (rr RemoteRef) GetType() RefType {
	return RemoteRefType
}

func (rr RemoteRef) GetPath() string {
	return path.Join(rr.remote, rr.branch)
}

func (rr RemoteRef) String() string {
	return String(rr)
}

func (rr RemoteRef) GetRemote() string {
	return rr.remote
}

// NewRemoteRef creates a remote ref from an origin name and a path
func NewRemoteRef(remote, branch string) RemoteRef {
	return RemoteRef{remote, branch}
}

// NewRemoteRefFromPathString creates a DoltRef from a string in the format origin/master, or remotes/origin/master, or
// refs/remotes/origin/master
func NewRemoteRefFromPathStr(remoteAndPath string) (DoltRef, error) {
	if IsRef(remoteAndPath) {
		prefix := PrefixForType(RemoteRefType)
		if strings.HasPrefix(remoteAndPath, prefix) {
			remoteAndPath = remoteAndPath[len(prefix):]
		} else {
			panic(remoteAndPath + " is a ref that is not of type " + prefix)
		}
	} else if strings.HasPrefix(remoteAndPath, remotesPrefix) {
		remoteAndPath = remoteAndPath[len(remotesPrefix):]
	}

	remote, ok := strhelp.NthToken(remoteAndPath, '/', 0)

	if !ok {
		return nil, ErrInvalidRefSpec
	}

	branch := remoteAndPath[len(remote)+1:]

	if remote == "" || branch == "" {
		return nil, ErrInvalidRefSpec
	}

	return RemoteRef{remote, branch}, nil
}
