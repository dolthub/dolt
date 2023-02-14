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
	"path"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/strhelp"
)

// RemoteRef is a reference to a reference that tracks a branch on a remote
type RemoteRef struct {
	remote string
	branch string
}

// GetType returns RemoteRefType
func (rr RemoteRef) GetType() RefType {
	return RemoteRefType
}

// GetPath returns the remote name separated by the branch e.g. origin/main
func (rr RemoteRef) GetPath() string {
	return path.Join(rr.remote, rr.branch)
}

// String returns the fully qualified reference e.g. refs/remotes/origin/main
func (rr RemoteRef) String() string {
	return String(rr)
}

// GetRemote returns the name of the remote that this reference is referring to.
func (rr RemoteRef) GetRemote() string {
	return rr.remote
}

// GetBranch returns the name of a remote branch
func (rr RemoteRef) GetBranch() string {
	return rr.branch
}

// NewRemoteRef creates a remote ref from an origin name and a path
func NewRemoteRef(remote, branch string) RemoteRef {
	return RemoteRef{remote, branch}
}

// NewRemoteRefFromPathStr creates a DoltRef from a string in the format origin/main, or remotes/origin/main, or
// refs/remotes/origin/main
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

	if len(remote)+1 > len(remoteAndPath) {
		return nil, ErrInvalidRefSpec
	}

	branch := remoteAndPath[len(remote)+1:]

	if remote == "" || branch == "" {
		return nil, ErrInvalidRefSpec
	}

	return RemoteRef{remote, branch}, nil
}
