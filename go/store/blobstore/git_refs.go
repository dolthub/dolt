// Copyright 2026 Dolthub, Inc.
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

package blobstore

import (
	"fmt"
	"strings"
)

// DoltDataRef is the default remote ref backing a git-object-db dolt blobstore.
const DoltDataRef = "refs/dolt/data"

func trimRefsPrefix(ref string) string {
	return strings.TrimPrefix(ref, "refs/")
}

// RemoteTrackingRef returns the remote-tracking ref for a named remote and remote ref.
// This ref represents the remote's |remoteRef| as of the last fetch.
func RemoteTrackingRef(remoteName, remoteRef string) string {
	return fmt.Sprintf("refs/dolt/remotes/%s/%s", remoteName, trimRefsPrefix(remoteRef))
}

// OwnedLocalRef returns a UUID-owned local ref for a GitBlobstore instance.
//
// Note: these UUID refs can accumulate in the local repo over time. This is
// intentional for now; callers that want best-effort cleanup can use
// (*GitBlobstore).CleanupOwnedLocalRef.
func OwnedLocalRef(remoteName, remoteRef, uuid string) string {
	return fmt.Sprintf("refs/dolt/blobstore/%s/%s/%s", remoteName, trimRefsPrefix(remoteRef), uuid)
}
