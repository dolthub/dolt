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
	"os"
	"strings"
)

// DoltDataRef is the default remote ref backing a git-object-db dolt blobstore.
const DoltDataRef = "refs/dolt/data"

// DefaultInfoBranch is the default short branch name for the visible info branch
// that indicates a repository is being used as a Dolt remote.
const DefaultInfoBranch = "__dolt_remote_info__"

// InfoBranchEnvVar is the environment variable that overrides the info branch name.
// Set to an empty string to disable the info branch entirely.
const InfoBranchEnvVar = "DOLT_REMOTE_INFO_BRANCH"

// ResolveInfoBranch returns the info branch name from the environment, falling
// back to the provided default. An empty return value means "disabled".
func ResolveInfoBranch(defaultBranch string) string {
	if v, ok := os.LookupEnv(InfoBranchEnvVar); ok {
		return strings.TrimSpace(v)
	}
	return defaultBranch
}

func trimRefsPrefix(ref string) string {
	return strings.TrimPrefix(ref, "refs/")
}

// RemoteTrackingRef returns a UUID-owned remote-tracking ref for a GitBlobstore instance.
// Each instance gets its own tracking ref to avoid concurrent git-fetch races
// when multiple blobstore instances share the same cache repo.
func RemoteTrackingRef(remoteName, remoteRef, uuid string) string {
	return fmt.Sprintf("refs/dolt/remotes/%s/%s/%s", remoteName, trimRefsPrefix(remoteRef), uuid)
}

// OwnedLocalRef returns a UUID-owned local ref for a GitBlobstore instance.
//
// Note: these UUID refs can accumulate in the local repo over time. This is
// intentional for now; callers that want best-effort cleanup can use
// (*GitBlobstore).CleanupOwnedLocalRef.
func OwnedLocalRef(remoteName, remoteRef, uuid string) string {
	return fmt.Sprintf("refs/dolt/blobstore/%s/%s/%s", remoteName, trimRefsPrefix(remoteRef), uuid)
}
