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

import "fmt"

// DoltDataRef is the local writable ref backing a git-object-db dolt blobstore.
// It is the state that local operations mutate and (eventually) attempt to push.
const DoltDataRef = "refs/dolt/data"

// DoltRemoteTrackingDataRef returns the remote-tracking ref for a named remote.
// This ref represents the remote's DoltDataRef as of the last fetch.
func DoltRemoteTrackingDataRef(remote string) string {
	return fmt.Sprintf("refs/dolt/remotes/%s/data", remote)
}
