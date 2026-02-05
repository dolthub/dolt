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
	"context"

	git "github.com/dolthub/dolt/go/store/blobstore/internal/git"
	gitbs "github.com/dolthub/dolt/go/store/blobstore/internal/gitbs"
)

const (
	// gitblobstorePartFileMode is the canonical filemode used for part blobs staged into the tree.
	gitblobstorePartFileMode = "100644"
)

// stagePartReachable stages a tree entry for |partOID| into |indexFile| under the reserved
// parts namespace, ensuring the blob is reachable from the resulting tree/commit.
//
// This operation is idempotent: staging the same part OID at the same computed path twice
// should result in the same index state.
func stagePartReachable(ctx context.Context, api git.GitAPI, indexFile string, partOID git.OID) (path string, err error) {
	path, err = gitbs.PartPath(partOID.String())
	if err != nil {
		return "", err
	}
	return path, api.UpdateIndexCacheInfo(ctx, indexFile, gitblobstorePartFileMode, partOID, path)
}
