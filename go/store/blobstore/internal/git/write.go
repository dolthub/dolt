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

package git

import (
	"context"
	"fmt"
)

// WriteAPI defines the git plumbing operations needed for Approach A (temporary index
// via GIT_INDEX_FILE) to perform updates without a working tree checkout.
//
// This file intentionally does not implement these operations yet; the current
// GitBlobstore milestone is read-only. All methods on the default implementation
// return ErrUnimplemented.
type WriteAPI interface {
	// ReadTree populates |indexFile| with the entries from |commit|'s root tree.
	// Equivalent plumbing:
	//   GIT_DIR=... GIT_INDEX_FILE=<indexFile> git read-tree <commit>^{tree}
	ReadTree(ctx context.Context, commit OID, indexFile string) error

	// ReadTreeEmpty initializes |indexFile| to an empty index.
	// Equivalent plumbing:
	//   GIT_DIR=... GIT_INDEX_FILE=<indexFile> git read-tree --empty
	ReadTreeEmpty(ctx context.Context, indexFile string) error

	// UpdateIndexCacheInfo adds or replaces |path| in |indexFile| with the given blob |oid| and filemode.
	// Equivalent plumbing:
	//   GIT_DIR=... GIT_INDEX_FILE=<indexFile> git update-index --add --cacheinfo <mode> <oid> <path>
	UpdateIndexCacheInfo(ctx context.Context, indexFile string, mode string, oid OID, path string) error

	// WriteTree writes a tree object from the contents of |indexFile| and returns its oid.
	// Equivalent plumbing:
	//   GIT_DIR=... GIT_INDEX_FILE=<indexFile> git write-tree
	WriteTree(ctx context.Context, indexFile string) (OID, error)

	// CommitTree creates a commit object from |tree| with optional |parent| and returns its oid.
	// Equivalent plumbing:
	//   GIT_DIR=... git commit-tree <tree> [-p <parent>] -m <message>
	CommitTree(ctx context.Context, tree OID, parent *OID, message string, author *Identity) (OID, error)

	// UpdateRefCAS atomically updates |ref| from |old| to |new|.
	// Equivalent plumbing:
	//   GIT_DIR=... git update-ref -m <msg> <ref> <new> <old>
	UpdateRefCAS(ctx context.Context, ref string, newOID OID, oldOID OID, msg string) error

	// UpdateRef updates |ref| to |new| without a compare-and-swap.
	// Equivalent plumbing:
	//   GIT_DIR=... git update-ref -m <msg> <ref> <new>
	UpdateRef(ctx context.Context, ref string, newOID OID, msg string) error
}

// Identity represents git author/committer metadata. A future implementation
// may set this via environment variables (GIT_AUTHOR_NAME, etc.).
type Identity struct {
	Name  string
	Email string
}

// UnimplementedWriteAPI is the default write API for the read-only milestone.
// It can be embedded or returned by constructors to make write paths fail fast.
type UnimplementedWriteAPI struct{}

var _ WriteAPI = UnimplementedWriteAPI{}

func (UnimplementedWriteAPI) ReadTree(ctx context.Context, commit OID, indexFile string) error {
	return fmt.Errorf("%w: ReadTree", ErrUnimplemented)
}

func (UnimplementedWriteAPI) ReadTreeEmpty(ctx context.Context, indexFile string) error {
	return fmt.Errorf("%w: ReadTreeEmpty", ErrUnimplemented)
}

func (UnimplementedWriteAPI) UpdateIndexCacheInfo(ctx context.Context, indexFile string, mode string, oid OID, path string) error {
	return fmt.Errorf("%w: UpdateIndexCacheInfo", ErrUnimplemented)
}

func (UnimplementedWriteAPI) WriteTree(ctx context.Context, indexFile string) (OID, error) {
	return "", fmt.Errorf("%w: WriteTree", ErrUnimplemented)
}

func (UnimplementedWriteAPI) CommitTree(ctx context.Context, tree OID, parent *OID, message string, author *Identity) (OID, error) {
	return "", fmt.Errorf("%w: CommitTree", ErrUnimplemented)
}

func (UnimplementedWriteAPI) UpdateRefCAS(ctx context.Context, ref string, newOID OID, oldOID OID, msg string) error {
	return fmt.Errorf("%w: UpdateRefCAS", ErrUnimplemented)
}

func (UnimplementedWriteAPI) UpdateRef(ctx context.Context, ref string, newOID OID, msg string) error {
	return fmt.Errorf("%w: UpdateRef", ErrUnimplemented)
}

