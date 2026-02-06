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
	"io"
)

// GitAPI defines the git plumbing operations needed by GitBlobstore. It includes both
// read and write operations to allow swapping implementations (e.g. git CLI vs a Go git
// library) while keeping callers stable.
type GitAPI interface {
	// TryResolveRefCommit resolves |ref| to a commit OID. Returns ok=false if the ref does not exist.
	TryResolveRefCommit(ctx context.Context, ref string) (oid OID, ok bool, err error)

	// ResolveRefCommit resolves |ref| to a commit OID, returning RefNotFoundError if missing.
	ResolveRefCommit(ctx context.Context, ref string) (OID, error)

	// ResolvePathBlob resolves |path| within |commit| to a blob OID.
	// It returns PathNotFoundError if the path does not exist, and NotBlobError if it
	// resolves to a non-blob object.
	ResolvePathBlob(ctx context.Context, commit OID, path string) (OID, error)

	// ResolvePathObject resolves |path| within |commit| to an object OID and type.
	// It returns PathNotFoundError if the path does not exist.
	//
	// Typical types are "blob" and "tree".
	ResolvePathObject(ctx context.Context, commit OID, path string) (oid OID, typ string, err error)

	// ListTree lists the entries of the tree at |treePath| within |commit|.
	// The listing is non-recursive: it returns only immediate children.
	//
	// It returns PathNotFoundError if |treePath| does not exist.
	ListTree(ctx context.Context, commit OID, treePath string) ([]TreeEntry, error)

	// CatFileType returns the git object type for |oid| (e.g. "blob", "tree", "commit").
	CatFileType(ctx context.Context, oid OID) (string, error)

	// BlobSize returns the size in bytes of the blob object |oid|.
	BlobSize(ctx context.Context, oid OID) (int64, error)

	// BlobReader returns a reader for blob contents.
	BlobReader(ctx context.Context, oid OID) (io.ReadCloser, error)

	// HashObject writes a new blob object for the provided contents and returns its OID.
	// Equivalent plumbing:
	//   GIT_DIR=... git hash-object -w --stdin
	HashObject(ctx context.Context, contents io.Reader) (OID, error)

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

	// RemoveIndexPaths removes |paths| from |indexFile| if present.
	// Equivalent plumbing:
	//   GIT_DIR=... GIT_INDEX_FILE=<indexFile> git update-index --remove -z --stdin
	RemoveIndexPaths(ctx context.Context, indexFile string, paths []string) error

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

// TreeEntry describes one entry in a git tree listing.
type TreeEntry struct {
	Mode string
	Type string
	OID  OID
	Name string
}

// Identity represents git author/committer metadata. A future implementation may set
// this via environment variables (GIT_AUTHOR_NAME, etc.).
type Identity struct {
	Name  string
	Email string
}
