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

// ReadAPI defines the git plumbing operations used by read paths. Like WriteAPI,
// it is designed to support swapping implementations (e.g. git CLI vs a Go git library)
// while keeping callers (such as GitBlobstore) stable.
type ReadAPI interface {
	// TryResolveRefCommit resolves |ref| to a commit OID. Returns ok=false if the ref does not exist.
	TryResolveRefCommit(ctx context.Context, ref string) (oid OID, ok bool, err error)

	// ResolveRefCommit resolves |ref| to a commit OID, returning RefNotFoundError if missing.
	ResolveRefCommit(ctx context.Context, ref string) (OID, error)

	// ResolvePathBlob resolves |path| within |commit| to a blob OID.
	// It returns PathNotFoundError if the path does not exist, and NotBlobError if it
	// resolves to a non-blob object.
	ResolvePathBlob(ctx context.Context, commit OID, path string) (OID, error)

	// CatFileType returns the git object type for |oid| (e.g. "blob", "tree", "commit").
	CatFileType(ctx context.Context, oid OID) (string, error)

	// BlobSize returns the size in bytes of the blob object |oid|.
	BlobSize(ctx context.Context, oid OID) (int64, error)

	// BlobReader returns a reader for blob contents.
	BlobReader(ctx context.Context, oid OID) (io.ReadCloser, error)
}
