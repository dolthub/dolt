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
	"errors"
	"io"
)

// ReadAllBytes is a small helper for read-path callers that want a whole object.
// This is not used by GitBlobstore.Get (which must support BlobRange), but it is useful in tests.
func ReadAllBytes(ctx context.Context, r *Runner, oid OID) ([]byte, error) {
	rc, err := NewGitAPIImpl(r).BlobReader(ctx, oid)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// NormalizeGitPlumbingError unwraps CmdError wrappers, returning the underlying error.
// Mostly useful for callers that want to compare against context cancellation.
func NormalizeGitPlumbingError(err error) error {
	var ce *CmdError
	if errors.As(err, &ce) && ce.Cause != nil {
		return ce.Cause
	}
	return err
}
