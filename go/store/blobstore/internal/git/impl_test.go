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
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/dolthub/dolt/go/store/testutils/gitrepo"
)

func TestGitAPIImpl_HashObject_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	want := []byte("hello dolt\n")
	oid, err := api.HashObject(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	if oid == "" {
		t.Fatalf("expected non-empty oid")
	}

	typ, err := api.CatFileType(ctx, oid)
	if err != nil {
		t.Fatal(err)
	}
	if typ != "blob" {
		t.Fatalf("expected type blob, got %q", typ)
	}

	rc, err := api.BlobReader(ctx, oid)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("blob mismatch: got %q, want %q", string(got), string(want))
	}
}

func TestGitAPIImpl_HashObject_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo, err := gitrepo.InitBareTemp(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(repo.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	api := NewGitAPIImpl(r)

	oid, err := api.HashObject(ctx, bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if oid == "" {
		t.Fatalf("expected non-empty oid")
	}

	sz, err := api.BlobSize(ctx, oid)
	if err != nil {
		t.Fatal(err)
	}
	if sz != 0 {
		t.Fatalf("expected size 0, got %d", sz)
	}
}
