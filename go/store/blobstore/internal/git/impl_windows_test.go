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

//go:build windows

package git

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestGitAPIImpl_FetchRef_FetchHeadHeldOpen(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11904
	t.Parallel()

	ctx := context.Background()
	localRepo, _, localAPI, remoteHead := newFetchableRepos(t, ctx)
	path := filepath.Join(localRepo.GitDir, "FETCH_HEAD")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ, syscall.FILE_SHARE_READ, nil,
		syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(h)

	if err := localAPI.FetchRef(ctx, "origin", "refs/dolt/data", "refs/dolt/remotes/origin/data"); err != nil {
		t.Fatalf("FetchRef failed while another process holds FETCH_HEAD open: %v", err)
	}
	requireTrackingRef(t, ctx, localAPI, remoteHead)
}
