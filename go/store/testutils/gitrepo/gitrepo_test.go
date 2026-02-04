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

package gitrepo

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitBareAndSetRefToTree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found on PATH")
	}

	ctx := context.Background()
	root := t.TempDir()
	bareDir := filepath.Join(root, "repo.git")

	repo, err := InitBare(ctx, bareDir)
	if err != nil {
		t.Fatalf("InitBare failed: %v", err)
	}

	commit, err := repo.SetRefToTree(ctx, "refs/dolt/data", map[string][]byte{
		"manifest":   []byte("hello\n"),
		"dir/file":   []byte("abc"),
		"dir/file2":  []byte("def"),
		"dir2/x.txt": []byte("xyz"),
	}, "seed refs/dolt/data")
	if err != nil {
		t.Fatalf("SetRefToTree failed: %v", err)
	}
	if len(strings.TrimSpace(commit)) == 0 {
		t.Fatalf("expected non-empty commit oid")
	}

	// Validate the path exists in the commit.
	cmd := exec.CommandContext(ctx, "git", "--git-dir", repo.GitDir, "cat-file", "-e", commit+":manifest") //nolint:gosec
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cat-file -e failed: %v\n%s", err, string(out))
	}
}
