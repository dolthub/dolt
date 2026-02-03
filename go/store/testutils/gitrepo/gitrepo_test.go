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

