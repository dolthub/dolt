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

// Package gitrepo contains test helpers for creating and manipulating git repositories
// using plumbing commands without requiring a working tree checkout.
//
// This package is intended for tests of GitBlobstore and related read paths. It
// deliberately uses the git CLI (not a Go git library) to keep the harness small
// and to match how the initial GitBlobstore implementation interacts with git.
package gitrepo

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Repo is a test-only handle to a bare git repository (its directory is the GIT_DIR).
type Repo struct {
	// GitDir is the path to the bare repository directory.
	GitDir string
}

// InitBare initializes a new bare git repository at |dir| (which should not exist yet).
func InitBare(ctx context.Context, dir string) (*Repo, error) {
	if err := runGit(ctx, "", "", "", "init", "--bare", dir); err != nil {
		return nil, err
	}
	return &Repo{GitDir: dir}, nil
}

// InitBareTemp creates and initializes a new bare git repository under |parentDir|
// (or os.TempDir if empty).
func InitBareTemp(ctx context.Context, parentDir string) (*Repo, error) {
	if parentDir == "" {
		parentDir = os.TempDir()
	}
	dir, err := os.MkdirTemp(parentDir, "gitrepo-bare-")
	if err != nil {
		return nil, err
	}
	// git init --bare expects the target directory to not exist in some versions;
	// to avoid that, create a child directory.
	bareDir := filepath.Join(dir, "repo.git")
	return InitBare(ctx, bareDir)
}

// SetRefToTree writes a commit whose tree contains |files| and updates |ref| to point at it.
// This is done without a working tree checkout using a temporary index (GIT_INDEX_FILE).
//
// - |ref| example: "refs/dolt/data"
// - |files| keys are tree paths (e.g. "manifest", "a/b/c")
// - |message| becomes the commit message (defaults to "test commit" if empty)
func (r *Repo) SetRefToTree(ctx context.Context, ref string, files map[string][]byte, message string) (commitOID string, err error) {
	if message == "" {
		message = "test commit"
	}

	indexDir, err := os.MkdirTemp("", "gitrepo-index-")
	if err != nil {
		return "", err
	}
	defer func() {
		_ = os.RemoveAll(indexDir)
	}()

	indexFile := filepath.Join(indexDir, "index")

	// Empty index.
	if err := runGit(ctx, r.GitDir, indexFile, "", "read-tree", "--empty"); err != nil {
		return "", err
	}

	// Add paths. Sort for determinism.
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		oid, err := hashObject(ctx, r.GitDir, files[p])
		if err != nil {
			return "", err
		}
		if err := runGit(ctx, r.GitDir, indexFile, "", "update-index", "--add", "--cacheinfo", "100644", oid, p); err != nil {
			return "", err
		}
	}

	treeOID, err := outputGit(ctx, r.GitDir, indexFile, nil, "write-tree")
	if err != nil {
		return "", err
	}
	treeOID = strings.TrimSpace(treeOID)
	if treeOID == "" {
		return "", fmt.Errorf("write-tree returned empty oid")
	}

	commitOID, err = outputGit(ctx, r.GitDir, "", commitEnv(), "commit-tree", treeOID, "-m", message)
	if err != nil {
		return "", err
	}
	commitOID = strings.TrimSpace(commitOID)
	if commitOID == "" {
		return "", fmt.Errorf("commit-tree returned empty oid")
	}

	if err := runGit(ctx, r.GitDir, "", "", "update-ref", ref, commitOID); err != nil {
		return "", err
	}
	return commitOID, nil
}

func commitEnv() []string {
	// Deterministic-ish author/committer identity for tests.
	return []string{
		"GIT_AUTHOR_NAME=gitrepo test",
		"GIT_AUTHOR_EMAIL=gitrepo@test.invalid",
		"GIT_COMMITTER_NAME=gitrepo test",
		"GIT_COMMITTER_EMAIL=gitrepo@test.invalid",
	}
}

func hashObject(ctx context.Context, gitDir string, data []byte) (string, error) {
	out, err := outputGitWithStdin(ctx, gitDir, "", "", bytes.NewReader(data), "hash-object", "-w", "--stdin")
	if err != nil {
		return "", err
	}
	oid := strings.TrimSpace(out)
	if oid == "" {
		return "", fmt.Errorf("hash-object returned empty oid")
	}
	return oid, nil
}

func runGit(ctx context.Context, gitDir, indexFile string, extraEnv string, args ...string) error {
	_, err := outputGit(ctx, gitDir, indexFile, splitEnv(extraEnv), args...)
	return err
}

func outputGit(ctx context.Context, gitDir, indexFile string, extraEnv []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...) //nolint:gosec // test harness invokes git with controlled args.
	cmd.Env = envForGit(gitDir, indexFile, extraEnv)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w\ncommand: %s\noutput:\n%s", err, cmd.String(), strings.TrimRight(buf.String(), "\n"))
	}
	return buf.String(), nil
}

func outputGitWithStdin(ctx context.Context, gitDir, indexFile string, extraEnv string, stdin *bytes.Reader, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...) //nolint:gosec // test harness invokes git with controlled args.
	cmd.Env = envForGit(gitDir, indexFile, splitEnv(extraEnv))
	cmd.Stdin = stdin
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w\ncommand: %s\noutput:\n%s", err, cmd.String(), strings.TrimRight(buf.String(), "\n"))
	}
	return buf.String(), nil
}

func envForGit(gitDir, indexFile string, extra []string) []string {
	env := append([]string(nil), os.Environ()...)
	if gitDir != "" {
		env = append(env, "GIT_DIR="+gitDir)
	}
	if indexFile != "" {
		env = append(env, "GIT_INDEX_FILE="+indexFile)
	}
	env = append(env, extra...)
	return env
}

func splitEnv(extraEnv string) []string {
	if extraEnv == "" {
		return nil
	}
	// Allow callers to pass "K=V\nK2=V2" style strings.
	lines := strings.Split(extraEnv, "\n")
	out := lines[:0]
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}
