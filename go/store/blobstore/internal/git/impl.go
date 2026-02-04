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
	"fmt"
	"io"
	"strconv"
	"strings"
)

// GitAPIImpl implements GitAPI using the git CLI plumbing commands, via Runner.
// It supports reads and writes (temporary index via GIT_INDEX_FILE)
// without requiring a working tree checkout.
type GitAPIImpl struct {
	r *Runner
}

var _ GitAPI = (*GitAPIImpl)(nil)

func NewGitAPIImpl(r *Runner) *GitAPIImpl {
	return &GitAPIImpl{r: r}
}

func (a *GitAPIImpl) TryResolveRefCommit(ctx context.Context, ref string) (oid OID, ok bool, err error) {
	out, err := a.r.Run(ctx, RunOptions{}, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
	if err == nil {
		s := strings.TrimSpace(string(out))
		if s == "" {
			// Shouldn't happen, but treat as missing.
			return "", false, nil
		}
		return OID(s), true, nil
	}

	if isRefNotFoundErr(err) {
		return "", false, nil
	}
	return "", false, err
}

func (a *GitAPIImpl) ResolveRefCommit(ctx context.Context, ref string) (OID, error) {
	oid, ok, err := a.TryResolveRefCommit(ctx, ref)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", &RefNotFoundError{Ref: ref}
	}
	return oid, nil
}

func (a *GitAPIImpl) ResolvePathBlob(ctx context.Context, commit OID, path string) (OID, error) {
	spec := commit.String() + ":" + path
	out, err := a.r.Run(ctx, RunOptions{}, "rev-parse", "--verify", spec)
	if err != nil {
		if isPathNotFoundErr(err) {
			return "", &PathNotFoundError{Commit: commit.String(), Path: path}
		}
		return "", err
	}
	oid := strings.TrimSpace(string(out))
	if oid == "" {
		return "", fmt.Errorf("git rev-parse returned empty oid for %q", spec)
	}

	typ, err := a.CatFileType(ctx, OID(oid))
	if err != nil {
		return "", err
	}
	if typ != "blob" {
		return "", &NotBlobError{Commit: commit.String(), Path: path, Type: typ}
	}
	return OID(oid), nil
}

func (a *GitAPIImpl) CatFileType(ctx context.Context, oid OID) (string, error) {
	out, err := a.r.Run(ctx, RunOptions{}, "cat-file", "-t", oid.String())
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (a *GitAPIImpl) BlobSize(ctx context.Context, oid OID) (int64, error) {
	out, err := a.r.Run(ctx, RunOptions{}, "cat-file", "-s", oid.String())
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(out))
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("git cat-file -s parse error (%q): %w", s, err)
	}
	return n, nil
}

func (a *GitAPIImpl) BlobReader(ctx context.Context, oid OID) (io.ReadCloser, error) {
	rc, _, err := a.r.Start(ctx, RunOptions{}, "cat-file", "blob", oid.String())
	return rc, err
}

func (a *GitAPIImpl) HashObject(ctx context.Context, contents io.Reader) (OID, error) {
	out, err := a.r.Run(ctx, RunOptions{Stdin: contents}, "hash-object", "-w", "--stdin")
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(out))
	if len(fields) != 1 {
		return "", fmt.Errorf("git hash-object returned unexpected output: %q", strings.TrimSpace(string(out)))
	}
	return OID(fields[0]), nil
}

func (a *GitAPIImpl) ReadTree(ctx context.Context, commit OID, indexFile string) error {
	_, err := a.r.Run(ctx, RunOptions{IndexFile: indexFile}, "read-tree", commit.String()+"^{tree}")
	return err
}

func (a *GitAPIImpl) ReadTreeEmpty(ctx context.Context, indexFile string) error {
	_, err := a.r.Run(ctx, RunOptions{IndexFile: indexFile}, "read-tree", "--empty")
	return err
}

func (a *GitAPIImpl) UpdateIndexCacheInfo(ctx context.Context, indexFile string, mode string, oid OID, path string) error {
	_, err := a.r.Run(ctx, RunOptions{IndexFile: indexFile}, "update-index", "--add", "--cacheinfo", mode, oid.String(), path)
	return err
}

func (a *GitAPIImpl) WriteTree(ctx context.Context, indexFile string) (OID, error) {
	out, err := a.r.Run(ctx, RunOptions{IndexFile: indexFile}, "write-tree")
	if err != nil {
		return "", err
	}
	oid := strings.TrimSpace(string(out))
	if oid == "" {
		return "", fmt.Errorf("git write-tree returned empty oid")
	}
	return OID(oid), nil
}

func (a *GitAPIImpl) CommitTree(ctx context.Context, tree OID, parent *OID, message string, author *Identity) (OID, error) {
	args := []string{"commit-tree", tree.String(), "-m", message}
	if parent != nil && parent.String() != "" {
		args = append(args, "-p", parent.String())
	}

	var env []string
	if author != nil {
		if author.Name != "" {
			env = append(env,
				"GIT_AUTHOR_NAME="+author.Name,
				"GIT_COMMITTER_NAME="+author.Name,
			)
		}
		if author.Email != "" {
			env = append(env,
				"GIT_AUTHOR_EMAIL="+author.Email,
				"GIT_COMMITTER_EMAIL="+author.Email,
			)
		}
	}

	out, err := a.r.Run(ctx, RunOptions{Env: env}, args...)
	if err != nil {
		return "", err
	}
	oid := strings.TrimSpace(string(out))
	if oid == "" {
		return "", fmt.Errorf("git commit-tree returned empty oid")
	}
	return OID(oid), nil
}

func (a *GitAPIImpl) UpdateRefCAS(ctx context.Context, ref string, newOID OID, oldOID OID, msg string) error {
	args := []string{"update-ref"}
	if msg != "" {
		args = append(args, "-m", msg)
	}
	args = append(args, ref, newOID.String(), oldOID.String())
	_, err := a.r.Run(ctx, RunOptions{}, args...)
	return err
}

func (a *GitAPIImpl) UpdateRef(ctx context.Context, ref string, newOID OID, msg string) error {
	args := []string{"update-ref"}
	if msg != "" {
		args = append(args, "-m", msg)
	}
	args = append(args, ref, newOID.String())
	_, err := a.r.Run(ctx, RunOptions{}, args...)
	return err
}

func isRefNotFoundErr(err error) bool {
	ce, ok := err.(*CmdError)
	if !ok {
		return false
	}
	// For `git rev-parse --verify --quiet <ref>^{commit}`, a missing ref typically yields exit 1 and no output.
	if ce.ExitCode == 1 && len(bytes.TrimSpace(ce.Output)) == 0 {
		return true
	}
	// Some git versions may still emit "fatal: Needed a single revision" without --quiet; keep a defensive check.
	msg := strings.ToLower(string(ce.Output))
	return strings.Contains(msg, "needed a single revision") ||
		strings.Contains(msg, "unknown revision") ||
		strings.Contains(msg, "not a valid object name")
}

func isPathNotFoundErr(err error) bool {
	ce, ok := err.(*CmdError)
	if !ok {
		return false
	}
	if ce.ExitCode == 128 || ce.ExitCode == 1 {
		msg := strings.ToLower(string(ce.Output))
		// Common patterns:
		// - "fatal: Path 'x' does not exist in 'HEAD'"
		// - "fatal: invalid object name 'HEAD:x'"
		// - "fatal: Needed a single revision"
		// - "fatal: ambiguous argument '...': unknown revision or path not in the working tree."
		if strings.Contains(msg, "does not exist in") ||
			strings.Contains(msg, "invalid object name") ||
			strings.Contains(msg, "needed a single revision") ||
			strings.Contains(msg, "unknown revision or path not in the working tree") {
			return true
		}
	}
	return false
}
