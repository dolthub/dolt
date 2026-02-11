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

package dbfactory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// GitCacheRootParam is the absolute path to the local Dolt repository root (the directory that contains `.dolt/`).
	// When set for git remotes, callers can choose a per-repo cache location under `.dolt/`.
	GitCacheRootParam    = "git_cache_root"
	GitRefParam          = "git_ref"
	GitRemoteNameParam   = "git_remote_name"
	defaultGitRef        = "refs/dolt/data"
	defaultGitRemoteName = "origin"
)

// GitCacheRootProvider provides the local Dolt repo root for per-repo git remote caches.
// Implementations should return ok=false when no repo root is available.
type GitCacheRootProvider interface {
	GitCacheRoot() (string, bool)
}

// GitRemoteFactory opens a Dolt database backed by a Git remote, using a local bare
// repository as an object cache and remote configuration store.
//
// Supported schemes (registered in factory.go):
// - git+file
// - git+http
// - git+https
// - git+ssh
type GitRemoteFactory struct{}

var _ DBFactory = GitRemoteFactory{}

func (fact GitRemoteFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	switch strings.ToLower(urlObj.Scheme) {
	case GitFileScheme:
		remoteURL, _, err := parseGitRemoteFactoryURL(urlObj, params)
		if err != nil {
			return err
		}
		if remoteURL.Scheme != "file" {
			return fmt.Errorf("git+file: expected underlying file URL, got %q", remoteURL.Scheme)
		}
		p := filepath.Join(remoteURL.Host, filepath.FromSlash(remoteURL.Path))
		if p == "" {
			return fmt.Errorf("git+file: empty remote path")
		}
		if _, err := os.Stat(p); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return runGitInitBare(ctx, p)
	default:
		return fmt.Errorf("prepare not supported for scheme %q", urlObj.Scheme)
	}
}

func (fact GitRemoteFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	remoteURL, ref, err := parseGitRemoteFactoryURL(urlObj, params)
	if err != nil {
		return nil, nil, nil, err
	}

	cacheRoot, ok, err := resolveGitCacheRoot(params)
	if err != nil {
		return nil, nil, nil, err
	}
	if !ok {
		return nil, nil, nil, fmt.Errorf("%s is required for git remotes", GitCacheRootParam)
	}
	cacheBase := filepath.Join(cacheRoot, DoltDir, "git-remote-cache")

	cacheRepo, err := cacheRepoPath(cacheBase, remoteURL.String(), ref)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := ensureBareRepo(ctx, cacheRepo); err != nil {
		return nil, nil, nil, err
	}

	remoteName := resolveGitRemoteName(params)

	// Ensure the configured git remote exists and points to the underlying git remote URL.
	if err := ensureGitRemoteURL(ctx, cacheRepo, remoteName, remoteURL.String()); err != nil {
		return nil, nil, nil, err
	}

	q := nbs.NewUnlimitedMemQuotaProvider()
	cs, err := nbs.NewGitStore(ctx, nbf.VersionString(), cacheRepo, ref, blobstore.GitBlobstoreOptions{RemoteName: remoteName}, defaultMemTableSize, q)
	if err != nil {
		return nil, nil, nil, err
	}

	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db := datas.NewTypesDatabase(vrw, ns)
	return db, vrw, ns, nil
}

func parseGitRemoteFactoryURL(urlObj *url.URL, params map[string]interface{}) (remoteURL *url.URL, ref string, err error) {
	if urlObj == nil {
		return nil, "", fmt.Errorf("nil url")
	}
	scheme := strings.ToLower(urlObj.Scheme)
	if !strings.HasPrefix(scheme, "git+") {
		return nil, "", fmt.Errorf("expected git+ scheme, got %q", urlObj.Scheme)
	}
	underlyingScheme := strings.TrimPrefix(scheme, "git+")
	if underlyingScheme == "" {
		return nil, "", fmt.Errorf("invalid git+ scheme %q", urlObj.Scheme)
	}

	ref = resolveGitRemoteRef(params)

	cp := *urlObj
	cp.Scheme = underlyingScheme
	cp.RawQuery = ""
	cp.Fragment = ""
	return &cp, ref, nil
}

func resolveGitRemoteRef(params map[string]interface{}) string {
	// Prefer an explicit remote parameter (e.g. from `--ref`).
	if params != nil {
		if v, ok := params[GitRefParam]; ok && v != nil {
			s, ok := v.(string)
			if ok {
				if s = strings.TrimSpace(s); s != "" {
					return s
				}
			}
		}
	}
	return defaultGitRef
}

func resolveGitRemoteName(params map[string]interface{}) string {
	if params != nil {
		if v, ok := params[GitRemoteNameParam]; ok && v != nil {
			s, ok := v.(string)
			if ok {
				if s = strings.TrimSpace(s); s != "" {
					return s
				}
			}
		}
	}
	return defaultGitRemoteName
}

// resolveGitCacheRoot parses and validates the optional GitCacheRootParam.
// It returns ok=false when the param is not present.
func resolveGitCacheRoot(params map[string]interface{}) (root string, ok bool, err error) {
	if params == nil {
		return "", false, nil
	}
	v, ok := params[GitCacheRootParam]
	if !ok || v == nil {
		return "", false, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", false, fmt.Errorf("%s must be a string", GitCacheRootParam)
	}
	if strings.TrimSpace(s) == "" {
		return "", false, fmt.Errorf("%s cannot be empty", GitCacheRootParam)
	}
	return s, true, nil
}

func cacheRepoPath(cacheBase, remoteURL, ref string) (string, error) {
	if strings.TrimSpace(cacheBase) == "" {
		return "", fmt.Errorf("empty git cache base")
	}
	sum := sha256.Sum256([]byte(remoteURL + "|" + ref))
	h := hex.EncodeToString(sum[:])
	return filepath.Join(cacheBase, h, "repo.git"), nil
}

func ensureBareRepo(ctx context.Context, gitDir string) error {
	if gitDir == "" {
		return fmt.Errorf("empty gitDir")
	}
	if st, err := os.Stat(gitDir); err == nil {
		if !st.IsDir() {
			return fmt.Errorf("git cache repo path is not a directory: %s", gitDir)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(gitDir), 0o755); err != nil {
		return err
	}
	return runGitInitBare(ctx, gitDir)
}

func ensureGitRemoteURL(ctx context.Context, gitDir string, remoteName string, remoteURL string) error {
	if strings.TrimSpace(remoteName) == "" {
		return fmt.Errorf("empty remote name")
	}
	if strings.TrimSpace(remoteURL) == "" {
		return fmt.Errorf("empty remote url")
	}
	// Insert `--` so remoteName can't be interpreted as a flag.
	got, err := runGitInDir(ctx, gitDir, "remote", "get-url", "--", remoteName)
	if err != nil {
		// Remote likely doesn't exist; attempt to add.
		return runGitInDirNoOutput(ctx, gitDir, "remote", "add", "--", remoteName, remoteURL)
	}
	got = strings.TrimSpace(got)
	if got == remoteURL {
		return nil
	}
	return runGitInDirNoOutput(ctx, gitDir, "remote", "set-url", "--", remoteName, remoteURL)
}

func runGitInitBare(ctx context.Context, dir string) error {
	_, err := exec.LookPath("git")
	if err != nil {
		return fmt.Errorf("git not found on PATH: %w", err)
	}
	cmd := exec.CommandContext(ctx, "git", "init", "--bare", dir) //nolint:gosec // controlled args
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git init --bare failed: %w\noutput:\n%s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func runGitInDir(ctx context.Context, gitDir string, args ...string) (string, error) {
	_, err := exec.LookPath("git")
	if err != nil {
		return "", fmt.Errorf("git not found on PATH: %w", err)
	}
	all := append([]string{"--git-dir", gitDir}, args...)
	cmd := exec.CommandContext(ctx, "git", all...) //nolint:gosec // controlled args
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s failed: %w\noutput:\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func runGitInDirNoOutput(ctx context.Context, gitDir string, args ...string) error {
	_, err := runGitInDir(ctx, gitDir, args...)
	return err
}
