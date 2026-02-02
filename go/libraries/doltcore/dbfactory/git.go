// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/gitremote"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// GitScheme is the URL scheme for git-backed remotes
	GitScheme = "git"

	// GitRefParam is the parameter name for specifying a custom git ref
	GitRefParam = "ref"

	// GitLocalPathParam is the parameter name for specifying a local cache directory
	GitLocalPathParam = "local_path"
)

// GitFactory is a DBFactory implementation for creating git repository backed databases
type GitFactory struct{}

// PrepareDB initializes a git repository as a dolt remote by creating the
// .dolt_remote/ directory structure on the custom ref.
func (fact GitFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	repoURL := gitURLFromURLObj(urlObj)
	ref := gitRefFromParams(params)

	// Detect authentication
	auth, err := gitremote.DetectAuth(repoURL)
	if err != nil {
		return fmt.Errorf("failed to detect git auth: %w", err)
	}

	// Create a temporary directory for git operations
	localPath, err := os.MkdirTemp("", "dolt-git-prepare-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(localPath)

	// Open the repository
	repo, err := gitremote.Open(ctx, gitremote.OpenOptions{
		URL:       repoURL,
		Ref:       ref,
		Auth:      auth,
		LocalPath: localPath,
	})
	if err != nil {
		return fmt.Errorf("failed to open git repository: %w", err)
	}
	defer repo.Close()

	// Initialize the remote structure
	if err := repo.InitRemote(ctx); err != nil {
		return fmt.Errorf("failed to initialize git remote: %w", err)
	}

	return nil
}

// CreateDB creates a git repository backed database
func (fact GitFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	repoURL := gitURLFromURLObj(urlObj)
	ref := gitRefFromParams(params)
	localPath := gitLocalPathFromParams(params)

	// Create the GitBlobstore
	bs, err := blobstore.NewGitBlobstore(ctx, repoURL, ref, localPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create git blobstore: %w", err)
	}

	// Create the NBS store backed by the git blobstore
	q := nbs.NewUnlimitedMemQuotaProvider()
	gitStore, err := nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)
	if err != nil {
		bs.Close()
		return nil, nil, nil, fmt.Errorf("failed to create git store: %w", err)
	}

	vrw := types.NewValueStore(gitStore)
	ns := tree.NewNodeStore(gitStore)
	db := datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, nil
}

// gitURLFromURLObj converts a url.URL to a git repository URL string.
// For git:// scheme, it reconstructs the URL.
// For http(s):// with .git suffix, it preserves the original URL.
func gitURLFromURLObj(urlObj *url.URL) string {
	// Reconstruct the URL
	if urlObj.Scheme == GitScheme {
		// If there's no host, this is a local file path
		if urlObj.Host == "" {
			return urlObj.Path
		}
		// Convert git:// to https:// for actual git operations
		// git://github.com/user/repo.git -> https://github.com/user/repo.git
		return fmt.Sprintf("https://%s%s", urlObj.Host, urlObj.Path)
	}

	// For http/https URLs, return as-is
	return urlObj.String()
}

// gitRefFromParams extracts the git ref from params, or returns the default.
func gitRefFromParams(params map[string]interface{}) string {
	if params != nil {
		if refVal, ok := params[GitRefParam]; ok {
			if ref, ok := refVal.(string); ok && ref != "" {
				return ref
			}
		}
	}
	return gitremote.DefaultRef
}

// gitLocalPathFromParams extracts the local cache path from params.
func gitLocalPathFromParams(params map[string]interface{}) string {
	if params != nil {
		if pathVal, ok := params[GitLocalPathParam]; ok {
			if path, ok := pathVal.(string); ok && path != "" {
				return path
			}
		}
	}
	return ""
}

// IsGitURL returns true if the URL should be handled by the GitFactory.
// This includes:
// - URLs with the git:// scheme
// - HTTP(S) URLs ending with .git
// - Local file paths ending with .git
func IsGitURL(urlStr string) bool {
	// Check for git:// scheme
	if strings.HasPrefix(strings.ToLower(urlStr), "git://") {
		return true
	}

	lower := strings.ToLower(urlStr)

	// Check for .git suffix on http(s) URLs
	if (strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")) &&
		strings.HasSuffix(lower, ".git") {
		return true
	}

	// Check for local file paths ending with .git (bare repositories)
	if strings.HasSuffix(lower, ".git") {
		return true
	}

	return false
}

// GitCacheDir returns the default cache directory for git remotes.
func GitCacheDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	cacheDir := filepath.Join(homeDir, ".dolt", "git-remotes")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", err
	}
	return cacheDir, nil
}
