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

package env

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

const defaultGitRemoteRef = "refs/dolt/data"

var supportedGitPlusSchemes = map[string]struct{}{
	"git+file":  {},
	"git+http":  {},
	"git+https": {},
	"git+ssh":   {},
}

var supportedUnderlyingGitSchemes = map[string]struct{}{
	"file":  {},
	"http":  {},
	"https": {},
	"ssh":   {},
}

// NormalizeGitRemoteUrl translates user-provided git remote strings into a canonical dbfactory URL
// using git+* schemes.
//
// It accepts:
// - Explicit dbfactory URLs: git+file/http/https/ssh://...
// - URLs ending in .git: file/http/https/ssh URLs
// - scp-style ssh: [user@]host:path/repo.git
// - schemeless host/path: host/org/repo.git (defaults to git+https)
// - local paths ending in .git (absolute or relative) (translated to git+file)
//
// It returns ok=false when the input is not recognized as a git remote URL (so callers can fall back
// to existing remote handling).
func NormalizeGitRemoteUrl(urlArg string) (normalized string, ok bool, err error) {
	urlArg = strings.TrimSpace(urlArg)
	if urlArg == "" {
		return "", false, fmt.Errorf("empty remote url")
	}

	// Fast-path: explicit git+* dbfactory URL.
	if strings.HasPrefix(strings.ToLower(urlArg), "git+") {
		u, err := url.Parse(urlArg)
		if err != nil {
			return "", false, err
		}
		if _, ok := supportedGitPlusSchemes[strings.ToLower(u.Scheme)]; !ok {
			return "", false, fmt.Errorf("unsupported git dbfactory scheme %q", u.Scheme)
		}
		ensureDefaultRefQuery(u)
		return u.String(), true, nil
	}

	// Only translate obvious git remote strings (must end in .git).
	base := stripQueryAndFragment(urlArg)
	if !strings.HasSuffix(base, ".git") {
		return "", false, nil
	}

	// scp-like ssh: [user@]host:path/repo.git (no scheme, no ://)
	if isScpLikeGitRemote(urlArg) {
		host, p := splitScpLike(urlArg)
		ssh := "git+ssh://" + host + "/" + strings.TrimPrefix(p, "/")
		u, err := url.Parse(ssh)
		if err != nil {
			return "", false, err
		}
		ensureDefaultRefQuery(u)
		return u.String(), true, nil
	}

	// file/http/https/ssh url with a scheme.
	if strings.Contains(urlArg, "://") {
		u, err := url.Parse(urlArg)
		if err != nil {
			return "", false, err
		}
		s := strings.ToLower(u.Scheme)
		if _, ok := supportedUnderlyingGitSchemes[s]; !ok {
			return "", false, nil
		}
		u.Scheme = "git+" + s
		ensureDefaultRefQuery(u)
		return u.String(), true, nil
	}

	// Local filesystem path (absolute or relative).
	if looksLikeLocalPath(urlArg) {
		abs, err := filepath.Abs(urlArg)
		if err != nil {
			return "", false, err
		}
		abs = filepath.ToSlash(abs)
		u, err := url.Parse("git+file://" + abs)
		if err != nil {
			return "", false, err
		}
		ensureDefaultRefQuery(u)
		return u.String(), true, nil
	}

	// Schemeless host/path.git defaults to https.
	u, err := url.Parse("git+https://" + urlArg)
	if err != nil {
		return "", false, err
	}
	ensureDefaultRefQuery(u)
	return u.String(), true, nil
}

func stripQueryAndFragment(s string) string {
	// Order matters: strip fragment then query.
	if i := strings.IndexByte(s, '#'); i >= 0 {
		s = s[:i]
	}
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[:i]
	}
	return s
}

func looksLikeLocalPath(s string) bool {
	return strings.HasPrefix(s, "/") || strings.HasPrefix(s, "./") || strings.HasPrefix(s, "../")
}

func isScpLikeGitRemote(s string) bool {
	// This intentionally keeps the matcher simple:
	// - no scheme (no "://")
	// - contains a single ':' separating host from path
	// - host part contains no '/'
	// - path ends in .git (already checked by caller)
	if strings.Contains(s, "://") {
		return false
	}
	colon := strings.IndexByte(s, ':')
	if colon < 0 {
		return false
	}
	host := s[:colon]
	path := s[colon+1:]
	if host == "" || path == "" {
		return false
	}
	if strings.Contains(host, "/") {
		return false
	}
	// Avoid misclassifying Windows paths; host must contain a dot or an '@' (git@host:...).
	if !strings.Contains(host, ".") && !strings.Contains(host, "@") {
		return false
	}
	return true
}

func splitScpLike(s string) (host string, path string) {
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return "", s
	}
	return s[:i], s[i+1:]
}

func ensureDefaultRefQuery(u *url.URL) {
	q := u.Query()
	if q.Get("ref") == "" {
		q.Set("ref", defaultGitRemoteRef)
		u.RawQuery = q.Encode()
	}
}
