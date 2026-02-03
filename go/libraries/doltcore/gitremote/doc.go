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

// Package gitremote provides support for using git repositories as dolt remote backends.
// It enables clone, fetch, push, and pull operations to/from git remotes using a custom
// ref (refs/dolt/data) that doesn't interfere with normal git operations.
//
// # Authentication
//
// Dolt shells out to your installed `git` binary for transport. Authentication and
// credential discovery are therefore handled by Git itself (ssh-agent, ~/.ssh/config,
// git credential helper / OS keychain, ~/.netrc, etc.).
//
// # URL Schemes
//
// Git remotes can be specified using:
//   - git:// scheme: git://github.com/user/repo.git
//   - HTTPS with .git suffix: https://github.com/user/repo.git
//
// # Data Storage
//
// Dolt data is stored on a custom git ref (default: refs/dolt/data) under the
// .dolt_remote/ directory structure. This ref is not cloned or fetched by
// default git operations, keeping dolt data separate from normal git content.
package gitremote
