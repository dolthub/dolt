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

// Package gitauth keeps git and ssh subprocesses spawned by dolt from
// prompting for credentials. It builds environments with every interactive
// prompt disabled, detaches subprocesses from the controlling terminal, and
// rewrites the resulting authentication failures into actionable errors.
package gitauth

import (
	"maps"
	"slices"
	"strings"
)

// nonInteractiveOverrides lists the env entries that disable every interactive
// credential prompt reachable through git and ssh. See [SSH_ASKPASS_REQUIRE],
// [gitcredentials], and [Git Credential Manager].
//
// [SSH_ASKPASS_REQUIRE]: https://man.openbsd.org/ssh#SSH_ASKPASS_REQUIRE
// [gitcredentials]: https://git-scm.com/docs/gitcredentials
// [Git Credential Manager]: https://github.com/git-ecosystem/git-credential-manager
var nonInteractiveOverrides = map[string]string{
	"SSH_ASKPASS_REQUIRE": "never",
	"SSH_ASKPASS":         "",
	"GIT_ASKPASS":         "",
	"GIT_TERMINAL_PROMPT": "0",
	"GCM_INTERACTIVE":     "Never",
}

// NonInteractiveEnv returns a copy of |env| with every interactive credential
// prompt from git and ssh disabled. Entries in |env| whose keys conflict with
// the overrides are removed so the result is correct regardless of operating
// system environment deduplication semantics. Overrides are appended in
// sorted key order so the result is reproducible.
func NonInteractiveEnv(env []string) []string {
	out := make([]string, 0, len(env)+len(nonInteractiveOverrides))
	for _, e := range env {
		key, _, _ := strings.Cut(e, "=")
		if _, ok := nonInteractiveOverrides[key]; ok {
			continue
		}
		out = append(out, e)
	}
	for _, k := range slices.Sorted(maps.Keys(nonInteractiveOverrides)) {
		out = append(out, k+"="+nonInteractiveOverrides[k])
	}
	return out
}
