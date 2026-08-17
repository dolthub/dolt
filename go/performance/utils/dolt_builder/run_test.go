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

package dolt_builder

import "testing"

func TestRepositoryURLDefaultsToPublicDolt(t *testing.T) {
	t.Setenv(envDoltRepositoryURL, "")
	if got := repositoryURL(); got != GithubDolt {
		t.Fatalf("expected %q, got %q", GithubDolt, got)
	}
}

func TestRepositoryURLUsesEnvironmentOverride(t *testing.T) {
	const customURL = "https://github.com/example/dolt-fork.git"
	t.Setenv(envDoltRepositoryURL, customURL)
	if got := repositoryURL(); got != customURL {
		t.Fatalf("expected %q, got %q", customURL, got)
	}
}
