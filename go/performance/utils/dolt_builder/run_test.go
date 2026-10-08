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

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunWithBuildTags(t *testing.T) {
	// Build a tiny local repository to verify source selection and the complete
	// public API without downloading or compiling Dolt.
	repo := t.TempDir()
	files := map[string]string{
		"go/go.mod":              "module buildertest\n\ngo 1.20\n",
		"go/cmd/dolt/main.go":    "package main\nimport \"fmt\"\nfunc main() { fmt.Println(selected) }\n",
		"go/cmd/dolt/default.go": "//go:build !feature_one\n\npackage main\nconst selected = \"default\"\n",
		"go/cmd/dolt/one.go":     "//go:build feature_one && !feature_two\n\npackage main\nconst selected = \"one\"\n",
		"go/cmd/dolt/two.go":     "//go:build feature_one && feature_two\n\npackage main\nconst selected = \"two\"\n",
	}
	for name, content := range files {
		path := filepath.Join(repo, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"init"},
		{"add", "."},
		{"-c", "user.name=Builder Test", "-c", "user.email=builder@example.com", "-c", "commit.gpgsign=false", "commit", "-m", "test fixture"},
		{"tag", "v1.0.0"},
		{"tag", "v2.0.0"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	t.Setenv(envDoltRepositoryURL, repo)
	t.Setenv(envRepositoryAccessToken, "")
	// The extracted fixture has no .git directory for VCS stamping.
	t.Setenv("GOFLAGS", "-buildvcs=false")
	t.Setenv("GOWORK", "off")
	for _, tt := range []struct {
		name    string
		tags    []string
		profile string
		want    string
	}{
		{name: "existing API", want: "default"},
		{name: "no tags", want: "default"},
		{name: "one tag", tags: []string{"feature_one"}, want: "one"},
		{name: "multiple tags", tags: []string{"feature_one", "feature_two"}, want: "two"},
		{name: "tags with PGO option", tags: []string{"feature_one", "feature_two"}, profile: "off", want: "two"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			work := t.TempDir()
			t.Chdir(work)
			binDir := filepath.Join(work, "bin")
			t.Setenv(envDoltBin, binDir)
			revisions := []string{"v1.0.0", "v2.0.0"}
			if tt.profile != "" {
				revisions = revisions[:1]
			}
			var err error
			if tt.name == "existing API" {
				// Preserve compatibility with consumers that pass Run as a callback.
				var run func(context.Context, []string, string) error = Run
				err = run(context.Background(), revisions, tt.profile)
			} else {
				err = RunWithBuildTags(context.Background(), revisions, tt.profile, tt.tags)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, revision := range revisions {
				name := "dolt"
				if runtime.GOOS == "windows" {
					name += ".exe"
				}
				out, err := exec.Command(filepath.Join(binDir, revision, name), "version").CombinedOutput()
				if err != nil {
					t.Fatalf("run %s: %v\n%s", revision, err, out)
				}
				if got := strings.TrimSpace(string(out)); got != tt.want {
					t.Fatalf("%s: got %q, want %q", revision, got, tt.want)
				}
			}
		})
	}
}

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
