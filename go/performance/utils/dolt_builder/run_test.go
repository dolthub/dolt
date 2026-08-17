package dolt_builder

import (
	"testing"
)

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
