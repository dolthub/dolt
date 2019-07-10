package utils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindGitConfigDir(t *testing.T) {
	// Setup
	tmpDir, err := ioutil.TempDir("", "git-dolt-test")
	if err != nil {
		t.Errorf("Error creating temp directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Errorf("Error removing test directories: %v", err)
		}
	}()

	nestedExamplePath := filepath.Join(tmpDir, "__deeply", "nested", "example")
	topLevelExamplePath := filepath.Join(tmpDir, "__top", "level", "example")
	noGitExamplePath := filepath.Join(tmpDir, "__no", "git", "example")

	nestedGitPath := filepath.Join(nestedExamplePath, ".git")
	topLevelGitPath := filepath.Join(tmpDir, "__top", ".git")

	if err := os.MkdirAll(nestedGitPath, os.ModePerm); err != nil {
		t.Errorf("Error creating test directories: %v", err)
	}

	if err := os.MkdirAll(topLevelExamplePath, os.ModePerm); err != nil {
		t.Errorf("Error creating test directories: %v", err)
	}
	if err := os.MkdirAll(topLevelGitPath, os.ModePerm); err != nil {
		t.Errorf("Error creating test directories: %v", err)
	}

	if err := os.MkdirAll(noGitExamplePath, os.ModePerm); err != nil {
		t.Errorf("Error creating test directories: %v", err)
	}

	// Tests
	type args struct {
		startingPath string
		terminalPath string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"finds a deeply-nested git directory", args{nestedExamplePath, tmpDir}, nestedGitPath, false},
		{"finds a top-level git directory", args{topLevelExamplePath, tmpDir}, topLevelGitPath, false},
		{"returns an error when there is no git directory", args{noGitExamplePath, tmpDir}, "", true},
		{"returns an error (and does not hang) when startingPath is not a descendent of terminalPath", args{noGitExamplePath, nestedExamplePath}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FindGitConfigDir(tt.args.startingPath, tt.args.terminalPath)
			if tt.wantErr {
				assert.Error(t, err)
			} else if err != nil {
				t.Errorf("wanted %v, got error %v", tt.want, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
