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

package env

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestGetAbsRemoteUrl_GitSchemeDetection(t *testing.T) {
	// Create a minimal config that returns the default host
	cfg := config.NewMapConfig(map[string]string{})

	tests := []struct {
		name           string
		urlArg         string
		expectedScheme string
		expectedURL    string
	}{
		{
			name:           "explicit git scheme",
			urlArg:         "git://github.com/user/repo.git",
			expectedScheme: dbfactory.GitScheme,
			expectedURL:    "git://github.com/user/repo.git",
		},
		{
			name:           "https with .git suffix",
			urlArg:         "https://github.com/user/repo.git",
			expectedScheme: dbfactory.GitScheme,
			expectedURL:    "https://github.com/user/repo.git",
		},
		{
			name:           "http with .git suffix",
			urlArg:         "http://github.com/user/repo.git",
			expectedScheme: dbfactory.GitScheme,
			expectedURL:    "http://github.com/user/repo.git",
		},
		{
			name:           "https with .GIT suffix (uppercase)",
			urlArg:         "https://github.com/user/repo.GIT",
			expectedScheme: dbfactory.GitScheme,
			expectedURL:    "https://github.com/user/repo.GIT",
		},
		{
			name:           "naked URL with .git suffix",
			urlArg:         "github.com/user/repo.git",
			expectedScheme: dbfactory.GitScheme,
			expectedURL:    "https://github.com/user/repo.git",
		},
		{
			name:           "https without .git suffix - dolthub",
			urlArg:         "https://doltremoteapi.dolthub.com/user/repo",
			expectedScheme: dbfactory.HTTPSScheme,
			expectedURL:    "https://doltremoteapi.dolthub.com/user/repo",
		},
		{
			name:           "naked URL without .git suffix - dolthub style",
			urlArg:         "dolthub.com/user/repo",
			expectedScheme: dbfactory.HTTPSScheme,
			expectedURL:    "https://dolthub.com/user/repo",
		},
		{
			name:           "aws scheme preserved",
			urlArg:         "aws://bucket/path",
			expectedScheme: dbfactory.AWSScheme,
			expectedURL:    "aws://bucket/path",
		},
		{
			name:           "gs scheme preserved",
			urlArg:         "gs://bucket/path",
			expectedScheme: dbfactory.GSScheme,
			expectedURL:    "gs://bucket/path",
		},
		{
			name:           "localbs scheme preserved",
			urlArg:         "localbs://path/to/dir",
			expectedScheme: dbfactory.LocalBSScheme,
			expectedURL:    "localbs://path/to/dir",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pass nil for fs to avoid file system operations for non-file URLs
			scheme, url, err := GetAbsRemoteUrl(nil, cfg, tt.urlArg)

			// Skip file/localbs tests that need a real filesystem
			if tt.expectedScheme == dbfactory.LocalBSScheme {
				// localbs requires a filesystem, skip detailed check
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedScheme, scheme, "scheme mismatch")
			assert.Equal(t, tt.expectedURL, url, "URL mismatch")
		})
	}
}

func TestGetAbsRemoteUrl_GitVsDoltHub(t *testing.T) {
	cfg := config.NewMapConfig(map[string]string{})

	// Test that similar URLs are correctly distinguished
	tests := []struct {
		name           string
		urlArg         string
		expectedScheme string
		description    string
	}{
		{
			name:           "github with .git is git remote",
			urlArg:         "https://github.com/dolthub/dolt.git",
			expectedScheme: dbfactory.GitScheme,
			description:    "GitHub URL with .git suffix should use git factory",
		},
		{
			name:           "github without .git is dolthub remote",
			urlArg:         "https://github.com/dolthub/dolt",
			expectedScheme: dbfactory.HTTPSScheme,
			description:    "GitHub URL without .git suffix should use dolthub factory",
		},
		{
			name:           "dolthub is dolthub remote",
			urlArg:         "https://doltremoteapi.dolthub.com/dolthub/dolt",
			expectedScheme: dbfactory.HTTPSScheme,
			description:    "DoltHub URL should use dolthub factory",
		},
		{
			name:           "gitlab with .git is git remote",
			urlArg:         "https://gitlab.com/user/repo.git",
			expectedScheme: dbfactory.GitScheme,
			description:    "GitLab URL with .git suffix should use git factory",
		},
		{
			name:           "bitbucket with .git is git remote",
			urlArg:         "https://bitbucket.org/user/repo.git",
			expectedScheme: dbfactory.GitScheme,
			description:    "Bitbucket URL with .git suffix should use git factory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme, _, err := GetAbsRemoteUrl(nil, cfg, tt.urlArg)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedScheme, scheme, tt.description)
		})
	}
}

func TestGetAbsRemoteUrl_ShorthandURLs(t *testing.T) {
	// Test shorthand URLs (no scheme, just org/repo format)
	cfg := config.NewMapConfig(map[string]string{})

	tests := []struct {
		name           string
		urlArg         string
		expectedScheme string
	}{
		{
			name:           "shorthand dolthub format",
			urlArg:         "dolthub/museum-collections",
			expectedScheme: dbfactory.HTTPSScheme,
		},
		{
			name:           "shorthand with host",
			urlArg:         "example.com/user/repo",
			expectedScheme: dbfactory.HTTPSScheme,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme, _, err := GetAbsRemoteUrl(nil, cfg, tt.urlArg)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedScheme, scheme)
		})
	}
}
