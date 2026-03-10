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

package blobstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPathReferencedByManifest(t *testing.T) {
	allowed := map[string]bool{
		"abc123": true,
		"def456": true,
	}

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"manifest always allowed", "manifest", true},
		{"exact match", "abc123", true},
		{"exact match 2", "def456", true},
		{"not in set", "zzz999", false},
		{"chunked part", "abc123/0001", true},
		{"chunked part deep", "abc123/0042", true},
		{"unknown chunked", "zzz999/0001", false},
		{"records suffix", "abc123.records", true},
		{"tail suffix", "abc123.tail", true},
		{"darc suffix", "abc123.darc", true},
		{"darc.records suffix", "abc123.darc.records", true},
		{"darc.tail suffix", "abc123.darc.tail", true},
		{"unknown with records", "zzz999.records", false},
		{"unknown with tail", "zzz999.tail", false},
		{"empty path", "", false},
		{"empty allowed set", "abc123", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isPathReferencedByManifest(tt.path, allowed)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPathReferencedByManifestEmptySet(t *testing.T) {
	empty := map[string]bool{}
	assert.True(t, isPathReferencedByManifest("manifest", empty))
	assert.False(t, isPathReferencedByManifest("anything", empty))
}
