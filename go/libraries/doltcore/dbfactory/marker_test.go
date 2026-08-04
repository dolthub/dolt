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

package dbfactory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestInProgressMarkerRoundTrip(t *testing.T) {
	local, err := filesys.LocalFilesysWithWorkingDir(t.TempDir())
	require.NoError(t, err)

	cases := []struct {
		name string
		fs   filesys.Filesys
	}{
		{"local", local},
		{"inmem", filesys.EmptyInMemFS("/")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := tc.fs
			require.False(t, IsDatabaseInProgress(fs), "a fresh directory is not in progress")

			require.NoError(t, MarkDatabaseInProgress(fs))
			assert.True(t, IsDatabaseInProgress(fs), "directory is in progress once marked")

			// Marking a directory that is already marked is not an error.
			require.NoError(t, MarkDatabaseInProgress(fs))
			assert.True(t, IsDatabaseInProgress(fs))

			require.NoError(t, ClearDatabaseInProgress(fs))
			assert.False(t, IsDatabaseInProgress(fs), "directory is complete once the marker is cleared")

			require.NoError(t, ClearDatabaseInProgress(fs))
		})
	}
}
