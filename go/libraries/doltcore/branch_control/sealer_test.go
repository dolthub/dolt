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

package branch_control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// reversingSealer stores files reversed.
type reversingSealer struct{}

func reversed(b []byte) []byte {
	r := slices.Clone(b)
	slices.Reverse(r)
	return r
}

func (reversingSealer) Seal(_ context.Context, contents []byte) ([]byte, error) {
	return reversed(contents), nil
}

func (reversingSealer) Open(_ context.Context, _ string, data []byte, _ os.FileMode) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("no sealed data")
	}
	return reversed(data), nil
}

func TestBranchControlFileSealer(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "branch_control.db")

	controller, err := LoadData(ctx, path, dir, reversingSealer{})
	require.NoError(t, err, "a missing file is not opened")
	require.NoError(t, controller.SaveData(ctx, filesys.LocalFS))
	saved := *controller.Serialized.Load()
	stored, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, reversed(saved), stored)

	reloaded, err := LoadData(ctx, path, dir, reversingSealer{})
	require.NoError(t, err)
	assert.Equal(t, saved, *reloaded.Serialized.Load())

	_, err = LoadData(ctx, path, dir)
	assert.Error(t, err, "without a sealer the sealed file does not load")
}
