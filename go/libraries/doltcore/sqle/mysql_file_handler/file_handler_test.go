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

package mysql_file_handler

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	return reversed(data), nil
}

func TestPersisterSealsPrivileges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "privileges.db")
	data := []byte("privileges")

	p := NewPersister(path, dir, reversingSealer{})
	require.NoError(t, p.Persist(sql.NewEmptyContext(), data))
	stored, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, reversed(data), stored)
	loaded, err := p.LoadData(context.Background())
	require.NoError(t, err)
	assert.Equal(t, data, loaded)

	plain := NewPersister(path, dir)
	loaded, err = plain.LoadData(context.Background())
	require.NoError(t, err)
	assert.Equal(t, reversed(data), loaded, "without a sealer the file is read as it is")
}
