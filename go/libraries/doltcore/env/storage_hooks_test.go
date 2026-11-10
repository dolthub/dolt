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

package env

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageLoadParamsIsReached(t *testing.T) {
	refused := errors.New("refused")
	saved := StorageLoadParams
	t.Cleanup(func() { StorageLoadParams = saved })
	var got *DoltEnv
	StorageLoadParams = func(dEnv *DoltEnv, params map[string]interface{}) (map[string]interface{}, error) {
		got = dEnv
		return nil, refused
	}

	dEnv, _ := createTestEnv(true, true)
	LoadDoltDB(context.Background(), dEnv)
	require.Same(t, dEnv, got)
	require.ErrorIs(t, dEnv.DBLoadError, refused)
}
