// Copyright 2019 Dolthub, Inc.
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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

func TestCreateMemDB(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDB(ctx, types.Format_Default, "mem://", nil)

	assert.NoError(t, err)
	assert.NotNil(t, db)
}

func TestCreateFileDBIsManifestVersionGetter(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDB(ctx, types.Format_Default, "file://testdata/.dolt/noms", nil)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.True(t, datas.IsChunkStoreManifestVersionGetter(db))
}
