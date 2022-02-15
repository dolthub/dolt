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

	"github.com/dolthub/dolt/go/store/types"
)

/*
func TestCreateFileDB(t *testing.T) {
	ctx := context.Background()

	db, err := CreateDB(ctx, "file://testdata/.dolt/noms", nil)

	assert.NoError(t, err)

	datasets := db.Datasets(ctx)

	assert.Equal(t, int(datasets.Len()), 2)

	master, masterOK := datasets.MaybeGet(ctx, types.String("refs/heads/master"))
	assert.True(t, masterOK)

	masterVal := master.(types.Ref).TargetValue(ctx, db)
	assert.NotNil(t, masterVal)

	create, createOK := datasets.MaybeGet(ctx, types.String("refs/internal/create"))
	assert.True(t, createOK)

	createVal := create.(types.Ref).TargetValue(ctx, db)
	assert.NotNil(t, createVal)

	_, fakeOK := datasets.MaybeGet(ctx, types.String("refs/heads/fake"))
	assert.False(t, fakeOK)
}
*/

func TestCreateMemDB(t *testing.T) {
	ctx := context.Background()
	db, vrw, err := CreateDB(ctx, types.Format_Default, "mem://", nil)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, vrw)
}
