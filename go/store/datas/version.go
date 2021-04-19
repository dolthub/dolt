// Copyright 2020 Dolthub, Inc.
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

package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/chunks"
)

func GetManifestStorageVersion(ctx context.Context, db Database) (string, error) {
	store, ok := db.chunkStore().(chunks.ChunkStoreVersionGetter)
	if !ok {
		return "", chunks.ErrUnsupportedOperation
	}
	return store.GetManifestStorageVersion(ctx)
}
