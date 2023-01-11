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
	"net/url"

	"github.com/dolthub/dolt/go/store/chunks"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// MemFactory is a DBFactory implementation for creating in memory backed databases
type MemFactory struct {
}

func (fact MemFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates an in memory backed database
func (fact MemFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	var db datas.Database
	storage := &chunks.MemoryStorage{}
	cs := storage.NewViewWithFormat(nbf.VersionString())
	//bs := blobstore.NewInMemoryBlobstore(uuid.New().String())
	//q := nbs.NewUnlimitedMemQuotaProvider()
	//cs, err := nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)
	//if err != nil {
	//	return nil, nil, nil, err
	//}
	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db = datas.NewTypesDatabase(vrw, ns)
	return db, vrw, ns, nil
}
