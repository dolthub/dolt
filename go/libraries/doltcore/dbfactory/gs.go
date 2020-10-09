// Copyright 2019 Liquidata, Inc.
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
	"github.com/dolthub/dolt/go/store/blobstore"
	"net/url"

	"cloud.google.com/go/storage"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// GSFactory is a DBFactory implementation for creating GCS backed databases
type GSFactory struct {
}

// CreateDB creates an GCS backed database
func (fact GSFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	var db datas.Database
	gcs, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	bucket := gcs.Bucket(urlObj.Host)
	bs := blobstore.NewGCSBlobstore(bucket, urlObj.Path)
	gcsStore, err := nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize)

	if err != nil {
		return nil, err
	}

	db = datas.NewDatabase(gcsStore)

	return db, err
}

// MemBS is a DBFactory implementation for creating in memory blobstore backed databases for testing
type MemBSFactory struct {
}

// CreateDB creates an in memory blobstore backed database
func (fact MemBSFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	var db datas.Database
	bs := blobstore.NewInMemoryBlobstore()
	bsStore, err := nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize)

	if err != nil {
		return nil, err
	}

	db = datas.NewDatabase(bsStore)

	return db, err
}
