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
	"path/filepath"

	"cloud.google.com/go/storage"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// GSFactory is a DBFactory implementation for creating GCS backed databases
type GSFactory struct {
}

func (fact GSFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates an GCS backed database
func (fact GSFactory) GetDBLoader(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (DBLoader, error) {
	gcs, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	bs := blobstore.NewGCSBlobstore(gcs, urlObj.Host, urlObj.Path)
	q := nbs.NewUnlimitedMemQuotaProvider()
	gcsStore, err := nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)

	if err != nil {
		return nil, err
	}

	return ChunkStoreLoader{cs: gcsStore}, nil
}

// LocalBSFactory is a DBFactory implementation for creating a local filesystem blobstore backed databases for testing
type LocalBSFactory struct {
}

func (fact LocalBSFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates a local filesystem blobstore backed database
func (fact LocalBSFactory) GetDBLoader(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (DBLoader, error) {
	absPath, err := filepath.Abs(filepath.Join(urlObj.Host, urlObj.Path))

	if err != nil {
		return nil, err
	}

	bs := blobstore.NewLocalBlobstore(absPath)
	q := nbs.NewUnlimitedMemQuotaProvider()
	bsStore, err := nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)

	if err != nil {
		return nil, err
	}

	return ChunkStoreLoader{cs: bsStore}, err
}
