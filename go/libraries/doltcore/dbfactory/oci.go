// Copyright 2023 Dolthub, Inc.
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

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// OCIFactory is a DBFactory implementation for creating OCI backed databases
type OCIFactory struct {
}

func (fact OCIFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates an OCI backed database
func (fact OCIFactory) GetDBLoader(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (DBLoader, error) {
	provider := common.DefaultConfigProvider()

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, err
	}

	bs, err := blobstore.NewOCIBlobstore(ctx, provider, client, urlObj.Host, urlObj.Path)
	if err != nil {
		return nil, err
	}

	q := nbs.NewUnlimitedMemQuotaProvider()

	ociStore, err := nbs.NewNoConjoinBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)
	if err != nil {
		return nil, err
	}

	return ChunkStoreLoader{cs: ociStore}, nil
}
