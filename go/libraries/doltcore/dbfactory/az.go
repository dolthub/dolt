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

package dbfactory

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// AzureDBFactory is a DBFactory implementation for creating Azure Blob Storage backed databases
type AzureDBFactory struct {
}

func (fact AzureDBFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates an Azure Blob Storage backed database
// URL format: az://STORAGE_ACCOUNT.blob.core.windows.net/container_name/path
func (fact AzureDBFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	var db datas.Database

	// Parse the container name from the path
	// urlObj.Host is STORAGE_ACCOUNT.blob.core.windows.net
	// urlObj.Path is /container_name/path
	pathParts := strings.SplitN(strings.TrimPrefix(urlObj.Path, "/"), "/", 2)
	if len(pathParts) == 0 || pathParts[0] == "" {
		return nil, nil, nil, errors.New("azure url must include container name in path")
	}

	containerName := pathParts[0]
	var blobPrefix string
	if len(pathParts) > 1 {
		blobPrefix = "/" + pathParts[1]
	}

	// Create Azure credential using default authentication
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create Azure client using the full service URL from the host
	serviceURL := fmt.Sprintf("https://%s/", urlObj.Host)
	azClient, err := azblob.NewClient(serviceURL, credential, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	bs := blobstore.NewAzureBlobstore(azClient, containerName, blobPrefix)
	q := nbs.NewUnlimitedMemQuotaProvider()
	azStore, err := nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)

	if err != nil {
		return nil, nil, nil, err
	}

	vrw := types.NewValueStore(azStore)
	ns := tree.NewNodeStore(azStore)
	db = datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, nil
}
