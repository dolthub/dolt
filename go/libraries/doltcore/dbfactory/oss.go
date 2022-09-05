package dbfactory

import (
	"context"
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"net/url"
	"os"
)

const (
	ossEndpointEnvKey        = "OSS_ENDPOINT"
	ossAccessKeyIDEnvKey     = "OSS_ACCESS_KEY_ID"
	ossAccessKeySecretEnvKey = "OSS_ACCESS_KEY_SECRET"
)

// OSSFactory is a DBFactory implementation for creating GCS backed databases
type OSSFactory struct {
}

// CreateDB creates an GCS backed database
func (fact OSSFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	ossStore, err := fact.newChunkStore(ctx, nbf, urlObj, params)
	if err != nil {
		return nil, nil, nil, err
	}

	vrw := types.NewValueStore(ossStore)
	ns := tree.NewNodeStore(ossStore)
	db := datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, nil
}

func (fact OSSFactory) newChunkStore(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (chunks.ChunkStore, error) {
	// oss://[bucket]/[key]
	bucket := urlObj.Hostname()
	prefix := urlObj.Path
	ossClient, err := getOSSClient()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize oss err: %s", err)
	}
	bs, err := blobstore.NewOSSBlobstore(ossClient, bucket, prefix)
	if err != nil {
		return nil, errors.New("failed to initialize oss blob store")
	}
	q := nbs.NewUnlimitedMemQuotaProvider()
	return nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)
}

func getOSSClient() (*oss.Client, error) {
	var endpoint, accessKeyID, accessKeySecret string
	if endpoint = os.Getenv(ossEndpointEnvKey); endpoint == "" {
		return nil, fmt.Errorf("failed to find endpoint from env %s", ossEndpointEnvKey)
	}
	if accessKeyID = os.Getenv(ossAccessKeyIDEnvKey); accessKeyID == "" {
		return nil, fmt.Errorf("failed to find accessKeyID from env %s", ossAccessKeyIDEnvKey)
	}
	if accessKeySecret = os.Getenv(ossAccessKeySecretEnvKey); accessKeySecret == "" {
		return nil, fmt.Errorf("failed to find accessKeySecret from env %s", ossAccessKeySecretEnvKey)
	}
	return oss.New(
		endpoint,
		accessKeyID,
		accessKeySecret,
	)
}
