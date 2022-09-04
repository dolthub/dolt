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
	// todo get endpoint accesskeyid and secret from env or params
	ossClient, err := oss.New(
		"endpoint",
		"accesskey",
		"secret",
	)
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
