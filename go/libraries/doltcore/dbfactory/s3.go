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
	"net/url"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dolthub/dolt/go/libraries/doltcore/memlimit"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// S3EndpointParam is a creation parameter that overrides the S3 endpoint.
	// Use it to target S3-compatible stores such as Cloudflare R2
	// (https://<account>.r2.cloudflarestorage.com) or MinIO. When absent, the
	// AWS SDK's standard resolution applies, including the AWS_ENDPOINT_URL_S3
	// environment variable.
	S3EndpointParam = "s3-endpoint"

	// S3RegionParam is a creation parameter that overrides the signing region.
	// Some S3-compatible providers accept any value here (R2 uses "auto").
	S3RegionParam = "s3-region"

	// S3PathStyleParam is a creation parameter ("true"/"false") that forces
	// path-style addressing (https://endpoint/bucket/key) instead of
	// virtual-hosted style. Most non-AWS providers require or prefer it.
	S3PathStyleParam = "s3-path-style"
)

// S3Factory is a DBFactory implementation for databases backed by generic
// S3-compatible object stores (AWS S3, Cloudflare R2, MinIO, ...) that
// support conditional writes (If-Match / If-None-Match on PutObject).
//
// Unlike the aws:// scheme, which pairs an S3 bucket with a DynamoDB table
// for the manifest, s3:// stores the manifest as an ordinary object and
// relies on conditional PutObject for the atomic check-and-set. Credentials
// come from the standard AWS SDK chain (environment variables, shared
// config/credentials files, SSO, IMDS).
//
// URL format: s3://bucket/path/to/db
type S3Factory struct {
}

// PrepareDB prepares an S3-compatible backed database
func (fact S3Factory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates a database backed by a generic S3-compatible object store
func (fact S3Factory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	bucket := urlObj.Hostname()
	if bucket == "" {
		return nil, nil, nil, errors.New("s3 url must be of the form s3://bucket/path")
	}
	prefix := urlObj.Path

	client, err := newS3Client(ctx, params)
	if err != nil {
		return nil, nil, nil, err
	}

	bs := blobstore.NewS3Blobstore(client, bucket, prefix)

	q := nbs.NewUnlimitedMemQuotaProvider()
	// The S3 blobstore has no server-side compose operation, so use the
	// no-conjoin store: table files are written whole and never concatenated
	// through the blobstore.
	s3Store, err := nbs.NewNoConjoinBSStore(ctx, nbf.VersionString(), bs, memlimit.MemtableSize(), q)
	if err != nil {
		return nil, nil, nil, err
	}

	vrw := types.NewValueStore(s3Store)
	ns := tree.NewNodeStore(s3Store)
	db := datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, nil
}

func newS3Client(ctx context.Context, params map[string]interface{}) (*s3.Client, error) {
	var loadOpts []func(*config.LoadOptions) error
	if region, ok := paramString(params, S3RegionParam); ok {
		loadOpts = append(loadOpts, config.WithRegion(region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint, ok := paramString(params, S3EndpointParam); ok {
			o.BaseEndpoint = aws.String(endpoint)
		}
		if ps, ok := paramString(params, S3PathStyleParam); ok {
			if b, err := strconv.ParseBool(ps); err == nil {
				o.UsePathStyle = b
			}
		}
	}), nil
}

func paramString(params map[string]interface{}, key string) (string, bool) {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s, true
		}
	}
	return "", false
}
