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
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dolthub/dolt/go/libraries/doltcore/memlimit"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
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
// Provider routing travels in the url's query string rather than in creation
// params, so a repository can address two providers at once and so the values
// survive wherever the url does. Credentials are never accepted there.
//
// URL format: s3://bucket/path/to/db[?endpoint=...&region=...&path-style=true]
type S3Factory struct {
}

const (
	s3EndpointQuery  = "endpoint"
	s3RegionQuery    = "region"
	s3PathStyleQuery = "path-style"
)

// s3Routing is the provider addressing an s3:// url may carry. None of it is
// authentication: credentials always come from the AWS SDK chain. endpoint and
// region fall back to the SDK's own resolution when empty; pathStyle has no
// ambient equivalent, so the url is the only way to ask for it.
type s3Routing struct {
	endpoint  string
	region    string
	pathStyle bool
}

// PrepareDB validates the url so a malformed one is reported before any
// request is attempted.
func (fact S3Factory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	_, _, _, err := parseS3Url(urlObj)
	return err
}

// ValidateS3Url reports whether |urlStr| is a usable s3:// url. Commands that
// take a url from the user should call this so a mistake surfaces when the
// remote is added rather than at first push.
func ValidateS3Url(urlStr string) error {
	urlObj, err := earl.Parse(urlStr)
	if err != nil {
		return err
	}

	_, _, _, err = parseS3Url(urlObj)
	return err
}

func parseS3Url(urlObj *url.URL) (bucket, prefix string, routing s3Routing, err error) {
	bucket = urlObj.Hostname()
	if bucket == "" {
		return "", "", routing, errors.New("s3 url must be of the form s3://bucket/path")
	}

	if urlObj.User != nil {
		return "", "", routing, errors.New("s3 urls must not embed credentials: they would be stored in plaintext with the remote. Use the standard AWS credential chain instead")
	}

	for key, vals := range urlObj.Query() {
		if len(vals) != 1 || vals[0] == "" {
			return "", "", routing, fmt.Errorf("s3 url parameter %q needs exactly one non-empty value", key)
		}

		switch key {
		case s3EndpointQuery:
			routing.endpoint = vals[0]
		case s3RegionQuery:
			routing.region = vals[0]
		case s3PathStyleQuery:
			routing.pathStyle, err = strconv.ParseBool(vals[0])
			if err != nil {
				return "", "", routing, fmt.Errorf("s3 url parameter %q must be true or false, got %q", key, vals[0])
			}
		default:
			return "", "", routing, fmt.Errorf("unknown s3 url parameter %q; supported parameters are %q, %q and %q",
				key, s3EndpointQuery, s3RegionQuery, s3PathStyleQuery)
		}
	}

	return bucket, urlObj.Path, routing, nil
}

// CreateDB creates a database backed by a generic S3-compatible object store
func (fact S3Factory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	bucket, prefix, routing, err := parseS3Url(urlObj)
	if err != nil {
		return nil, nil, nil, err
	}

	client, err := newS3Client(ctx, routing)
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

func newS3Client(ctx context.Context, routing s3Routing) (*s3.Client, error) {
	var loadOpts []func(*config.LoadOptions) error
	if routing.region != "" {
		loadOpts = append(loadOpts, config.WithRegion(routing.region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if routing.endpoint != "" {
			o.BaseEndpoint = aws.String(routing.endpoint)
		}
		o.UsePathStyle = routing.pathStyle
	}), nil
}
