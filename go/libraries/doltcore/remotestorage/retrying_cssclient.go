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

package remotestorage

import (
	"context"

	"github.com/cenkalti/backoff"
	"google.golang.org/grpc"

	remotesapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
)

const (
	csClientRetries = 5
)

var csRetryParams = backoff.NewExponentialBackOff()

type RetryingChunkStoreServiceClient struct {
	client remotesapi.ChunkStoreServiceClient
}

func (c RetryingChunkStoreServiceClient) HasChunks(ctx context.Context, in *remotesapi.HasChunksRequest, opts ...grpc.CallOption) (*remotesapi.HasChunksResponse, error) {
	var resp *remotesapi.HasChunksResponse
	op := func() error {
		var err error
		resp, err = c.client.HasChunks(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) GetDownloadLocations(ctx context.Context, in *remotesapi.GetDownloadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetDownloadLocsResponse, error) {
	var resp *remotesapi.GetDownloadLocsResponse
	op := func() error {
		var err error
		resp, err = c.client.GetDownloadLocations(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) GetUploadLocations(ctx context.Context, in *remotesapi.GetUploadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetUploadLocsResponse, error) {
	var resp *remotesapi.GetUploadLocsResponse
	op := func() error {
		var err error
		resp, err = c.client.GetUploadLocations(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) GetRepoMetadata(ctx context.Context, in *remotesapi.GetRepoMetadataRequest, opts ...grpc.CallOption) (*remotesapi.GetRepoMetadataResponse, error) {
	var resp *remotesapi.GetRepoMetadataResponse
	op := func() error {
		var err error
		resp, err = c.client.GetRepoMetadata(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) Rebase(ctx context.Context, in *remotesapi.RebaseRequest, opts ...grpc.CallOption) (*remotesapi.RebaseResponse, error) {
	var resp *remotesapi.RebaseResponse
	op := func() error {
		var err error
		resp, err = c.client.Rebase(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) Root(ctx context.Context, in *remotesapi.RootRequest, opts ...grpc.CallOption) (*remotesapi.RootResponse, error) {
	var resp *remotesapi.RootResponse
	op := func() error {
		var err error
		resp, err = c.client.Root(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) Commit(ctx context.Context, in *remotesapi.CommitRequest, opts ...grpc.CallOption) (*remotesapi.CommitResponse, error) {
	var resp *remotesapi.CommitResponse
	op := func() error {
		var err error
		resp, err = c.client.Commit(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) ListTableFiles(ctx context.Context, in *remotesapi.ListTableFilesRequest, opts ...grpc.CallOption) (*remotesapi.ListTableFilesResponse, error) {
	var resp *remotesapi.ListTableFilesResponse
	op := func() error {
		var err error
		resp, err = c.client.ListTableFiles(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}

func (c RetryingChunkStoreServiceClient) AddTableFiles(ctx context.Context, in *remotesapi.AddTableFilesRequest, opts ...grpc.CallOption) (*remotesapi.AddTableFilesResponse, error) {
	var resp *remotesapi.AddTableFilesResponse
	op := func() error {
		var err error
		resp, err = c.client.AddTableFiles(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, backoff.WithMaxRetries(csRetryParams, csClientRetries))

	return resp, err
}
