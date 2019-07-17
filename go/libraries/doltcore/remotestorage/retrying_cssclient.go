package remotestorage

import (
	"context"

	"github.com/cenkalti/backoff"
	remotesapi "github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"google.golang.org/grpc"
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
