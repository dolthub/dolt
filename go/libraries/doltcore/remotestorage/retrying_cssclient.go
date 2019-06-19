package remotestorage

import (
	"context"
	"errors"
	"github.com/cenkalti/backoff"
	remotesapi "github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"google.golang.org/grpc"
)

var ErrRCSSCUnknown = errors.New("unknown error occurred")

var csRetryParams backoff.BackOff

func init() {
	csRetryParams = backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5)
}

type RetryingChunkStoreServiceClient struct {
	client remotesapi.ChunkStoreServiceClient
}

func (rcssc RetryingChunkStoreServiceClient) HasChunks(ctx context.Context, in *remotesapi.HasChunksRequest, opts ...grpc.CallOption) (*remotesapi.HasChunksResponse, error) {
	var resp *remotesapi.HasChunksResponse
	op := func() error {
		var err error
		resp, err = rcssc.client.HasChunks(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, csRetryParams)

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) GetDownloadLocations(ctx context.Context, in *remotesapi.GetDownloadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetDownloadLocsResponse, error) {
	var resp *remotesapi.GetDownloadLocsResponse
	op := func() error {
		var err error
		resp, err = rcssc.client.GetDownloadLocations(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, csRetryParams)

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) GetUploadLocations(ctx context.Context, in *remotesapi.GetUploadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetUploadLocsResponse, error) {
	var resp *remotesapi.GetUploadLocsResponse
	op := func() error {
		var err error
		resp, err = rcssc.client.GetUploadLocations(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, csRetryParams)

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) Rebase(ctx context.Context, in *remotesapi.RebaseRequest, opts ...grpc.CallOption) (*remotesapi.RebaseResponse, error) {
	var resp *remotesapi.RebaseResponse
	op := func() error {
		var err error
		resp, err = rcssc.client.Rebase(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, csRetryParams)

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) Root(ctx context.Context, in *remotesapi.RootRequest, opts ...grpc.CallOption) (*remotesapi.RootResponse, error) {
	var resp *remotesapi.RootResponse
	op := func() error {
		var err error
		resp, err = rcssc.client.Root(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, csRetryParams)

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) Commit(ctx context.Context, in *remotesapi.CommitRequest, opts ...grpc.CallOption) (*remotesapi.CommitResponse, error) {
	var resp *remotesapi.CommitResponse
	op := func() error {
		var err error
		resp, err = rcssc.client.Commit(ctx, in, opts...)
		return processGrpcErr(err)
	}

	err := backoff.Retry(op, csRetryParams)

	return resp, err
}
