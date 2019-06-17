package remotestorage

import (
	"context"
	remotesapi "github.com/liquidata-inc/ld/dolt/go/gen/proto/dolt/services/remotesapi_v1alpha1"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/retry"
	"google.golang.org/grpc"
	"time"
)

var csRetryParams = retry.RetryParams{
	NumRetries: 5,
	MaxDelay:   2 * time.Second,
	Backoff:    200 * time.Millisecond,
}

type RetryingChunkStoreServiceClient struct {
	client remotesapi.ChunkStoreServiceClient
}

func (rcssc RetryingChunkStoreServiceClient) HasChunks(ctx context.Context, in *remotesapi.HasChunksRequest, opts ...grpc.CallOption) (*remotesapi.HasChunksResponse, error) {
	var resp *remotesapi.HasChunksResponse
	var err error
	retry.CallWithRetries(csRetryParams, func() retry.RetriableCallState {
		resp, err = rcssc.client.HasChunks(ctx, in, opts...)
		return retry.ProcessGrpcErr(err)
	})

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) GetDownloadLocations(ctx context.Context, in *remotesapi.GetDownloadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetDownloadLocsResponse, error) {
	var resp *remotesapi.GetDownloadLocsResponse
	var err error
	retry.CallWithRetries(csRetryParams, func() retry.RetriableCallState {
		resp, err = rcssc.client.GetDownloadLocations(ctx, in, opts...)
		return retry.ProcessGrpcErr(err)
	})

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) GetUploadLocations(ctx context.Context, in *remotesapi.GetUploadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetUploadLocsResponse, error) {
	var resp *remotesapi.GetUploadLocsResponse
	var err error
	retry.CallWithRetries(csRetryParams, func() retry.RetriableCallState {
		resp, err = rcssc.client.GetUploadLocations(ctx, in, opts...)
		return retry.ProcessGrpcErr(err)
	})

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) Rebase(ctx context.Context, in *remotesapi.RebaseRequest, opts ...grpc.CallOption) (*remotesapi.RebaseResponse, error) {
	var resp *remotesapi.RebaseResponse
	var err error
	retry.CallWithRetries(csRetryParams, func() retry.RetriableCallState {
		resp, err = rcssc.client.Rebase(ctx, in, opts...)
		return retry.ProcessGrpcErr(err)
	})

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) Root(ctx context.Context, in *remotesapi.RootRequest, opts ...grpc.CallOption) (*remotesapi.RootResponse, error) {
	var resp *remotesapi.RootResponse
	var err error
	retry.CallWithRetries(csRetryParams, func() retry.RetriableCallState {
		resp, err = rcssc.client.Root(ctx, in, opts...)
		return retry.ProcessGrpcErr(err)
	})

	return resp, err
}

func (rcssc RetryingChunkStoreServiceClient) Commit(ctx context.Context, in *remotesapi.CommitRequest, opts ...grpc.CallOption) (*remotesapi.CommitResponse, error) {
	var resp *remotesapi.CommitResponse
	var err error
	retry.CallWithRetries(csRetryParams, func() retry.RetriableCallState {
		resp, err = rcssc.client.Commit(ctx, in, opts...)
		return retry.ProcessGrpcErr(err)
	})

	return resp, err
}
