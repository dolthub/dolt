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

package blobstore

import (
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

// interfaces to abstract Azure Blob Storage operations for better testability

// azureBlobProperties abstracts blob properties
type azureBlobProperties interface {
	GetETag() *azcore.ETag
	GetContentLength() *int64
}

// realAzBlobProperties wraps blob.GetPropertiesResponse
type realAzBlobProperties struct {
	resp blob.GetPropertiesResponse
}

func (p *realAzBlobProperties) GetETag() *azcore.ETag {
	return p.resp.ETag
}

func (p *realAzBlobProperties) GetContentLength() *int64 {
	return p.resp.ContentLength
}

// azureBlobClient abstracts Azure Blob Storage operations for testability
type azureBlobClient interface {
	GetProperties(ctx context.Context, containerName, blobName string) (azureBlobProperties, error)
	DownloadStream(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error)
	UploadBuffer(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error)
	StageBlock(ctx context.Context, containerName, blobName, blockID string, body io.ReadSeekCloser) error
	StageBlockFromURL(ctx context.Context, containerName, blobName, blockID, sourceURL string) error
	CommitBlockList(ctx context.Context, containerName, blobName string, blockIDs []string) (azureUploadResponse, error)
	GetBlobURL(containerName, blobName string) string
}

// realAzClient wraps the actual Azure SDK client
type realAzClient struct {
	client *azblob.Client
}

func newRealAzClient(client *azblob.Client) *realAzClient {
	return &realAzClient{client: client}
}

func (c *realAzClient) GetProperties(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
	blobClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)
	resp, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &realAzBlobProperties{resp: resp}, nil
}

func (c *realAzClient) DownloadStream(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
	blobClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)
	resp, err := blobClient.DownloadStream(ctx, options)
	if err != nil {
		return nil, err
	}
	return &realAzDownloadResponse{resp: resp}, nil
}

func (c *realAzClient) UploadBuffer(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
	resp, err := c.client.UploadBuffer(ctx, containerName, blobName, data, options)
	if err != nil {
		return nil, err
	}
	return &realAzUploadResponse{etag: resp.ETag}, nil
}

func (c *realAzClient) StageBlock(ctx context.Context, containerName, blobName, blockID string, body io.ReadSeekCloser) error {
	blockBlobClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(blobName)
	_, err := blockBlobClient.StageBlock(ctx, blockID, body, &blockblob.StageBlockOptions{})
	return err
}

func (c *realAzClient) CommitBlockList(ctx context.Context, containerName, blobName string, blockIDs []string) (azureUploadResponse, error) {
	blockBlobClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(blobName)
	resp, err := blockBlobClient.CommitBlockList(ctx, blockIDs, nil)
	if err != nil {
		return nil, err
	}
	return &realAzUploadResponse{etag: resp.ETag}, nil
}

func (c *realAzClient) StageBlockFromURL(ctx context.Context, containerName, blobName, blockID, sourceURL string) error {
	blockBlobClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(blobName)
	_, err := blockBlobClient.StageBlockFromURL(ctx, blockID, sourceURL, &blockblob.StageBlockFromURLOptions{})
	return err
}

func (c *realAzClient) GetBlobURL(containerName, blobName string) string {
	blobClient := c.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)
	return blobClient.URL()
}

// azureDownloadResponse abstracts download response
type azureDownloadResponse interface {
	GetBody() io.ReadCloser
	GetETag() *azcore.ETag
	GetContentLength() *int64
	GetContentRange() *string
}

// realAzDownloadResponse wraps blob.DownloadStreamResponse
type realAzDownloadResponse struct {
	resp blob.DownloadStreamResponse
}

func (r *realAzDownloadResponse) GetBody() io.ReadCloser {
	return r.resp.Body
}

func (r *realAzDownloadResponse) GetETag() *azcore.ETag {
	return r.resp.ETag
}

func (r *realAzDownloadResponse) GetContentLength() *int64 {
	return r.resp.ContentLength
}

func (r *realAzDownloadResponse) GetContentRange() *string {
	return r.resp.ContentRange
}

// azureUploadResponse abstracts upload response
type azureUploadResponse interface {
	GetETag() *azcore.ETag
}

// realAzUploadResponse wraps upload response
type realAzUploadResponse struct {
	etag *azcore.ETag
}

func (r *realAzUploadResponse) GetETag() *azcore.ETag {
	return r.etag
}
