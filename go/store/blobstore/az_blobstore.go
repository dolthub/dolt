// Copyright 2025 Dolthub, Inc.
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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

// AzureBlobstore provides an Azure Blob Storage implementation of the Blobstore interface
type AzureBlobstore struct {
	client        *azblob.Client
	containerName string
	prefix        string
}

var _ Blobstore = &AzureBlobstore{}

// NewAzureBlobstore creates a new instance of an AzureBlobstore
func NewAzureBlobstore(client *azblob.Client, containerName, prefix string) *AzureBlobstore {
	// Remove leading slashes from prefix
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}

	return &AzureBlobstore{
		client:        client,
		containerName: containerName,
		prefix:        prefix,
	}
}

// Path returns this blobstore's path (i.e. container name + prefix)
func (bs *AzureBlobstore) Path() string {
	return path.Join(bs.containerName, bs.prefix)
}

// Exists returns true if a blob keyed by |key| exists
func (bs *AzureBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	absKey := path.Join(bs.prefix, key)
	blobClient := bs.client.ServiceClient().NewContainerClient(bs.containerName).NewBlobClient(absKey)

	_, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		// Check if blob doesn't exist (404 error)
		errMsg := err.Error()
		if strings.Contains(errMsg, "BlobNotFound") || strings.Contains(errMsg, "404") {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func errOrNotFound(err error, containerName, absKey string) error {
	// Check if blob doesn't exist
	errMsg := err.Error()
	if strings.Contains(errMsg, "BlobNotFound") || strings.Contains(errMsg, "404") {
		return NotFound{path.Join(containerName, absKey)}
	}

	return err
}

// Get retrieves an io.ReadCloser for the portion of a blob specified by br along with its version
func (bs *AzureBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	absKey := path.Join(bs.prefix, key)
	blobClient := bs.client.ServiceClient().NewContainerClient(bs.containerName).NewBlobClient(absKey)

	var downloadOptions *blob.DownloadStreamOptions
	if !br.isAllRange() {
		if br.offset >= 0 {
			downloadOptions = &blob.DownloadStreamOptions{
				Range: blob.HTTPRange{
					Offset: br.offset,
					Count:  br.length,
				},
			}
		} else {
			// Azure doesn't support negative offsets, so we need to get the blob size first to calculate the positive
			// offset from the end of the blob
			props, err := blobClient.GetProperties(ctx, nil)
			if err != nil {
				return nil, 0, "", errOrNotFound(err, bs.containerName, absKey)
			}

			if props.ContentLength == nil {
				return nil, 0, "", fmt.Errorf("blob properties missing ContentLength for blob %s", absKey)
			}

			downloadOptions = &blob.DownloadStreamOptions{
				Range: blob.HTTPRange{
					Offset: *props.ContentLength + br.offset, // Negative offset means distance from end of blob
					Count:  0,                                // Length of 0 means read to end of blob
				},
			}
		}
	}

	resp, err := blobClient.DownloadStream(ctx, downloadOptions)
	if err != nil {
		return nil, 0, "", errOrNotFound(err, bs.containerName, absKey)
	}

	// Get the ETag as the version
	var version string
	if resp.ETag != nil {
		version = string(*resp.ETag)
	}

	// Get the total size of the blob
	var size uint64
	if resp.ContentLength != nil {
		size = uint64(*resp.ContentLength)
	}

	// If this is a range request, try to get the full size from Content-Range header
	if !br.isAllRange() && resp.ContentRange != nil {
		fullSize := parseContentRangeSize(*resp.ContentRange)
		if fullSize > 0 {
			size = fullSize
		}
	}

	return resp.Body, size, version, nil
}

// Put creates a new blob from |reader| keyed by |key|
func (bs *AzureBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)

	// Read all data from reader into a buffer
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	// Upload the blob
	resp, err := bs.client.UploadBuffer(ctx, bs.containerName, absKey, data, nil)
	if err != nil {
		return "", err
	}

	// Return the ETag as the version
	var version string
	if resp.ETag != nil {
		version = string(*resp.ETag)
	}

	return version, nil
}

func blobExistsWhenShouldnt(expectedVersion string, err error) bool {
	errMsg := err.Error()
	return expectedVersion == "" && (strings.Contains(errMsg, "BlobAlreadyExists") || strings.Contains(errMsg, "409"))
}

func blobHasChanged(expectedVersion string, err error) bool {
	errMsg := err.Error()
	return expectedVersion != "" && (strings.Contains(errMsg, "ConditionNotMet") || strings.Contains(errMsg, "412"))
}

// CheckAndPut updates the blob keyed by |key| using a check-and-set on |expectedVersion|
func (bs *AzureBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	absKey := path.Join(bs.prefix, key)

	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	var uploadOptions *azblob.UploadBufferOptions
	if expectedVersion != "" {
		// Blob should exist - use If-Match with the expected ETag
		etag := azcore.ETag(expectedVersion)
		uploadOptions = &azblob.UploadBufferOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfMatch: &etag,
				},
			},
		}
	} else {
		// Blob should not exist - use If-None-Match: "*"
		uploadOptions = &azblob.UploadBufferOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfNoneMatch: to.Ptr(azcore.ETagAny),
				},
			},
		}
	}

	resp, err := bs.client.UploadBuffer(ctx, bs.containerName, absKey, data, uploadOptions)
	if err != nil {
		if blobExistsWhenShouldnt(expectedVersion, err) || blobHasChanged(expectedVersion, err) {
			// Get the current version to return in the error
			blobClient := bs.client.ServiceClient().NewContainerClient(bs.containerName).NewBlobClient(absKey)
			props, propErr := blobClient.GetProperties(ctx, nil)
			actualVersion := "unknown"
			if propErr == nil && props.ETag != nil {
				actualVersion = string(*props.ETag)
			}
			return "", CheckAndPutError{key, expectedVersion, actualVersion}
		}
	
		return "", err
	}

	// Return the ETag as the version
	var version string
	if resp.ETag != nil {
		version = string(*resp.ETag)
	}

	return version, nil
}

// readSeekCloser wraps a bytes.Reader to implement io.ReadSeekCloser
type readSeekCloser struct {
	*bytes.Reader
}

func (rsc *readSeekCloser) Close() error {
	return nil
}

// Concatenate creates a new blob named |key| by concatenating |sources|
func (bs *AzureBlobstore) Concatenate(ctx context.Context, key string, sources []string) (string, error) {
	absKey := path.Join(bs.prefix, key)
	blockBlobClient := bs.client.ServiceClient().NewContainerClient(bs.containerName).NewBlockBlobClient(absKey)

	// Azure Block Blob allows us to stage blocks and then commit them
	// We'll stage each source blob as a block, then commit all blocks
	blockIDs := make([]string, len(sources))

	for i, source := range sources {
		// Generate a unique block ID (must be base64 encoded, same length for all blocks)
		blockIDRaw := fmt.Sprintf("%064d", i)
		blockID := base64.StdEncoding.EncodeToString([]byte(blockIDRaw))
		blockIDs[i] = blockID

		// Download the source blob
		sourceBlobClient := bs.client.ServiceClient().NewContainerClient(bs.containerName).NewBlobClient(path.Join(bs.prefix, source))
		downloadResp, err := sourceBlobClient.DownloadStream(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("failed to download source blob %s: %w", source, err)
		}

		// Read the data into memory to create a ReadSeekCloser
		data, err := io.ReadAll(downloadResp.Body)
		downloadResp.Body.Close()
		if err != nil {
			return "", fmt.Errorf("failed to read source blob %s: %w", source, err)
		}

		// Stage the block
		reader := &readSeekCloser{bytes.NewReader(data)}
		_, err = blockBlobClient.StageBlock(ctx, blockID, reader, &blockblob.StageBlockOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to stage block for source %s: %w", source, err)
		}
	}

	// Commit all blocks to create the final blob
	resp, err := blockBlobClient.CommitBlockList(ctx, blockIDs, nil)
	if err != nil {
		return "", fmt.Errorf("failed to commit block list: %w", err)
	}

	// Return the ETag as the version
	var version string
	if resp.ETag != nil {
		version = string(*resp.ETag)
	}

	return version, nil
}
