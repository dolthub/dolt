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
)

var _ Blobstore = &AzureBlobstore{}

type AzureBlobstore struct {
	azClient      azureBlobClient
	containerName string
	prefix        string
}

// NewAzureBlobstore creates a new instance of an AzureBlobstore
func NewAzureBlobstore(client *azblob.Client, containerName, prefix string) *AzureBlobstore {
	return newAzureBlobstoreWithClient(newRealAzClient(client), containerName, prefix)
}

// newAzureBlobstoreWithClient creates a new instance with a custom client (for testing)
func newAzureBlobstoreWithClient(azClient azureBlobClient, containerName, prefix string) *AzureBlobstore {
	// Remove leading slashes from prefix
	prefix = normalizeAzPrefix(prefix)

	return &AzureBlobstore{
		azClient:      azClient,
		containerName: containerName,
		prefix:        prefix,
	}
}

// normalizeAzPrefix removes leading slashes from a prefix
func normalizeAzPrefix(prefix string) string {
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	return prefix
}

// Path returns this blobstore's path (i.e. container name + prefix)
func (bs *AzureBlobstore) Path() string {
	return path.Join(bs.containerName, bs.prefix)
}

// absKey returns the absolute key for a blob (prefix + key)
func (bs *AzureBlobstore) absKey(key string) string {
	return path.Join(bs.prefix, key)
}

// Exists returns true if a blob keyed by |key| exists
func (bs *AzureBlobstore) Exists(ctx context.Context, key string) (bool, error) {
	absKey := bs.absKey(key)

	_, err := bs.azClient.GetProperties(ctx, bs.containerName, absKey)
	if err != nil {
		if isBlobNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// isBlobNotFoundError checks if an error indicates a blob doesn't exist
func isBlobNotFoundError(err error) bool {
	errMsg := err.Error()
	return strings.Contains(errMsg, "BlobNotFound") || strings.Contains(errMsg, "404")
}

// errOrNotFound converts Azure errors to NotFound errors when appropriate
func errOrNotFound(err error, containerName, absKey string) error {
	if isBlobNotFoundError(err) {
		return NotFound{path.Join(containerName, absKey)}
	}
	return err
}

// Get retrieves an io.ReadCloser for the portion of a blob specified by br along with its version
func (bs *AzureBlobstore) Get(ctx context.Context, key string, br BlobRange) (io.ReadCloser, uint64, string, error) {
	absKey := bs.absKey(key)

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
			// Azure doesn't support negative offsets, so we need to get the blob size first
			props, err := bs.azClient.GetProperties(ctx, bs.containerName, absKey)
			if err != nil {
				return nil, 0, "", errOrNotFound(err, bs.containerName, absKey)
			}

			contentLength := props.GetContentLength()
			if contentLength == nil {
				return nil, 0, "", fmt.Errorf("blob properties missing ContentLength for blob %s", absKey)
			}

			downloadOptions = &blob.DownloadStreamOptions{
				Range: blob.HTTPRange{
					Offset: *contentLength + br.offset, // Negative offset means distance from end of blob
					Count:  0,                          // Length of 0 means read to end of blob
				},
			}
		}
	}

	resp, err := bs.azClient.DownloadStream(ctx, bs.containerName, absKey, downloadOptions)
	if err != nil {
		return nil, 0, "", errOrNotFound(err, bs.containerName, absKey)
	}

	// Get the ETag as the version
	version := etagToString(resp.GetETag())

	// Get the total size of the blob
	var size uint64
	if resp.GetContentLength() != nil {
		size = uint64(*resp.GetContentLength())
	}

	// If this is a range request, try to get the full size from Content-Range header
	if !br.isAllRange() && resp.GetContentRange() != nil {
		fullSize := parseContentRangeSize(*resp.GetContentRange())
		if fullSize > 0 {
			size = fullSize
		}
	}

	return resp.GetBody(), size, version, nil
}

// Put creates a new blob from |reader| keyed by |key|
func (bs *AzureBlobstore) Put(ctx context.Context, key string, totalSize int64, reader io.Reader) (string, error) {
	absKey := bs.absKey(key)

	// Read all data from reader into a buffer
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	// Upload the blob
	resp, err := bs.azClient.UploadBuffer(ctx, bs.containerName, absKey, data, nil)
	if err != nil {
		return "", err
	}

	return etagToString(resp.GetETag()), nil
}

// blobExistsWhenShouldnt checks if error indicates blob exists when it shouldn't (409)
func blobExistsWhenShouldnt(expectedVersion string, err error) bool {
	errMsg := err.Error()
	return expectedVersion == "" && (strings.Contains(errMsg, "BlobAlreadyExists") || strings.Contains(errMsg, "409"))
}

// blobHasChanged checks if error indicates blob version has changed (412)
func blobHasChanged(expectedVersion string, err error) bool {
	errMsg := err.Error()
	return expectedVersion != "" && (strings.Contains(errMsg, "ConditionNotMet") || strings.Contains(errMsg, "412"))
}

// CheckAndPut updates the blob keyed by |key| using a check-and-set on |expectedVersion|
func (bs *AzureBlobstore) CheckAndPut(ctx context.Context, expectedVersion, key string, totalSize int64, reader io.Reader) (string, error) {
	absKey := bs.absKey(key)

	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	uploadOptions := buildCheckAndPutOptions(expectedVersion)

	resp, err := bs.azClient.UploadBuffer(ctx, bs.containerName, absKey, data, uploadOptions)
	if err != nil {
		if blobExistsWhenShouldnt(expectedVersion, err) || blobHasChanged(expectedVersion, err) {
			// Get the current version to return in the error
			actualVersion := bs.getCurrentVersion(ctx, absKey)
			return "", CheckAndPutError{key, expectedVersion, actualVersion}
		}
		return "", err
	}

	return etagToString(resp.GetETag()), nil
}

// buildCheckAndPutOptions creates upload options for CheckAndPut
func buildCheckAndPutOptions(expectedVersion string) *azblob.UploadBufferOptions {
	if expectedVersion != "" {
		// Blob should exist - use If-Match with the expected ETag
		etag := azcore.ETag(expectedVersion)
		return &azblob.UploadBufferOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfMatch: &etag,
				},
			},
		}
	}

	// Blob should not exist - use If-None-Match: "*"
	return &azblob.UploadBufferOptions{
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: to.Ptr(azcore.ETagAny),
			},
		},
	}
}

// getCurrentVersion gets the current version (ETag) of a blob
func (bs *AzureBlobstore) getCurrentVersion(ctx context.Context, absKey string) string {
	props, err := bs.azClient.GetProperties(ctx, bs.containerName, absKey)
	if err != nil {
		return "unknown"
	}
	return etagToString(props.GetETag())
}

// etagToString converts an ETag pointer to a string
func etagToString(etag *azcore.ETag) string {
	if etag == nil {
		return ""
	}
	return string(*etag)
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
	absKey := bs.absKey(key)

	// Generate block IDs
	blockIDs := make([]string, len(sources))
	for i := range sources {
		blockIDs[i] = generateBlockID(i)
	}

	// Stage each source blob as a block
	for i, source := range sources {
		sourceKey := bs.absKey(source)
		data, err := bs.downloadBlob(ctx, sourceKey)
		if err != nil {
			return "", fmt.Errorf("failed to download source blob %s: %w", source, err)
		}

		reader := &readSeekCloser{bytes.NewReader(data)}
		err = bs.azClient.StageBlock(ctx, bs.containerName, absKey, blockIDs[i], reader)
		if err != nil {
			return "", fmt.Errorf("failed to stage block for source %s: %w", source, err)
		}
	}

	// Commit all blocks to create the final blob
	resp, err := bs.azClient.CommitBlockList(ctx, bs.containerName, absKey, blockIDs)
	if err != nil {
		return "", fmt.Errorf("failed to commit block list: %w", err)
	}

	return etagToString(resp.GetETag()), nil
}

// generateBlockID generates a base64-encoded block ID
func generateBlockID(index int) string {
	blockIDRaw := fmt.Sprintf("%064d", index)
	return base64.StdEncoding.EncodeToString([]byte(blockIDRaw))
}

// downloadBlob downloads a blob and returns its contents
func (bs *AzureBlobstore) downloadBlob(ctx context.Context, absKey string) ([]byte, error) {
	resp, err := bs.azClient.DownloadStream(ctx, bs.containerName, absKey, nil)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.GetBody())
	resp.GetBody().Close()
	if err != nil {
		return nil, err
	}

	return data, nil
}
