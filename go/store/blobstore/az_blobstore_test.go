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
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock implementations

type mockAzureClient struct {
	getPropertiesFn   func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error)
	downloadStreamFn  func(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error)
	uploadBufferFn    func(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error)
	stageBlockFn      func(ctx context.Context, containerName, blobName, blockID string, body io.ReadSeekCloser) error
	commitBlockListFn func(ctx context.Context, containerName, blobName string, blockIDs []string) (azureUploadResponse, error)
}

func (m *mockAzureClient) GetProperties(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
	if m.getPropertiesFn != nil {
		return m.getPropertiesFn(ctx, containerName, blobName)
	}
	return nil, errors.New("not implemented")
}

func (m *mockAzureClient) DownloadStream(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
	if m.downloadStreamFn != nil {
		return m.downloadStreamFn(ctx, containerName, blobName, options)
	}
	return nil, errors.New("not implemented")
}

func (m *mockAzureClient) UploadBuffer(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
	if m.uploadBufferFn != nil {
		return m.uploadBufferFn(ctx, containerName, blobName, data, options)
	}
	return nil, errors.New("not implemented")
}

func (m *mockAzureClient) StageBlock(ctx context.Context, containerName, blobName, blockID string, body io.ReadSeekCloser) error {
	if m.stageBlockFn != nil {
		return m.stageBlockFn(ctx, containerName, blobName, blockID, body)
	}
	return errors.New("not implemented")
}

func (m *mockAzureClient) CommitBlockList(ctx context.Context, containerName, blobName string, blockIDs []string) (azureUploadResponse, error) {
	if m.commitBlockListFn != nil {
		return m.commitBlockListFn(ctx, containerName, blobName, blockIDs)
	}
	return nil, errors.New("not implemented")
}

type mockBlobProperties struct {
	etag          *azcore.ETag
	contentLength *int64
}

func (m *mockBlobProperties) GetETag() *azcore.ETag {
	return m.etag
}

func (m *mockBlobProperties) GetContentLength() *int64 {
	return m.contentLength
}

type mockDownloadResponse struct {
	body          io.ReadCloser
	etag          *azcore.ETag
	contentLength *int64
	contentRange  *string
}

func (m *mockDownloadResponse) GetBody() io.ReadCloser {
	return m.body
}

func (m *mockDownloadResponse) GetETag() *azcore.ETag {
	return m.etag
}

func (m *mockDownloadResponse) GetContentLength() *int64 {
	return m.contentLength
}

func (m *mockDownloadResponse) GetContentRange() *string {
	return m.contentRange
}

type mockUploadResponse struct {
	etag *azcore.ETag
}

func (m *mockUploadResponse) GetETag() *azcore.ETag {
	return m.etag
}

func newMockETag(s string) *azcore.ETag {
	etag := azcore.ETag(s)
	return &etag
}

func int64Ptr(i int64) *int64 {
	return &i
}

func stringPtr(s string) *string {
	return &s
}

// Tests for normalizeAzPrefix

func TestNormalizePrefix(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		expected string
	}{
		{
			name:     "no_leading_slash",
			prefix:   "myprefix",
			expected: "myprefix",
		},
		{
			name:     "single_leading_slash",
			prefix:   "/myprefix",
			expected: "myprefix",
		},
		{
			name:     "multiple_leading_slashes",
			prefix:   "///myprefix",
			expected: "myprefix",
		},
		{
			name:     "empty_prefix",
			prefix:   "",
			expected: "",
		},
		{
			name:     "only_slashes",
			prefix:   "///",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeAzPrefix(tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Tests for Path

func TestAzureBlobstore_Path(t *testing.T) {
	tests := []struct {
		name          string
		containerName string
		prefix        string
		expected      string
	}{
		{
			name:          "with_prefix",
			containerName: "mycontainer",
			prefix:        "myprefix",
			expected:      "mycontainer/myprefix",
		},
		{
			name:          "without_prefix",
			containerName: "mycontainer",
			prefix:        "",
			expected:      "mycontainer",
		},
		{
			name:          "prefix_with_leading_slash",
			containerName: "mycontainer",
			prefix:        "/myprefix",
			expected:      "mycontainer/myprefix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockAzureClient{}
			bs := newAzureBlobstoreWithClient(mockClient, tt.containerName, tt.prefix)
			assert.Equal(t, tt.expected, bs.Path())
		})
	}
}

// Tests for absKey

func TestAzureBlobstore_AbsKey(t *testing.T) {
	mockClient := &mockAzureClient{}

	tests := []struct {
		name     string
		prefix   string
		key      string
		expected string
	}{
		{
			name:     "with_prefix",
			prefix:   "myprefix",
			key:      "mykey",
			expected: "myprefix/mykey",
		},
		{
			name:     "without_prefix",
			prefix:   "",
			key:      "mykey",
			expected: "mykey",
		},
		{
			name:     "nested_key",
			prefix:   "myprefix",
			key:      "folder/mykey",
			expected: "myprefix/folder/mykey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := newAzureBlobstoreWithClient(mockClient, "container", tt.prefix)
			result := bs.absKey(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Tests for Exists

func TestAzureBlobstore_Exists(t *testing.T) {
	ctx := context.Background()

	t.Run("blob_exists", func(t *testing.T) {
		mockClient := &mockAzureClient{
			getPropertiesFn: func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
				return &mockBlobProperties{
					etag: newMockETag("test-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		exists, err := bs.Exists(ctx, "mykey")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("blob_not_found", func(t *testing.T) {
		mockClient := &mockAzureClient{
			getPropertiesFn: func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
				return nil, errors.New("BlobNotFound: The specified blob does not exist")
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		exists, err := bs.Exists(ctx, "mykey")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("blob_not_found_404", func(t *testing.T) {
		mockClient := &mockAzureClient{
			getPropertiesFn: func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
				return nil, errors.New("RESPONSE 404: 404 Not Found")
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		exists, err := bs.Exists(ctx, "mykey")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("other_error", func(t *testing.T) {
		mockClient := &mockAzureClient{
			getPropertiesFn: func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
				return nil, errors.New("some other error")
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		exists, err := bs.Exists(ctx, "mykey")
		require.Error(t, err)
		assert.False(t, exists)
	})
}

// Tests for Put

func TestAzureBlobstore_Put(t *testing.T) {
	ctx := context.Background()

	t.Run("successful_put", func(t *testing.T) {
		expectedData := []byte("test data")
		var capturedData []byte

		mockClient := &mockAzureClient{
			uploadBufferFn: func(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
				capturedData = data
				assert.Equal(t, "container", containerName)
				assert.Equal(t, "prefix/mykey", blobName)
				return &mockUploadResponse{
					etag: newMockETag("test-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.Put(ctx, "mykey", int64(len(expectedData)), bytes.NewReader(expectedData))
		require.NoError(t, err)
		assert.Equal(t, "test-etag", version)
		assert.Equal(t, expectedData, capturedData)
	})

	t.Run("upload_error", func(t *testing.T) {
		mockClient := &mockAzureClient{
			uploadBufferFn: func(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
				return nil, errors.New("upload failed")
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.Put(ctx, "mykey", 9, bytes.NewReader([]byte("test data")))
		require.Error(t, err)
		assert.Empty(t, version)
	})
}

// Tests for Get

func TestAzureBlobstore_Get(t *testing.T) {
	ctx := context.Background()

	t.Run("get_full_blob", func(t *testing.T) {
		expectedData := []byte("test data")
		mockClient := &mockAzureClient{
			downloadStreamFn: func(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
				assert.Nil(t, options)
				return &mockDownloadResponse{
					body:          io.NopCloser(bytes.NewReader(expectedData)),
					etag:          newMockETag("test-etag"),
					contentLength: int64Ptr(int64(len(expectedData))),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		rc, size, version, err := bs.Get(ctx, "mykey", AllRange)
		require.NoError(t, err)
		assert.Equal(t, uint64(len(expectedData)), size)
		assert.Equal(t, "test-etag", version)

		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		assert.Equal(t, expectedData, data)
	})

	t.Run("get_with_positive_range", func(t *testing.T) {
		expectedData := []byte("test")
		mockClient := &mockAzureClient{
			downloadStreamFn: func(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
				require.NotNil(t, options)
				assert.Equal(t, int64(10), options.Range.Offset)
				assert.Equal(t, int64(4), options.Range.Count)
				return &mockDownloadResponse{
					body:          io.NopCloser(bytes.NewReader(expectedData)),
					etag:          newMockETag("test-etag"),
					contentLength: int64Ptr(4),
					contentRange:  stringPtr("bytes 10-13/100"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		rc, size, version, err := bs.Get(ctx, "mykey", NewBlobRange(10, 4))
		require.NoError(t, err)
		assert.Equal(t, uint64(100), size) // Extracted from Content-Range
		assert.Equal(t, "test-etag", version)

		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		assert.Equal(t, expectedData, data)
	})

	t.Run("get_with_negative_offset", func(t *testing.T) {
		expectedData := []byte("end")
		mockClient := &mockAzureClient{
			getPropertiesFn: func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
				return &mockBlobProperties{
					contentLength: int64Ptr(100),
				}, nil
			},
			downloadStreamFn: func(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
				require.NotNil(t, options)
				assert.Equal(t, int64(97), options.Range.Offset) // 100 + (-3) = 97
				assert.Equal(t, int64(0), options.Range.Count)   // 0 means to end
				return &mockDownloadResponse{
					body:          io.NopCloser(bytes.NewReader(expectedData)),
					etag:          newMockETag("test-etag"),
					contentLength: int64Ptr(3),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		rc, size, version, err := bs.Get(ctx, "mykey", NewBlobRange(-3, 0))
		require.NoError(t, err)
		assert.Equal(t, uint64(3), size)
		assert.Equal(t, "test-etag", version)

		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		assert.Equal(t, expectedData, data)
	})

	t.Run("get_not_found", func(t *testing.T) {
		mockClient := &mockAzureClient{
			downloadStreamFn: func(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
				return nil, errors.New("BlobNotFound: blob does not exist")
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		_, _, _, err := bs.Get(ctx, "mykey", AllRange)
		require.Error(t, err)
		assert.True(t, IsNotFoundError(err))
	})
}

// Tests for CheckAndPut

func TestAzureBlobstore_CheckAndPut(t *testing.T) {
	ctx := context.Background()

	t.Run("create_new_blob", func(t *testing.T) {
		mockClient := &mockAzureClient{
			uploadBufferFn: func(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
				// Verify options are set for creation (If-None-Match: *)
				return &mockUploadResponse{
					etag: newMockETag("new-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.CheckAndPut(ctx, "", "mykey", 9, bytes.NewReader([]byte("test data")))
		require.NoError(t, err)
		assert.Equal(t, "new-etag", version)
	})

	t.Run("update_with_correct_version", func(t *testing.T) {
		mockClient := &mockAzureClient{
			uploadBufferFn: func(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
				return &mockUploadResponse{
					etag: newMockETag("new-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.CheckAndPut(ctx, "old-etag", "mykey", 9, bytes.NewReader([]byte("test data")))
		require.NoError(t, err)
		assert.Equal(t, "new-etag", version)
	})

	t.Run("create_fails_blob_exists", func(t *testing.T) {
		mockClient := &mockAzureClient{
			uploadBufferFn: func(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
				return nil, errors.New("RESPONSE 409: BlobAlreadyExists")
			},
			getPropertiesFn: func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
				return &mockBlobProperties{
					etag: newMockETag("existing-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.CheckAndPut(ctx, "", "mykey", 9, bytes.NewReader([]byte("test data")))
		require.Error(t, err)
		assert.Empty(t, version)
		assert.True(t, IsCheckAndPutError(err))

		cpe, ok := err.(CheckAndPutError)
		require.True(t, ok)
		assert.Equal(t, "mykey", cpe.Key)
		assert.Equal(t, "", cpe.ExpectedVersion)
		assert.Equal(t, "existing-etag", cpe.ActualVersion)
	})

	t.Run("update_fails_version_mismatch", func(t *testing.T) {
		mockClient := &mockAzureClient{
			uploadBufferFn: func(ctx context.Context, containerName, blobName string, data []byte, options *azblob.UploadBufferOptions) (azureUploadResponse, error) {
				return nil, errors.New("RESPONSE 412: ConditionNotMet")
			},
			getPropertiesFn: func(ctx context.Context, containerName, blobName string) (azureBlobProperties, error) {
				return &mockBlobProperties{
					etag: newMockETag("current-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.CheckAndPut(ctx, "old-etag", "mykey", 9, bytes.NewReader([]byte("test data")))
		require.Error(t, err)
		assert.Empty(t, version)
		assert.True(t, IsCheckAndPutError(err))

		cpe, ok := err.(CheckAndPutError)
		require.True(t, ok)
		assert.Equal(t, "mykey", cpe.Key)
		assert.Equal(t, "old-etag", cpe.ExpectedVersion)
		assert.Equal(t, "current-etag", cpe.ActualVersion)
	})
}

// Tests for Concatenate

func TestAzureBlobstore_Concatenate(t *testing.T) {
	ctx := context.Background()

	t.Run("concatenate_multiple_blobs", func(t *testing.T) {
		blobData := map[string][]byte{
			"prefix/part1": []byte("Hello "),
			"prefix/part2": []byte("World"),
		}

		var stagedBlocks []string
		var stagedData [][]byte

		mockClient := &mockAzureClient{
			downloadStreamFn: func(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
				data, ok := blobData[blobName]
				if !ok {
					return nil, fmt.Errorf("blob not found: %s", blobName)
				}
				return &mockDownloadResponse{
					body: io.NopCloser(bytes.NewReader(data)),
				}, nil
			},
			stageBlockFn: func(ctx context.Context, containerName, blobName, blockID string, body io.ReadSeekCloser) error {
				stagedBlocks = append(stagedBlocks, blockID)
				data, _ := io.ReadAll(body)
				stagedData = append(stagedData, data)
				return nil
			},
			commitBlockListFn: func(ctx context.Context, containerName, blobName string, blockIDs []string) (azureUploadResponse, error) {
				assert.Equal(t, len(stagedBlocks), len(blockIDs))
				return &mockUploadResponse{
					etag: newMockETag("concat-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.Concatenate(ctx, "result", []string{"part1", "part2"})
		require.NoError(t, err)
		assert.Equal(t, "concat-etag", version)
		assert.Equal(t, 2, len(stagedBlocks))
		assert.Equal(t, []byte("Hello "), stagedData[0])
		assert.Equal(t, []byte("World"), stagedData[1])
	})

	t.Run("concatenate_empty_list", func(t *testing.T) {
		mockClient := &mockAzureClient{
			commitBlockListFn: func(ctx context.Context, containerName, blobName string, blockIDs []string) (azureUploadResponse, error) {
				assert.Equal(t, 0, len(blockIDs))
				return &mockUploadResponse{
					etag: newMockETag("empty-etag"),
				}, nil
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.Concatenate(ctx, "result", []string{})
		require.NoError(t, err)
		assert.Equal(t, "empty-etag", version)
	})

	t.Run("concatenate_source_not_found", func(t *testing.T) {
		mockClient := &mockAzureClient{
			downloadStreamFn: func(ctx context.Context, containerName, blobName string, options *blob.DownloadStreamOptions) (azureDownloadResponse, error) {
				return nil, errors.New("BlobNotFound")
			},
		}

		bs := newAzureBlobstoreWithClient(mockClient, "container", "prefix")
		version, err := bs.Concatenate(ctx, "result", []string{"missing"})
		require.Error(t, err)
		assert.Empty(t, version)
		assert.Contains(t, err.Error(), "failed to download source blob")
	})
}

// Tests for helper functions

func TestBlobExistsWhenShouldnt(t *testing.T) {
	tests := []struct {
		name            string
		expectedVersion string
		err             error
		want            bool
	}{
		{
			name:            "empty_version_with_409",
			expectedVersion: "",
			err:             errors.New("RESPONSE 409: Conflict"),
			want:            true,
		},
		{
			name:            "empty_version_with_BlobAlreadyExists",
			expectedVersion: "",
			err:             errors.New("ERROR CODE: BlobAlreadyExists"),
			want:            true,
		},
		{
			name:            "non_empty_version_with_409",
			expectedVersion: "version123",
			err:             errors.New("RESPONSE 409"),
			want:            false,
		},
		{
			name:            "empty_version_with_different_error",
			expectedVersion: "",
			err:             errors.New("Some other error"),
			want:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blobExistsWhenShouldnt(tt.expectedVersion, tt.err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestBlobHasChanged(t *testing.T) {
	tests := []struct {
		name            string
		expectedVersion string
		err             error
		want            bool
	}{
		{
			name:            "non_empty_version_with_412",
			expectedVersion: "version123",
			err:             errors.New("RESPONSE 412: Precondition Failed"),
			want:            true,
		},
		{
			name:            "non_empty_version_with_ConditionNotMet",
			expectedVersion: "version123",
			err:             errors.New("ERROR CODE: ConditionNotMet"),
			want:            true,
		},
		{
			name:            "empty_version_with_412",
			expectedVersion: "",
			err:             errors.New("RESPONSE 412"),
			want:            false,
		},
		{
			name:            "non_empty_version_with_different_error",
			expectedVersion: "version123",
			err:             errors.New("Some other error"),
			want:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blobHasChanged(tt.expectedVersion, tt.err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestIsBlobNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "BlobNotFound_error",
			err:  errors.New("BlobNotFound: The blob does not exist"),
			want: true,
		},
		{
			name: "404_error",
			err:  errors.New("RESPONSE 404: Not Found"),
			want: true,
		},
		{
			name: "other_error",
			err:  errors.New("Some other error"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isBlobNotFoundError(tt.err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestErrOrNotFound(t *testing.T) {
	t.Run("blob_not_found_error", func(t *testing.T) {
		err := errOrNotFound(errors.New("BlobNotFound"), "container", "key")
		require.Error(t, err)
		assert.True(t, IsNotFoundError(err))

		nf, ok := err.(NotFound)
		require.True(t, ok)
		assert.Equal(t, "container/key", nf.Key)
	})

	t.Run("404_error", func(t *testing.T) {
		err := errOrNotFound(errors.New("RESPONSE 404"), "container", "key")
		require.Error(t, err)
		assert.True(t, IsNotFoundError(err))
	})

	t.Run("other_error", func(t *testing.T) {
		originalErr := errors.New("some other error")
		err := errOrNotFound(originalErr, "container", "key")
		require.Error(t, err)
		assert.False(t, IsNotFoundError(err))
		assert.Equal(t, originalErr, err)
	})
}

func TestGenerateBlockID(t *testing.T) {
	tests := []struct {
		index int
	}{
		{index: 0},
		{index: 1},
		{index: 42},
		{index: 999},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("index_%d", tt.index), func(t *testing.T) {
			blockID := generateBlockID(tt.index)

			// Block ID should be base64 encoded
			assert.NotEmpty(t, blockID)

			// Should be able to decode it
			decoded, err := base64.StdEncoding.DecodeString(blockID)
			require.NoError(t, err)

			// Decoded value should contain the index
			assert.Contains(t, string(decoded), fmt.Sprintf("%d", tt.index))
		})
	}
}

func TestEtagToString(t *testing.T) {
	t.Run("nil_etag", func(t *testing.T) {
		result := etagToString(nil)
		assert.Empty(t, result)
	})

	t.Run("valid_etag", func(t *testing.T) {
		result := etagToString(newMockETag("test-etag"))
		assert.Equal(t, "test-etag", result)
	})
}

func TestBuildCheckAndPutOptions(t *testing.T) {
	t.Run("empty_expected_version", func(t *testing.T) {
		options := buildCheckAndPutOptions("")
		require.NotNil(t, options)
		require.NotNil(t, options.AccessConditions)
		require.NotNil(t, options.AccessConditions.ModifiedAccessConditions)
		require.NotNil(t, options.AccessConditions.ModifiedAccessConditions.IfNoneMatch)
		assert.Nil(t, options.AccessConditions.ModifiedAccessConditions.IfMatch)
	})

	t.Run("non_empty_expected_version", func(t *testing.T) {
		options := buildCheckAndPutOptions("test-version")
		require.NotNil(t, options)
		require.NotNil(t, options.AccessConditions)
		require.NotNil(t, options.AccessConditions.ModifiedAccessConditions)
		require.NotNil(t, options.AccessConditions.ModifiedAccessConditions.IfMatch)
		assert.Equal(t, azcore.ETag("test-version"), *options.AccessConditions.ModifiedAccessConditions.IfMatch)
		assert.Nil(t, options.AccessConditions.ModifiedAccessConditions.IfNoneMatch)
	})
}

func TestReadSeekCloser(t *testing.T) {
	data := []byte("test data")
	rsc := &readSeekCloser{bytes.NewReader(data)}

	// Test Read
	buf := make([]byte, 4)
	n, err := rsc.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("test"), buf)

	// Test Seek
	offset, err := rsc.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	// Test Close (should be no-op)
	err = rsc.Close()
	require.NoError(t, err)
}
