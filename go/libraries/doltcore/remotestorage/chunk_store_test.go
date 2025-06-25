// Copyright 2020 Dolthub, Inc.
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
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
)

func TestSanitizeSignedUrl(t *testing.T) {
	res := sanitizeSignedUrl("")
	assert.Equal(t, "", res)

	res = sanitizeSignedUrl("https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCcBe%EXAMPLEKnz3r8O0AgEXAMPLE&Expires=1555531131")
	assert.Equal(t, "https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCc...&Expires=1555531131", res)

	res = sanitizeSignedUrl("https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCcBe%EXAMPLEKnz3r8O0AgEXAMPLE")
	assert.Equal(t, "https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCc...", res)
}

func TestDoltRemoteTableFile(t *testing.T) {
	t.Run("Open", func(t *testing.T) {
		t.Run("200", func(t *testing.T) {
			f := DoltRemoteTableFile{
				dcs: &DoltChunkStore{
					httpFetcher: fetcher(func(*http.Request) (*http.Response, error) {
						return &http.Response{
							Body:          io.NopCloser(&bytes.Buffer{}),
							StatusCode:    200,
							ContentLength: 1024,
						}, nil
					}),
				},
				info: &remotesapi.TableFileInfo{
					Url: "http://localhost/example_url/fileid",
				},
			}
			reader, sz, err := f.Open(context.Background())
			require.NoError(t, err)
			require.Equal(t, uint64(1024), sz)
			require.NoError(t, reader.Close())
		})
		t.Run("DoesNotRefreshBeforeExpiry", func(t *testing.T) {
			f := DoltRemoteTableFile{
				dcs: &DoltChunkStore{
					httpFetcher: fetcher(func(*http.Request) (*http.Response, error) {
						return &http.Response{
							Body:       io.NopCloser(&bytes.Buffer{}),
							StatusCode: 200,
						}, nil
					}),
				},
				info: &remotesapi.TableFileInfo{
					Url:          "http://localhost/example_url/fileid",
					RefreshAfter: timestamppb.Now(),
				},
			}
			f.info.RefreshAfter.Seconds += 10
			_, _, err := f.Open(context.Background())
			require.NoError(t, err)
		})
		t.Run("DoesRefreshAfterExpiry", func(t *testing.T) {
			var csClient refreshClient
			csClient.resp = &remotesapi.RefreshTableFileUrlResponse{
				Url:          "http://localhost/example_url/fileid&refreshed",
				RefreshAfter: timestamppb.Now(),
			}
			csClient.resp.RefreshAfter.Seconds += 10
			var seenUrl string
			f := DoltRemoteTableFile{
				dcs: &DoltChunkStore{
					httpFetcher: fetcher(func(req *http.Request) (*http.Response, error) {
						seenUrl = req.URL.String()
						return &http.Response{
							Body:       io.NopCloser(&bytes.Buffer{}),
							StatusCode: 200,
						}, nil
					}),
					csClient: &csClient,
				},
				info: &remotesapi.TableFileInfo{
					Url:          "http://localhost/example_url/fileid",
					RefreshAfter: timestamppb.Now(),
				},
			}
			f.info.RefreshAfter.Seconds -= 10
			_, _, err := f.Open(context.Background())
			require.NoError(t, err)
			require.Equal(t, seenUrl, "http://localhost/example_url/fileid&refreshed")
			require.Equal(t, f.info.RefreshAfter, csClient.resp.RefreshAfter)
		})
		t.Run("404", func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteString("an error message")
			f := DoltRemoteTableFile{
				dcs: &DoltChunkStore{
					httpFetcher: fetcher(func(*http.Request) (*http.Response, error) {
						return &http.Response{
							Body:       io.NopCloser(&buf),
							StatusCode: 404,
						}, nil
					}),
				},
				info: &remotesapi.TableFileInfo{
					Url: "http://localhost/example_url/fileid",
				},
			}
			reader, _, err := f.Open(context.Background())
			require.ErrorIs(t, err, ErrRemoteTableFileGet)
			require.ErrorContains(t, err, "an error message")
			require.ErrorContains(t, err, "404")
			require.Nil(t, reader)
		})
		t.Run("403UpdatesRefreshAfter", func(t *testing.T) {
			f := DoltRemoteTableFile{
				dcs: &DoltChunkStore{
					httpFetcher: fetcher(func(*http.Request) (*http.Response, error) {
						return &http.Response{
							Body:       io.NopCloser(&bytes.Buffer{}),
							StatusCode: 403,
						}, nil
					}),
				},
				info: &remotesapi.TableFileInfo{
					Url:          "http://localhost/example_url/fileid",
					RefreshAfter: timestamppb.Now(),
				},
			}
			f.info.RefreshAfter.Seconds += 10
			require.True(t, time.Now().Before(f.info.RefreshAfter.AsTime()))
			reader, _, err := f.Open(context.Background())
			require.ErrorIs(t, err, ErrRemoteTableFileGet)
			require.ErrorContains(t, err, "403")
			require.Nil(t, reader)
			require.True(t, time.Now().After(f.info.RefreshAfter.AsTime()))
		})
	})
}

type fetcher func(*http.Request) (*http.Response, error)

func (f fetcher) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

type refreshClient struct {
	called bool
	resp   *remotesapi.RefreshTableFileUrlResponse
}

func (f refreshClient) GetRepoMetadata(ctx context.Context, in *remotesapi.GetRepoMetadataRequest, opts ...grpc.CallOption) (*remotesapi.GetRepoMetadataResponse, error) {
	return nil, nil
}
func (f refreshClient) HasChunks(ctx context.Context, in *remotesapi.HasChunksRequest, opts ...grpc.CallOption) (*remotesapi.HasChunksResponse, error) {
	return nil, nil
}
func (f refreshClient) GetDownloadLocations(ctx context.Context, in *remotesapi.GetDownloadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetDownloadLocsResponse, error) {
	return nil, nil
}
func (f refreshClient) StreamDownloadLocations(ctx context.Context, opts ...grpc.CallOption) (remotesapi.ChunkStoreService_StreamDownloadLocationsClient, error) {
	return nil, nil
}
func (f refreshClient) GetUploadLocations(ctx context.Context, in *remotesapi.GetUploadLocsRequest, opts ...grpc.CallOption) (*remotesapi.GetUploadLocsResponse, error) {
	return nil, nil
}
func (f refreshClient) Rebase(ctx context.Context, in *remotesapi.RebaseRequest, opts ...grpc.CallOption) (*remotesapi.RebaseResponse, error) {
	return nil, nil
}
func (f refreshClient) Root(ctx context.Context, in *remotesapi.RootRequest, opts ...grpc.CallOption) (*remotesapi.RootResponse, error) {
	return nil, nil
}
func (f refreshClient) Commit(ctx context.Context, in *remotesapi.CommitRequest, opts ...grpc.CallOption) (*remotesapi.CommitResponse, error) {
	return nil, nil
}
func (f refreshClient) ListTableFiles(ctx context.Context, in *remotesapi.ListTableFilesRequest, opts ...grpc.CallOption) (*remotesapi.ListTableFilesResponse, error) {
	return nil, nil
}
func (f *refreshClient) RefreshTableFileUrl(ctx context.Context, in *remotesapi.RefreshTableFileUrlRequest, opts ...grpc.CallOption) (*remotesapi.RefreshTableFileUrlResponse, error) {
	f.called = true
	return f.resp, nil
}
func (f refreshClient) AddTableFiles(ctx context.Context, in *remotesapi.AddTableFilesRequest, opts ...grpc.CallOption) (*remotesapi.AddTableFilesResponse, error) {
	return nil, nil
}
