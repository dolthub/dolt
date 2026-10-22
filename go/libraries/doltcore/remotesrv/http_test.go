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

package remotesrv

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type rejectingDBCache struct{}

func (rejectingDBCache) Get(context.Context, string, string) (RemoteSrvStore, error) {
	return nil, errors.New("unexpected cache lookup")
}

func TestUploadRejectsTraversalPath(t *testing.T) {
	fileID := hash.New(GoodHash).String()
	handler := newFileHandler(logrus.NewEntry(logrus.New()), rejectingDBCache{}, nil, false, identitySealer{})
	// A leading slash becomes an absolute path after ServeHTTP trims one slash.
	for _, repoPath := range []string{"../outside", "org/../outside", "org/./repo", "org\\..\\outside", "/org/repo"} {
		t.Run(repoPath, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://example.com/"+repoPath+"/"+fileID+"?num_chunks=1&content_length=3", strings.NewReader("abc"))
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)
		})
	}
}

type singleStoreDBCache struct {
	store RemoteSrvStore
}

func (c singleStoreDBCache) Get(context.Context, string, string) (RemoteSrvStore, error) {
	return c.store, nil
}

func TestUploadVerifiesBodyAndDoesNotReplace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := nbs.NewLocalStore(ctx, types.Format_DOLT.VersionString(), dir, 1<<20, nbs.NewUnlimitedMemQuotaProvider(), false)
	require.NoError(t, err)
	defer store.Close()
	handler := newFileHandler(logrus.NewEntry(logrus.New()), singleStoreDBCache{store}, nil, false, identitySealer{})

	fileID, data, _, err := nbs.WriteChunks([]chunks.Chunk{chunks.NewChunk([]byte("a chunk"))})
	require.NoError(t, err)
	filePath := filepath.Join(dir, fileID)

	put := func(t *testing.T, contentLength int, contentHash []byte, body []byte) int {
		url := fmt.Sprintf("http://example.com/org/repo/%s?num_chunks=1&content_length=%d&content_hash=%s",
			fileID, contentLength, base64.RawURLEncoding.EncodeToString(contentHash))
		req := httptest.NewRequest(http.MethodPut, url, bytes.NewReader(body))
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		return resp.Code
	}
	md5Of := func(b []byte) []byte {
		sum := md5.Sum(b)
		return sum[:]
	}
	requireNoFile := func(t *testing.T) {
		_, err := os.Stat(filePath)
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	garbage := bytes.Repeat([]byte("x"), len(data))

	t.Run("HashMismatch", func(t *testing.T) {
		require.Equal(t, http.StatusBadRequest, put(t, len(data), md5Of(data), garbage))
		requireNoFile(t)
	})
	t.Run("BodyShort", func(t *testing.T) {
		require.Equal(t, http.StatusBadRequest, put(t, len(data)+1, md5Of(data), data))
		requireNoFile(t)
	})
	t.Run("BodyLong", func(t *testing.T) {
		require.Equal(t, http.StatusBadRequest, put(t, len(data)-1, md5Of(data), data))
		requireNoFile(t)
	})
	t.Run("Lands", func(t *testing.T) {
		require.Equal(t, http.StatusOK, put(t, len(data), md5Of(data), data))
		got, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Equal(t, data, got)
	})
	t.Run("DoesNotReplace", func(t *testing.T) {
		// A well formed upload of other bytes to an existing file ID
		// succeeds, but leaves the existing file in place.
		require.Equal(t, http.StatusOK, put(t, len(garbage), md5Of(garbage), garbage))
		got, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Equal(t, data, got)
	})
}
