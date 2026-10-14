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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

type rejectingDBCache struct{}

func (rejectingDBCache) Get(context.Context, string, string) (RemoteSrvStore, error) {
	return nil, errors.New("unexpected cache lookup")
}

func TestUploadRejectsTraversalPath(t *testing.T) {
	fileID := hash.New(GoodHash).String()
	handler := newFileHandler(logrus.NewEntry(logrus.New()), rejectingDBCache{}, nil, false, identitySealer{})
	for _, repoPath := range []string{"../outside", "org/../outside", "org/./repo", "org\\..\\outside"} {
		t.Run(repoPath, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://example.com/"+repoPath+"/"+fileID+"?num_chunks=1&content_length=3", strings.NewReader("abc"))
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)
		})
	}
}
