// Copyright 2024 Dolthub, Inc.
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
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestGRPCSchemeSelection(t *testing.T) {
	rs := &RemoteChunkStore{
		httpScheme: "http",
	}

	md := metadata.New(nil)
	scheme := rs.getScheme(md)
	assert.Equal(t, scheme, "http")

	md.Append("x-forwarded-proto", "https")
	scheme = rs.getScheme(md)
	assert.Equal(t, scheme, "https")
}

func TestGetUploadDownloadUrl(t *testing.T) {
	type testCase struct {
		storeScheme       string
		forwardedScheme   string
		expectedUrlScheme string
	}
	var id hash.Hash
	rand.Read(id[:])
	testCases := []testCase{
		{"http", "", "http"},
		{"http", "http", "http"},
		{"http", "https", "https"},
		{"https", "", "https"},
		{"https", "https", "https"},
		{"https", "http", "http"},
	}
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s,%s", testCase.storeScheme, testCase.forwardedScheme), func(t *testing.T) {
			rs := &RemoteChunkStore{
				httpScheme: testCase.storeScheme,
			}
			md := metadata.New(nil)
			md.Append(":authority", "hostname.local")
			if testCase.forwardedScheme != "" {
				md.Append("x-forwarded-proto", testCase.forwardedScheme)
			}
			db := "databasename"
			path := db + "/" + id.String() + ".darc"
			url := rs.getDownloadUrl(md, path)
			assert.Equal(t, testCase.expectedUrlScheme, url.Scheme)
			prefix := testCase.expectedUrlScheme + "://hostname.local/" + path
			assert.True(t, strings.HasPrefix(url.String(), prefix), "%s should have a prefix of %s", url.String(), prefix)
			url = rs.getUploadUrl(md, "databasename", &remotesapi.TableFileDetails{
				Id:            id[:],
				Suffix:        ".darc",
				NumChunks:     64,
				SplitOffset:   64 * 1024,
				ContentLength: 64*1024 + 8*1024,
			})
			assert.Equal(t, testCase.expectedUrlScheme, url.Scheme)
			assert.True(t, strings.HasPrefix(url.String(), prefix), "%s should have a prefix of %s", url.String(), prefix)
		})
	}
}
