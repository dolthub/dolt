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

package dbfactory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/earl"
)

func TestParseS3Url(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		bucket  string
		prefix  string
		routing s3Routing
	}{
		{
			name:   "bucket and database only",
			url:    "s3://bucket/db",
			bucket: "bucket",
			prefix: "/db",
		},
		{
			name:   "nested prefix",
			url:    "s3://bucket/team/db",
			bucket: "bucket",
			prefix: "/team/db",
		},
		{
			name:    "r2 style endpoint and region",
			url:     "s3://bucket/db?endpoint=https://account.r2.cloudflarestorage.com&region=auto",
			bucket:  "bucket",
			prefix:  "/db",
			routing: s3Routing{endpoint: "https://account.r2.cloudflarestorage.com", region: "auto"},
		},
		{
			name:    "minio style path addressing",
			url:     "s3://bucket/db?endpoint=http://localhost:9000&path-style=true",
			bucket:  "bucket",
			prefix:  "/db",
			routing: s3Routing{endpoint: "http://localhost:9000", pathStyle: true},
		},
		{
			name:    "path style can be turned off explicitly",
			url:     "s3://bucket/db?path-style=false",
			bucket:  "bucket",
			prefix:  "/db",
			routing: s3Routing{pathStyle: false},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			urlObj, err := earl.Parse(test.url)
			require.NoError(t, err)

			bucket, prefix, routing, err := parseS3Url(urlObj)
			require.NoError(t, err)
			assert.Equal(t, test.bucket, bucket)
			assert.Equal(t, test.prefix, prefix)
			assert.Equal(t, test.routing, routing)
		})
	}
}

func TestParseS3UrlErrors(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		errLike string
	}{
		{
			name:    "no bucket",
			url:     "s3:///db",
			errLike: "s3://bucket/path",
		},
		{
			name:    "credentials in the url are refused",
			url:     "s3://AKID:secret@bucket/db",
			errLike: "must not embed credentials",
		},
		{
			name:    "unknown parameter",
			url:     "s3://bucket/db?entpoint=https://example.com",
			errLike: "unknown s3 url parameter",
		},
		{
			name:    "path-style must be a bool",
			url:     "s3://bucket/db?path-style=yes",
			errLike: "must be true or false",
		},
		{
			name:    "empty value",
			url:     "s3://bucket/db?endpoint=",
			errLike: "needs exactly one non-empty value",
		},
		{
			name:    "repeated parameter is ambiguous",
			url:     "s3://bucket/db?region=auto&region=us-east-1",
			errLike: "needs exactly one non-empty value",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateS3Url(test.url)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errLike)
		})
	}
}

// A url that survives validation must survive the round trip through the
// remote's stored url string, since that is how routing is persisted.
func TestValidateS3UrlAcceptsRoutedUrls(t *testing.T) {
	urls := []string{
		"s3://bucket/db",
		"s3://bucket/db?region=auto",
		"s3://bucket/db?endpoint=https://account.r2.cloudflarestorage.com&region=auto&path-style=true",
	}

	for _, u := range urls {
		assert.NoError(t, ValidateS3Url(u), u)
	}
}
