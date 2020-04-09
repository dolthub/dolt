// Copyright 2019 Liquidata, Inc.
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

package earl

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		urlStr      string
		expectedUrl url.URL
		expectErr   bool
	}{
		{
			"",
			url.URL{
				Path: "/",
			},
			false,
		},
		{
			"http://test.com",
			url.URL{
				Scheme: "http",
				Host:   "test.com",
				Path:   "/",
			},
			false,
		},
		{
			"test.com",
			url.URL{
				Host: "test.com",
				Path: "/",
			},
			false,
		},
		{
			"http://127.0.0.1/",
			url.URL{
				Scheme: "http",
				Host:   "127.0.0.1",
				Path:   "/",
			},
			false,
		},
		{
			"127.0.0.1",
			url.URL{
				Host: "127.0.0.1",
				Path: "/",
			},
			false,
		},
		{
			"ftp://test.com/path1/file.name",
			url.URL{
				Scheme: "ftp",
				Host:   "test.com",
				Path:   "/path1/file.name",
			},
			false,
		},
		{
			"test.com/path1/file.name",
			url.URL{
				Host: "test.com",
				Path: "/path1/file.name",
			},
			false,
		},
		{
			"path1/path2",
			url.URL{
				Path: "/path1/path2",
			},
			false,
		},
		{
			"localhost/path1/path2",
			url.URL{
				Host: "localhost",
				Path: "/path1/path2",
			},
			false,
		},
		{
			"http://localhost/path1/path2",
			url.URL{
				Scheme: "http",
				Host:   "localhost",
				Path:   "/path1/path2",
			},
			false,
		},
		{
			"http://localhost:50051/path1/path2",
			url.URL{
				Scheme: "http",
				Host:   "localhost:50051",
				Path:   "/path1/path2",
			},
			false,
		},
		{
			"localhost:50051/path1/path2",
			url.URL{
				Host: "localhost:50051",
				Path: "/path1/path2",
			},
			false,
		},
		{
			"user:pass@place.org/path1/path2",
			url.URL{
				Host: "place.org",
				Path: "/path1/path2",
				User: url.UserPassword("user", "pass"),
			},
			false,
		},
		{
			"https://user:pass@place.org/path1/path2",
			url.URL{
				Scheme: "https",
				Host:   "place.org",
				Path:   "/path1/path2",
				User:   url.UserPassword("user", "pass"),
			},
			false,
		},
		{
			"user:pass@localhost/path1/path2",
			url.URL{
				Host: "localhost",
				Path: "/path1/path2",
				User: url.UserPassword("user", "pass"),
			},
			false,
		},
		{
			"https://user:pass@localhost/path1/path2",
			url.URL{
				Scheme: "https",
				Host:   "localhost",
				Path:   "/path1/path2",
				User:   url.UserPassword("user", "pass"),
			},
			false,
		},
		{
			"http://test.com:8080",
			url.URL{
				Scheme: "http",
				Host:   "test.com:8080",
				Path:   "/",
			},
			false,
		},
		{
			"test.com:8080/",
			url.URL{
				Host: "test.com:8080",
				Path: "/",
			},
			false,
		},
		{
			"ftp://user:pass@test.com:8080/path/file.name",
			url.URL{
				Scheme: "ftp",
				Host:   "test.com:8080",
				Path:   "/path/file.name",
				User:   url.UserPassword("user", "pass"),
			},
			false,
		},
		{
			"user:pass@test.com:8080/path/file.name",
			url.URL{
				Host: "test.com:8080",
				Path: "/path/file.name",
				User: url.UserPassword("user", "pass"),
			},
			false,
		},
		{
			"file:///C:/Users/name/datasets",
			url.URL{
				Scheme: "file",
				Path:   "C:/Users/name/datasets",
			},
			false,
		},
		{
			`file:///C:\Users\name\datasets`,
			url.URL{
				Scheme: "file",
				Path:   "C:/Users/name/datasets",
			},
			false,
		},
		{
			"file://localhost/C$/Users/name/datasets",
			url.URL{
				Scheme: "file",
				Host:   "localhost",
				Path:   "C:/Users/name/datasets",
			},
			false,
		},
		{
			FileUrlFromPath(`C:\Users\name\datasets`, '\\'),
			url.URL{
				Scheme: "file",
				Path:   "C:/Users/name/datasets",
			},
			false,
		},
		{
			FileUrlFromPath(`./.dolt/noms`, '/'),
			url.URL{
				Scheme: "file",
				Path:   "./.dolt/noms",
			},
			false,
		},
		{
			FileUrlFromPath(`./.dolt\noms`, '\\'),
			url.URL{
				Scheme: "file",
				Path:   "./.dolt/noms",
			},
			false,
		},
		{
			FileUrlFromPath(`.dolt/noms`, '/'),
			url.URL{
				Scheme: "file",
				Path:   ".dolt/noms",
			},
			false,
		},
		{
			FileUrlFromPath(`.dolt\noms`, '\\'),
			url.URL{
				Scheme: "file",
				Path:   ".dolt/noms",
			},
			false,
		},
	}

	for _, test := range tests {
		actualUrl, err := Parse(test.urlStr)

		if (err != nil) != test.expectErr {
			t.Error("input:", test.urlStr, "got error:", err != nil, "expected error:", test.expectErr, "result:", actualUrl, "err:", err)
		} else if err == nil {
			assert.Equal(t, &test.expectedUrl, actualUrl)
		}
	}
}
