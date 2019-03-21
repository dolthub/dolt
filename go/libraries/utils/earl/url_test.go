package earl

import (
	"net/url"
	"reflect"
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
	}

	for _, test := range tests {
		actualUrl, err := Parse(test.urlStr)

		if (err != nil) != test.expectErr {
			t.Error("input:", test.urlStr, "got error:", err != nil, "expected error:", test.expectErr, "result:", actualUrl, "err:", err)
		} else if err == nil && !reflect.DeepEqual(actualUrl, &test.expectedUrl) {
			t.Error(actualUrl, "!=", &test.expectedUrl)
		}
	}
}
