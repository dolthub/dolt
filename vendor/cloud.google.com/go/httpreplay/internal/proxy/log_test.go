// Copyright 2018 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build go1.8

package proxy

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"cloud.google.com/go/internal/testutil"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/martian"
)

func TestRedactHeaders(t *testing.T) {
	clone := func(h http.Header) http.Header {
		h2 := http.Header{}
		for k, v := range h {
			h2[k] = v
		}
		return h2
	}

	in := http.Header{
		"Content-Type":                      {"text/plain"},
		"Authorization":                     {"oauth2-token"},
		"X-Goog-Encryption-Key":             {"a-secret-key"},
		"X-Goog-Copy-Source-Encryption-Key": {"another-secret-key"},
	}
	orig := clone(in)
	got := redactHeaders(in)
	// Logged headers should be redacted.
	want := http.Header{
		"Content-Type":                      {"text/plain"},
		"Authorization":                     {"REDACTED"},
		"X-Goog-Encryption-Key":             {"REDACTED"},
		"X-Goog-Copy-Source-Encryption-Key": {"REDACTED"},
	}
	if !testutil.Equal(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
	// The original headers should be the same.
	if got, want := in, orig; !testutil.Equal(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
}

func TestLogger(t *testing.T) {
	req := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: "https",
			Host:   "example.com",
			Path:   "a/b/c",
		},
		Header:  http.Header{"H1": {"v1", "v2"}},
		Body:    ioutil.NopCloser(strings.NewReader("hello")),
		Trailer: http.Header{"T1": {"v3", "v4"}},
	}
	res := &http.Response{
		Request:    req,
		StatusCode: 204,
		Body:       ioutil.NopCloser(strings.NewReader("goodbye")),
		Header:     http.Header{"H2": {"v5"}},
		Trailer:    http.Header{"T2": {"v6", "v7"}},
	}
	l := NewLogger()
	_, remove, err := martian.TestContext(req, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer remove()
	if err := l.ModifyRequest(req); err != nil {
		t.Fatal(err)
	}
	if err := l.ModifyResponse(res); err != nil {
		t.Fatal(err)
	}
	lg := l.Extract()
	want := []*Entry{
		{
			ID: lg.Entries[0].ID,
			Request: &Request{
				Method:  "POST",
				URL:     "https://example.com/a/b/c",
				Header:  http.Header{"H1": {"v1", "v2"}},
				Body:    []byte("hello"),
				Trailer: http.Header{"T1": {"v3", "v4"}},
			},
			Response: &Response{
				StatusCode: 204,
				Body:       []byte("goodbye"),
				Header:     http.Header{"H2": {"v5"}},
				Trailer:    http.Header{"T2": {"v6", "v7"}},
			},
		},
	}
	if diff := testutil.Diff(lg.Entries, want); diff != "" {
		t.Error(diff)
	}
}

func TestToHTTPResponse(t *testing.T) {
	for _, test := range []struct {
		desc string
		lr   *Response
		req  *http.Request
		want *http.Response
	}{
		{
			desc: "GET request",
			lr: &Response{
				StatusCode: 201,
				Proto:      "1.1",
				Header:     http.Header{"h": {"v"}},
				Body:       []byte("text"),
			},
			req: &http.Request{Method: "GET"},
			want: &http.Response{
				Request:       &http.Request{Method: "GET"},
				StatusCode:    201,
				Proto:         "1.1",
				Header:        http.Header{"h": {"v"}},
				ContentLength: 4,
			},
		},
		{
			desc: "HEAD request with no Content-Length header",
			lr: &Response{
				StatusCode: 201,
				Proto:      "1.1",
				Header:     http.Header{"h": {"v"}},
				Body:       []byte("text"),
			},
			req: &http.Request{Method: "HEAD"},
			want: &http.Response{
				Request:       &http.Request{Method: "HEAD"},
				StatusCode:    201,
				Proto:         "1.1",
				Header:        http.Header{"h": {"v"}},
				ContentLength: -1,
			},
		},
		{
			desc: "HEAD request with Content-Length header",
			lr: &Response{
				StatusCode: 201,
				Proto:      "1.1",
				Header:     http.Header{"h": {"v"}, "Content-Length": {"17"}},
				Body:       []byte("text"),
			},
			req: &http.Request{Method: "HEAD"},
			want: &http.Response{
				Request:       &http.Request{Method: "HEAD"},
				StatusCode:    201,
				Proto:         "1.1",
				Header:        http.Header{"h": {"v"}, "Content-Length": {"17"}},
				ContentLength: 17,
			},
		},
	} {
		got := toHTTPResponse(test.lr, test.req)
		got.Body = nil
		if diff := testutil.Diff(got, test.want, cmpopts.IgnoreUnexported(http.Request{})); diff != "" {
			t.Errorf("%s: %s", test.desc, diff)
		}
	}
}
