// Copyright 2018 Google LLC
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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"reflect"
	"strings"
	"sync"

	"github.com/google/martian/martianlog"
)

// ForReplaying returns a Proxy configured to replay.
func ForReplaying(filename string, port int) (*Proxy, error) {
	p, err := newProxy(filename)
	if err != nil {
		return nil, err
	}
	calls, initial, err := readLog(filename)
	if err != nil {
		return nil, err
	}
	p.mproxy.SetRoundTripper(&replayRoundTripper{
		calls:         calls,
		ignoreHeaders: p.ignoreHeaders,
	})
	p.Initial = initial

	// Debug logging.
	// TODO(jba): factor out from here and ForRecording.
	logger := martianlog.NewLogger()
	logger.SetDecode(true)
	p.mproxy.SetRequestModifier(logger)
	p.mproxy.SetResponseModifier(logger)

	if err := p.start(port); err != nil {
		return nil, err
	}
	return p, nil
}

// A call is an HTTP request and its matching response.
type call struct {
	req     *Request
	reqBody *requestBody // parsed request body
	res     *Response
}

func readLog(filename string) ([]*call, []byte, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}
	var lg Log
	if err := json.Unmarshal(bytes, &lg); err != nil {
		return nil, nil, fmt.Errorf("%s: %v", filename, err)
	}
	if lg.Version != LogVersion {
		return nil, nil, fmt.Errorf("httpreplay proxy: read log version %s but current version is %s",
			lg.Version, LogVersion)
	}
	ignoreIDs := map[string]bool{} // IDs of requests to ignore
	callsByID := map[string]*call{}
	var calls []*call
	for _, e := range lg.Entries {
		if ignoreIDs[e.ID] {
			continue
		}
		c, ok := callsByID[e.ID]
		switch {
		case !ok:
			if e.Request == nil {
				return nil, nil, fmt.Errorf("first entry for ID %s does not have a request", e.ID)
			}
			if e.Request.Method == "CONNECT" {
				// Ignore CONNECT methods.
				ignoreIDs[e.ID] = true
			} else {
				reqBody, err := newRequestBodyFromLog(e.Request)
				if err != nil {
					return nil, nil, err
				}
				c := &call{e.Request, reqBody, e.Response}
				calls = append(calls, c)
				callsByID[e.ID] = c
			}
		case e.Request != nil:
			if e.Response != nil {
				return nil, nil, errors.New("HAR entry has both request and response")
			}
			c.req = e.Request
		case e.Response != nil:
			c.res = e.Response
		default:
			return nil, nil, errors.New("HAR entry has neither request nor response")
		}
	}
	for _, c := range calls {
		if c.req == nil || c.res == nil {
			return nil, nil, fmt.Errorf("missing request or response: %+v", c)
		}
	}
	return calls, lg.Initial, nil
}

type replayRoundTripper struct {
	mu            sync.Mutex
	calls         []*call
	ignoreHeaders map[string]bool
}

func (r *replayRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	reqBody, err := newRequestBodyFromHTTP(req)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, call := range r.calls {
		if call == nil {
			continue
		}
		if requestsMatch(req, reqBody, call.req, call.reqBody, r.ignoreHeaders) {
			r.calls[i] = nil // nil out this call so we don't reuse it
			return toHTTPResponse(call.res, req), nil
		}
	}
	return nil, fmt.Errorf("no matching request for %+v", req)
}

// Headers that shouldn't be compared, because they may differ on different executions
// of the same code, or may not be present during record or replay.
var ignoreHeaders = map[string]bool{}

func init() {
	// Sensitive headers are redacted in the log, so they won't be equal to incoming values.
	for h := range sensitiveHeaders {
		ignoreHeaders[h] = true
	}
	for _, h := range []string{
		"Content-Type", // handled by requestBody
		"Connection",
		"Date",
		"Host",
		"Transfer-Encoding",
		"Via",
		"X-Forwarded-For",
		"X-Forwarded-Host",
		"X-Forwarded-Proto",
		"X-Forwarded-Url",
		"X-Cloud-Trace-Context", // OpenCensus traces have a random ID
		"X-Goog-Api-Client",     // can differ for, e.g., different Go versions
	} {
		ignoreHeaders[h] = true
	}
}

// Report whether the incoming request in matches the candidate request cand.
func requestsMatch(in *http.Request, inBody *requestBody, cand *Request, candBody *requestBody, ignoreHeaders map[string]bool) bool {
	if in.Method != cand.Method {
		return false
	}
	if in.URL.String() != cand.URL {
		return false
	}
	if !inBody.equal(candBody) {
		return false
	}
	// Check headers last. See DebugHeaders.
	return headersMatch(in.Header, cand.Header, ignoreHeaders)
}

// A requestBody represents the body of a request. If the content type is multipart, the
// body is split into parts.
//
// The replaying proxy needs to understand multipart bodies because the boundaries are
// generated randomly, so we can't just compare the entire bodies for equality.
type requestBody struct {
	mediaType string   // the media type part of the Content-Type header
	parts     [][]byte // the parts of the body, or just a single []byte if not multipart
}

func newRequestBodyFromHTTP(req *http.Request) (*requestBody, error) {
	defer req.Body.Close()
	return newRequestBody(req.Header.Get("Content-Type"), req.Body)
}

func newRequestBodyFromLog(req *Request) (*requestBody, error) {
	if req.Body == nil {
		return nil, nil
	}
	return newRequestBody(req.Header.Get("Content-Type"), bytes.NewReader(req.Body))
}

// newRequestBody parses the Content-Type header, reads the body, and splits it into
// parts if necessary.
func newRequestBody(contentType string, body io.Reader) (*requestBody, error) {
	if contentType == "" {
		// No content-type header. There should not be a body.
		if _, err := body.Read(make([]byte, 1)); err != io.EOF {
			return nil, errors.New("no Content-Type, but body")
		}
		return nil, nil
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}
	rb := &requestBody{mediaType: mediaType}
	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			part, err := ioutil.ReadAll(p)
			if err != nil {
				return nil, err
			}
			// TODO(jba): care about part headers?
			rb.parts = append(rb.parts, part)
		}
	} else {
		bytes, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, err
		}
		rb.parts = [][]byte{bytes}
	}
	return rb, nil
}

func (r1 *requestBody) equal(r2 *requestBody) bool {
	if r1 == nil || r2 == nil {
		return r1 == r2
	}
	if r1.mediaType != r2.mediaType {
		return false
	}
	if len(r1.parts) != len(r2.parts) {
		return false
	}
	for i, p1 := range r1.parts {
		if !bytes.Equal(p1, r2.parts[i]) {
			return false
		}
	}
	return true
}

// DebugHeaders helps to determine whether a header should be ignored.
// When true, if requests have the same method, URL and body but differ
// in a header, the first mismatched header is logged.
var DebugHeaders = false

func headersMatch(in, cand http.Header, ignores map[string]bool) bool {
	for k1, v1 := range in {
		if ignores[k1] {
			continue
		}
		v2 := cand[k1]
		if v2 == nil {
			if DebugHeaders {
				log.Printf("header %s: present in incoming request but not candidate", k1)
			}
			return false
		}
		if !reflect.DeepEqual(v1, v2) {
			if DebugHeaders {
				log.Printf("header %s: incoming %v, candidate %v", k1, v1, v2)
			}
			return false
		}
	}
	for k2 := range cand {
		if ignores[k2] {
			continue
		}
		if in[k2] == nil {
			if DebugHeaders {
				log.Printf("header %s: not in incoming request but present in candidate", k2)
			}
			return false
		}
	}
	return true
}
