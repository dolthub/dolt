// Copyright 2013 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package oauth_test

import (
	"github.com/garyburd/go-oauth/oauth"
	"net/http"
	"net/url"
	"strings"
)

// This example shows how to sign a request when the URL Opaque field is used.
// See the note at http://golang.org/pkg/net/url/#URL for information on the
// use of the URL Opaque field.
func ExampleClient_SetAuthorizationHeader(client *oauth.Client, credentials *oauth.Credentials) error {
	form := url.Values{"maxResults": {"100"}}

	// The last element of path contains a "/".
	path := "/document/encoding%2gizp"

	// Create the request with the temporary path "/".
	req, err := http.NewRequest("GET", "http://api.example.com/", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}

	// Overwrite the temporary path with the actual request path.
	req.URL.Opaque = path

	// Sign the request.
	if err := client.SetAuthorizationHeader(req.Header, credentials, "GET", req.URL, form); err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// process the response
	return nil
}
