// Copyright 2010 Gary Burd
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

// Package oauth is consumer interface for OAuth 1.0, OAuth 1.0a and RFC 5849.
//
// Redirection-based Authorization
//
// This section outlines how to use the oauth package in redirection-based
// authorization (http://tools.ietf.org/html/rfc5849#section-2).
//
// Step 1: Create a Client using credentials and URIs provided by the server.
// The Client can be initialized once at application startup and stored in a
// package-level variable.
//
// Step 2: Request temporary credentials using the Client
// RequestTemporaryCredentials method. The callbackURL parameter is the URL of
// the callback handler in step 4. Save the returned credential secret so that
// it can be later found using credential token as a key. The secret can be
// stored in a database keyed by the token. Another option is to store the
// token and secret in session storage or a cookie.
//
// Step 3: Redirect the user to URL returned from AuthorizationURL method. The
// AuthorizationURL method uses the temporary credentials from step 2 and other
// parameters as specified by the server.
//
// Step 4: The server redirects back to the callback URL specified in step 2
// with the temporary token and a verifier. Use the temporary token to find the
// temporary secret saved in step 2. Using the temporary token, temporary
// secret and verifier, request token credentials using the client RequestToken
// method. Save the returned credentials for later use in the application.
//
// Signing Requests
//
// The Client type has two low-level methods for signing requests, SignForm and
// SetAuthorizationHeader.
//
// The SignForm method adds an OAuth signature to a form. The application makes
// an authenticated request by encoding the modified form to the query string
// or request body.
//
// The SetAuthorizationHeader method adds an OAuth siganture to a request
// header. The SetAuthorizationHeader method is the only way to correctly sign
// a request if the application sets the URL Opaque field when making a
// request.
//
// The Get, Put, Post and Delete methods sign and invoke a request using the
// supplied net/http Client. These methods are easy to use, but not as flexible
// as constructing a request using one of the low-level methods.
package oauth // import "github.com/garyburd/go-oauth/oauth"

import (
	"bytes"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// noscape[b] is true if b should not be escaped per section 3.6 of the RFC.
var noEscape = [256]bool{
	'A': true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
	'a': true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
	'0': true, true, true, true, true, true, true, true, true, true,
	'-': true,
	'.': true,
	'_': true,
	'~': true,
}

// encode encodes string per section 3.6 of the RFC. If double is true, then
// the encoding is applied twice.
func encode(s string, double bool) []byte {
	// Compute size of result.
	m := 3
	if double {
		m = 5
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if noEscape[s[i]] {
			n++
		} else {
			n += m
		}
	}

	p := make([]byte, n)

	// Encode it.
	j := 0
	for i := 0; i < len(s); i++ {
		b := s[i]
		if noEscape[b] {
			p[j] = b
			j++
		} else if double {
			p[j] = '%'
			p[j+1] = '2'
			p[j+2] = '5'
			p[j+3] = "0123456789ABCDEF"[b>>4]
			p[j+4] = "0123456789ABCDEF"[b&15]
			j += 5
		} else {
			p[j] = '%'
			p[j+1] = "0123456789ABCDEF"[b>>4]
			p[j+2] = "0123456789ABCDEF"[b&15]
			j += 3
		}
	}
	return p
}

type keyValue struct{ key, value []byte }

type byKeyValue []keyValue

func (p byKeyValue) Len() int      { return len(p) }
func (p byKeyValue) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p byKeyValue) Less(i, j int) bool {
	sgn := bytes.Compare(p[i].key, p[j].key)
	if sgn == 0 {
		sgn = bytes.Compare(p[i].value, p[j].value)
	}
	return sgn < 0
}

func (p byKeyValue) appendValues(values url.Values) byKeyValue {
	for k, vs := range values {
		k := encode(k, true)
		for _, v := range vs {
			v := encode(v, true)
			p = append(p, keyValue{k, v})
		}
	}
	return p
}

// writeBaseString writes method, url, and params to w using the OAuth signature
// base string computation described in section 3.4.1 of the RFC.
func writeBaseString(w io.Writer, method string, u *url.URL, form url.Values, oauthParams map[string]string) {
	// Method
	w.Write(encode(strings.ToUpper(method), false))
	w.Write([]byte{'&'})

	// URL
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Host)

	uNoQuery := *u
	uNoQuery.RawQuery = ""
	path := uNoQuery.RequestURI()

	switch {
	case scheme == "http" && strings.HasSuffix(host, ":80"):
		host = host[:len(host)-len(":80")]
	case scheme == "https" && strings.HasSuffix(host, ":443"):
		host = host[:len(host)-len(":443")]
	}

	w.Write(encode(scheme, false))
	w.Write(encode("://", false))
	w.Write(encode(host, false))
	w.Write(encode(path, false))
	w.Write([]byte{'&'})

	// Create sorted slice of encoded parameters. Parameter keys and values are
	// double encoded in a single step. This is safe because double encoding
	// does not change the sort order.
	queryParams := u.Query()
	p := make(byKeyValue, 0, len(form)+len(queryParams)+len(oauthParams))
	p = p.appendValues(form)
	p = p.appendValues(queryParams)
	for k, v := range oauthParams {
		p = append(p, keyValue{encode(k, true), encode(v, true)})
	}
	sort.Sort(p)

	// Write the parameters.
	encodedAmp := encode("&", false)
	encodedEqual := encode("=", false)
	sep := false
	for _, kv := range p {
		if sep {
			w.Write(encodedAmp)
		} else {
			sep = true
		}
		w.Write(kv.key)
		w.Write(encodedEqual)
		w.Write(kv.value)
	}
}

var nonceCounter uint64

// nonce returns a unique string.
func nonce() string {
	n := atomic.AddUint64(&nonceCounter, 1)
	if n == 1 {
		binary.Read(rand.Reader, binary.BigEndian, &n)
		n ^= uint64(time.Now().UnixNano())
		atomic.CompareAndSwapUint64(&nonceCounter, 1, n)
	}
	return strconv.FormatUint(n, 16)
}

// SignatureMethod identifies a signature method.
type SignatureMethod int

func (sm SignatureMethod) String() string {
	switch sm {
	case RSASHA1:
		return "RSA-SHA1"
	case HMACSHA1:
		return "HMAC-SHA1"
	case PLAINTEXT:
		return "PLAINTEXT"
	default:
		return "unknown"
	}
}

const (
	HMACSHA1  SignatureMethod = iota // HMAC-SHA1
	RSASHA1                          // RSA-SHA1
	PLAINTEXT                        // Plain text
)

// Credentials represents client, temporary and token credentials.
type Credentials struct {
	Token  string // Also known as consumer key or access token.
	Secret string // Also known as consumer secret or access token secret.
}

// Client represents an OAuth client.
type Client struct {
	// Credentials specifies the client key and secret.
	// Also known as the consumer key and secret
	Credentials Credentials

	// TemporaryCredentialRequestURI is the endpoint used by the client to
	// obtain a set of temporary credentials. Also known as the request token
	// URL.
	TemporaryCredentialRequestURI string

	// ResourceOwnerAuthorizationURI is the endpoint to which the resource
	// owner is redirected to grant authorization. Also known as authorization
	// URL.
	ResourceOwnerAuthorizationURI string

	// TokenRequestURI is the endpoint used by the client to request a set of
	// token credentials using a set of temporary credentials. Also known as
	// access token URL.
	TokenRequestURI string

	// Header specifies optional extra headers for requests.
	Header http.Header

	// SignatureMethod specifies the method for signing a request.
	SignatureMethod SignatureMethod

	// PrivateKey is the private key to use for RSA-SHA1 signatures. This field
	// must be set for RSA-SHA1 signatures and ignored for other signature
	// methods.
	PrivateKey *rsa.PrivateKey
}

var testHook = func(map[string]string) {}

// oauthParams returns the OAuth request parameters for the given credentials,
// method, URL and application params. See
// http://tools.ietf.org/html/rfc5849#section-3.4 for more information about
// signatures.
func (c *Client) oauthParams(credentials *Credentials, method string, u *url.URL, form url.Values) (map[string]string, error) {
	oauthParams := map[string]string{
		"oauth_consumer_key":     c.Credentials.Token,
		"oauth_signature_method": c.SignatureMethod.String(),
		"oauth_version":          "1.0",
	}

	if c.SignatureMethod != PLAINTEXT {
		oauthParams["oauth_timestamp"] = strconv.FormatInt(time.Now().Unix(), 10)
		oauthParams["oauth_nonce"] = nonce()
	}

	if credentials != nil {
		oauthParams["oauth_token"] = credentials.Token
	}

	testHook(oauthParams)

	var signature string

	switch c.SignatureMethod {
	case HMACSHA1:
		key := encode(c.Credentials.Secret, false)
		key = append(key, '&')
		if credentials != nil {
			key = append(key, encode(credentials.Secret, false)...)
		}
		h := hmac.New(sha1.New, key)
		writeBaseString(h, method, u, form, oauthParams)
		signature = base64.StdEncoding.EncodeToString(h.Sum(key[:0]))
	case RSASHA1:
		if c.PrivateKey == nil {
			return nil, errors.New("oauth: private key not set")
		}
		h := sha1.New()
		writeBaseString(h, method, u, form, oauthParams)
		rawSignature, err := rsa.SignPKCS1v15(rand.Reader, c.PrivateKey, crypto.SHA1, h.Sum(nil))
		if err != nil {
			return nil, err
		}
		signature = base64.StdEncoding.EncodeToString(rawSignature)
	case PLAINTEXT:
		rawSignature := encode(c.Credentials.Secret, false)
		rawSignature = append(rawSignature, '&')
		if credentials != nil {
			rawSignature = append(rawSignature, encode(credentials.Secret, false)...)
		}
		signature = string(rawSignature)
	default:
		return nil, errors.New("oauth: unknown signature method")
	}

	oauthParams["oauth_signature"] = signature
	return oauthParams, nil
}

// SignForm adds an OAuth signature to form. The urlStr argument must not
// include a query string.
//
// See http://tools.ietf.org/html/rfc5849#section-3.5.2 for
// information about transmitting OAuth parameters in a request body and
// http://tools.ietf.org/html/rfc5849#section-3.5.2 for information about
// transmitting OAuth parameters in a query string.
func (c *Client) SignForm(credentials *Credentials, method, urlStr string, form url.Values) error {
	u, err := url.Parse(urlStr)
	switch {
	case err != nil:
		return err
	case u.RawQuery != "":
		return errors.New("oauth: urlStr argument to SignForm must not include a query string")
	}
	p, err := c.oauthParams(credentials, method, u, form)
	if err != nil {
		return err
	}
	for k, v := range p {
		form.Set(k, v)
	}
	return nil
}

// SignParam is deprecated. Use SignForm instead.
func (c *Client) SignParam(credentials *Credentials, method, urlStr string, params url.Values) {
	u, _ := url.Parse(urlStr)
	u.RawQuery = ""
	p, _ := c.oauthParams(credentials, method, u, params)
	for k, v := range p {
		params.Set(k, v)
	}
}

var oauthKeys = []string{
	"oauth_consumer_key",
	"oauth_nonce",
	"oauth_signature",
	"oauth_signature_method",
	"oauth_timestamp",
	"oauth_token",
	"oauth_version",
}

func (c *Client) authorizationHeader(credentials *Credentials, method string, u *url.URL, params url.Values) (string, error) {
	p, err := c.oauthParams(credentials, method, u, params)
	if err != nil {
		return "", err
	}
	var h []byte
	// Append parameters in a fixed order to support testing.
	for _, k := range oauthKeys {
		if v, ok := p[k]; ok {
			if h == nil {
				h = []byte(`OAuth `)
			} else {
				h = append(h, ", "...)
			}
			h = append(h, k...)
			h = append(h, `="`...)
			h = append(h, encode(v, false)...)
			h = append(h, '"')
		}
	}
	return string(h), nil
}

// AuthorizationHeader returns the HTTP authorization header value for given
// method, URL and parameters.
//
// AuthorizationHeader is deprecated. Use SetAuthorizationHeader instead.
func (c *Client) AuthorizationHeader(credentials *Credentials, method string, u *url.URL, params url.Values) string {
	// Signing a request can return an error. This method is deprecated because
	// this method does not return an error.
	v, _ := c.authorizationHeader(credentials, method, u, params)
	return v
}

// SetAuthorizationHeader adds an OAuth signature to a request header.
//
// See http://tools.ietf.org/html/rfc5849#section-3.5.1 for information about
// transmitting OAuth parameters in an HTTP request header.
func (c *Client) SetAuthorizationHeader(header http.Header, credentials *Credentials, method string, u *url.URL, params url.Values) error {
	v, err := c.authorizationHeader(credentials, method, u, params)
	if err != nil {
		return err
	}
	header.Set("Authorization", v)
	return nil
}

// Get issues a GET to the specified URL with form added as a query string.
func (c *Client) Get(client *http.Client, credentials *Credentials, urlStr string, form url.Values) (*http.Response, error) {
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	if req.URL.RawQuery != "" {
		return nil, errors.New("oauth: url must not contain a query string")
	}
	for k, v := range c.Header {
		req.Header[k] = v
	}
	if err := c.SetAuthorizationHeader(req.Header, credentials, "GET", req.URL, form); err != nil {
		return nil, err
	}
	req.URL.RawQuery = form.Encode()
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

func (c *Client) do(client *http.Client, method string, credentials *Credentials, urlStr string, form url.Values) (*http.Response, error) {
	req, err := http.NewRequest(method, urlStr, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	for k, v := range c.Header {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := c.SetAuthorizationHeader(req.Header, credentials, method, req.URL, form); err != nil {
		return nil, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

// Post issues a POST with the specified form.
func (c *Client) Post(client *http.Client, credentials *Credentials, urlStr string, form url.Values) (*http.Response, error) {
	return c.do(client, "POST", credentials, urlStr, form)
}

// Delete issues a DELETE with the specified form.
func (c *Client) Delete(client *http.Client, credentials *Credentials, urlStr string, form url.Values) (*http.Response, error) {
	return c.do(client, "DELETE", credentials, urlStr, form)
}

// Put issues a PUT with the specified form.
func (c *Client) Put(client *http.Client, credentials *Credentials, urlStr string, form url.Values) (*http.Response, error) {
	return c.do(client, "PUT", credentials, urlStr, form)
}

func (c *Client) requestCredentials(client *http.Client, credentials *Credentials, urlStr string, form url.Values) (*Credentials, url.Values, error) {
	resp, err := c.Post(client, credentials, urlStr, form)
	if err != nil {
		return nil, nil, err
	}
	p, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return nil, nil, fmt.Errorf("OAuth server status %d, %s", resp.StatusCode, string(p))
	}
	m, err := url.ParseQuery(string(p))
	if err != nil {
		return nil, nil, err
	}
	tokens := m["oauth_token"]
	if len(tokens) == 0 || tokens[0] == "" {
		return nil, nil, errors.New("oauth: token missing from server result")
	}
	secrets := m["oauth_token_secret"]
	if len(secrets) == 0 { // allow "" as a valid secret.
		return nil, nil, errors.New("oauth: secret missing from server result")
	}
	return &Credentials{Token: tokens[0], Secret: secrets[0]}, m, nil
}

// RequestTemporaryCredentials requests temporary credentials from the server.
// See http://tools.ietf.org/html/rfc5849#section-2.1 for information about
// temporary credentials.
func (c *Client) RequestTemporaryCredentials(client *http.Client, callbackURL string, additionalParams url.Values) (*Credentials, error) {
	params := make(url.Values)
	for k, vs := range additionalParams {
		params[k] = vs
	}
	if callbackURL != "" {
		params.Set("oauth_callback", callbackURL)
	}
	credentials, _, err := c.requestCredentials(client, nil, c.TemporaryCredentialRequestURI, params)
	return credentials, err
}

// RequestToken requests token credentials from the server. See
// http://tools.ietf.org/html/rfc5849#section-2.3 for information about token
// credentials.
func (c *Client) RequestToken(client *http.Client, temporaryCredentials *Credentials, verifier string) (*Credentials, url.Values, error) {
	params := make(url.Values)
	if verifier != "" {
		params.Set("oauth_verifier", verifier)
	}
	credentials, vals, err := c.requestCredentials(client, temporaryCredentials, c.TokenRequestURI, params)
	if err != nil {
		return nil, nil, err
	}
	return credentials, vals, nil
}

// AuthorizationURL returns the URL for resource owner authorization. See
// http://tools.ietf.org/html/rfc5849#section-2.2 for information about
// resource owner authorization.
func (c *Client) AuthorizationURL(temporaryCredentials *Credentials, additionalParams url.Values) string {
	params := make(url.Values)
	for k, vs := range additionalParams {
		params[k] = vs
	}
	params.Set("oauth_token", temporaryCredentials.Token)
	return c.ResourceOwnerAuthorizationURI + "?" + params.Encode()
}
