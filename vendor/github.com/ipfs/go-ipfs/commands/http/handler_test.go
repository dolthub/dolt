package http

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	cmds "github.com/ipfs/go-ipfs/commands"
	ipfscmd "github.com/ipfs/go-ipfs/core/commands"
	coremock "github.com/ipfs/go-ipfs/core/mock"
)

func assertHeaders(t *testing.T, resHeaders http.Header, reqHeaders map[string]string) {
	for name, value := range reqHeaders {
		if resHeaders.Get(name) != value {
			t.Errorf("Invalid header '%s', wanted '%s', got '%s'", name, value, resHeaders.Get(name))
		}
	}
}

func assertStatus(t *testing.T, actual, expected int) {
	if actual != expected {
		t.Errorf("Expected status: %d got: %d", expected, actual)
	}
}

func originCfg(origins []string) *ServerConfig {
	cfg := NewServerConfig()
	cfg.SetAllowedOrigins(origins...)
	cfg.SetAllowedMethods("GET", "PUT", "POST")
	return cfg
}

type testCase struct {
	Method       string
	Path         string
	Code         int
	Origin       string
	Referer      string
	AllowOrigins []string
	ReqHeaders   map[string]string
	ResHeaders   map[string]string
}

var defaultOrigins = []string{
	"http://localhost",
	"http://127.0.0.1",
	"https://localhost",
	"https://127.0.0.1",
}

func getTestServer(t *testing.T, origins []string) *httptest.Server {
	cmdsCtx, err := coremock.MockCmdsCtx()
	if err != nil {
		t.Error("failure to initialize mock cmds ctx", err)
		return nil
	}

	cmdRoot := &cmds.Command{
		Subcommands: map[string]*cmds.Command{
			"version": ipfscmd.VersionCmd,
		},
	}

	if len(origins) == 0 {
		origins = defaultOrigins
	}

	handler := NewHandler(cmdsCtx, cmdRoot, originCfg(origins))
	return httptest.NewServer(handler)
}

func (tc *testCase) test(t *testing.T) {
	// defaults
	method := tc.Method
	if method == "" {
		method = "GET"
	}

	path := tc.Path
	if path == "" {
		path = "/api/v0/version"
	}

	expectCode := tc.Code
	if expectCode == 0 {
		expectCode = 200
	}

	// request
	req, err := http.NewRequest(method, path, nil)
	if err != nil {
		t.Error(err)
		return
	}

	for k, v := range tc.ReqHeaders {
		req.Header.Add(k, v)
	}
	if tc.Origin != "" {
		req.Header.Add("Origin", tc.Origin)
	}
	if tc.Referer != "" {
		req.Header.Add("Referer", tc.Referer)
	}

	// server
	server := getTestServer(t, tc.AllowOrigins)
	if server == nil {
		return
	}
	defer server.Close()

	req.URL, err = url.Parse(server.URL + path)
	if err != nil {
		t.Error(err)
		return
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return
	}

	// checks
	t.Log("GET", server.URL+path, req.Header, res.Header)
	assertHeaders(t, res.Header, tc.ResHeaders)
	assertStatus(t, res.StatusCode, expectCode)
}

func TestDisallowedOrigins(t *testing.T) {
	gtc := func(origin string, allowedOrigins []string) testCase {
		return testCase{
			Origin:       origin,
			AllowOrigins: allowedOrigins,
			ResHeaders: map[string]string{
				ACAOrigin:                       "",
				ACAMethods:                      "",
				ACACredentials:                  "",
				"Access-Control-Max-Age":        "",
				"Access-Control-Expose-Headers": "",
			},
			Code: http.StatusForbidden,
		}
	}

	tcs := []testCase{
		gtc("http://barbaz.com", nil),
		gtc("http://barbaz.com", []string{"http://localhost"}),
		gtc("http://127.0.0.1", []string{"http://localhost"}),
		gtc("http://localhost", []string{"http://127.0.0.1"}),
		gtc("http://127.0.0.1:1234", nil),
		gtc("http://localhost:1234", nil),
	}

	for _, tc := range tcs {
		tc.test(t)
	}
}

func TestAllowedOrigins(t *testing.T) {
	gtc := func(origin string, allowedOrigins []string) testCase {
		return testCase{
			Origin:       origin,
			AllowOrigins: allowedOrigins,
			ResHeaders: map[string]string{
				ACAOrigin:                       origin,
				ACAMethods:                      "",
				ACACredentials:                  "",
				"Access-Control-Max-Age":        "",
				"Access-Control-Expose-Headers": AllowedExposedHeaders,
			},
			Code: http.StatusOK,
		}
	}

	tcs := []testCase{
		gtc("http://barbaz.com", []string{"http://barbaz.com", "http://localhost"}),
		gtc("http://localhost", []string{"http://barbaz.com", "http://localhost"}),
		gtc("http://localhost", nil),
		gtc("http://127.0.0.1", nil),
	}

	for _, tc := range tcs {
		tc.test(t)
	}
}

func TestWildcardOrigin(t *testing.T) {
	gtc := func(origin string, allowedOrigins []string) testCase {
		return testCase{
			Origin:       origin,
			AllowOrigins: allowedOrigins,
			ResHeaders: map[string]string{
				ACAOrigin:                       origin,
				ACAMethods:                      "",
				ACACredentials:                  "",
				"Access-Control-Max-Age":        "",
				"Access-Control-Expose-Headers": AllowedExposedHeaders,
			},
			Code: http.StatusOK,
		}
	}

	tcs := []testCase{
		gtc("http://barbaz.com", []string{"*"}),
		gtc("http://barbaz.com", []string{"http://localhost", "*"}),
		gtc("http://127.0.0.1", []string{"http://localhost", "*"}),
		gtc("http://localhost", []string{"http://127.0.0.1", "*"}),
		gtc("http://127.0.0.1", []string{"*"}),
		gtc("http://localhost", []string{"*"}),
		gtc("http://127.0.0.1:1234", []string{"*"}),
		gtc("http://localhost:1234", []string{"*"}),
	}

	for _, tc := range tcs {
		tc.test(t)
	}
}

func TestDisallowedReferer(t *testing.T) {
	gtc := func(referer string, allowedOrigins []string) testCase {
		return testCase{
			Origin:       "http://localhost",
			Referer:      referer,
			AllowOrigins: allowedOrigins,
			ResHeaders: map[string]string{
				ACAOrigin:                       "http://localhost",
				ACAMethods:                      "",
				ACACredentials:                  "",
				"Access-Control-Max-Age":        "",
				"Access-Control-Expose-Headers": "",
			},
			Code: http.StatusForbidden,
		}
	}

	tcs := []testCase{
		gtc("http://foobar.com", nil),
		gtc("http://localhost:1234", nil),
		gtc("http://127.0.0.1:1234", nil),
	}

	for _, tc := range tcs {
		tc.test(t)
	}
}

func TestAllowedReferer(t *testing.T) {
	gtc := func(referer string, allowedOrigins []string) testCase {
		return testCase{
			Origin:       "http://localhost",
			AllowOrigins: allowedOrigins,
			ResHeaders: map[string]string{
				ACAOrigin:                       "http://localhost",
				ACAMethods:                      "",
				ACACredentials:                  "",
				"Access-Control-Max-Age":        "",
				"Access-Control-Expose-Headers": AllowedExposedHeaders,
			},
			Code: http.StatusOK,
		}
	}

	tcs := []testCase{
		gtc("http://barbaz.com", []string{"http://barbaz.com", "http://localhost"}),
		gtc("http://localhost", []string{"http://barbaz.com", "http://localhost"}),
		gtc("http://localhost", nil),
		gtc("http://127.0.0.1", nil),
	}

	for _, tc := range tcs {
		tc.test(t)
	}
}

func TestWildcardReferer(t *testing.T) {
	gtc := func(origin string, allowedOrigins []string) testCase {
		return testCase{
			Origin:       origin,
			AllowOrigins: allowedOrigins,
			ResHeaders: map[string]string{
				ACAOrigin:                       origin,
				ACAMethods:                      "",
				ACACredentials:                  "",
				"Access-Control-Max-Age":        "",
				"Access-Control-Expose-Headers": AllowedExposedHeaders,
			},
			Code: http.StatusOK,
		}
	}

	tcs := []testCase{
		gtc("http://barbaz.com", []string{"*"}),
		gtc("http://barbaz.com", []string{"http://localhost", "*"}),
		gtc("http://127.0.0.1", []string{"http://localhost", "*"}),
		gtc("http://localhost", []string{"http://127.0.0.1", "*"}),
		gtc("http://127.0.0.1", []string{"*"}),
		gtc("http://localhost", []string{"*"}),
		gtc("http://127.0.0.1:1234", []string{"*"}),
		gtc("http://localhost:1234", []string{"*"}),
	}

	for _, tc := range tcs {
		tc.test(t)
	}
}

func TestAllowedMethod(t *testing.T) {
	gtc := func(method string, ok bool) testCase {
		code := http.StatusOK
		hdrs := map[string]string{
			ACAOrigin:                       "http://localhost",
			ACAMethods:                      method,
			ACACredentials:                  "",
			"Access-Control-Max-Age":        "",
			"Access-Control-Expose-Headers": "",
		}

		if !ok {
			hdrs[ACAOrigin] = ""
			hdrs[ACAMethods] = ""
		}

		return testCase{
			Method:       "OPTIONS",
			Origin:       "http://localhost",
			AllowOrigins: []string{"*"},
			ReqHeaders: map[string]string{
				"Access-Control-Request-Method": method,
			},
			ResHeaders: hdrs,
			Code:       code,
		}
	}

	tcs := []testCase{
		gtc("PUT", true),
		gtc("GET", true),
		gtc("FOOBAR", false),
	}

	for _, tc := range tcs {
		tc.test(t)
	}
}
