package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	driver "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/sql_server_driver"

	"github.com/stretchr/testify/require"
	"gopkg.in/go-jose/go-jose.v2"
)

type getConfigFunc func(serverPort, metricsPort int) string

// runJWKSServer starts a local HTTP server that serves the JWKS file at /.well-known/jwks.json.
// The server is started in a goroutine and will be shut down via t.Cleanup.
func runJWKSServer(t *testing.T, jwksFilePath string, port int) {
	data, err := os.ReadFile(jwksFilePath)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	})

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			// Use t.Log instead of t.Fatal because Serve runs in goroutine
			t.Logf("runJWKSServer: server error: %v", err)
		}
	}()

	t.Logf("Started test JWKS server on %s serving %s", addr, jwksFilePath)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Logf("runJWKSServer: shutdown error: %v", err)
		}
	})
}

func createJWT(t *testing.T, issuer, audience, subject string) string {
	const kid = "749df841-6e38-48f1-a178-20ecdd0b09f7"

	// load jwks from testdata
	data, err := os.ReadFile("testdata/test_jwks_private.json")
	require.NoError(t, err)

	var jwks jose.JSONWebKeySet
	err = json.Unmarshal(data, &jwks)
	require.NoError(t, err)
	require.NotEmpty(t, jwks.Keys)

	// choose key by kid or default to first
	var jwk *jose.JSONWebKey
	if kid == "" {
		jwk = &jwks.Keys[0]
	} else {
		for i := range jwks.Keys {
			if jwks.Keys[i].KeyID == kid {
				jwk = &jwks.Keys[i]
				break
			}
		}
		require.NotNil(t, jwk)
	}

	// ensure we have a private key
	require.False(t, jwk.IsPublic())

	// create signer with kid header (if present)
	opts := (&jose.SignerOptions{}).WithType("JWT")
	if jwk.KeyID != "" {
		opts = opts.WithHeader("kid", jwk.KeyID)
	}

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jwk.Key}, opts)
	require.NoError(t, err)

	// build claims
	now := time.Now().UTC()
	claims := map[string]interface{}{
		"iss": issuer,
		"aud": audience,
		"sub": subject,
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}

	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	jws, err := signer.Sign(payload)
	require.NoError(t, err)

	compact, err := jws.CompactSerialize()
	require.NoError(t, err)

	return compact
}

func makeMetricsCall(t *testing.T, metricsPort int, bearerToken string) *http.Response {
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", metricsPort)
	req, err := http.NewRequest("GET", url, nil)
	require.NoError(t, err)

	if bearerToken != "" {
		req.Header.Add("Authorization", "Bearer "+bearerToken)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	return resp
}

var jwksPort int

func TestMetricsAuth(t *testing.T) {
	jwksPort = GlobalPorts.GetPort(t)
	absPath, err := filepath.Abs("./testdata/test_jwks.json")
	require.NoError(t, err)
	runJWKSServer(t, absPath, jwksPort)

	t.Parallel()
	t.Run("No Metrics Auth", testNoMetricsAuth)
	t.Run("Missing Metrics Auth", testMissingMetricsAuth)
	t.Run("Valid Metrics Auth", testValidMetricsAuth)
	t.Run("Bad Audience Claim", testBadAudienceClaim)
	t.Run("Bad Issuer Claim", testBadIssuerClaim)
	t.Run("Bad Subject Claim", testBadSubjectClaim)
}

func startServerWithMetrics(t *testing.T, getConfig getConfigFunc) int {
	var ports DynamicResources
	ports.global = &GlobalPorts
	ports.t = t

	serverPort := ports.GetOrAllocatePort("server")
	metricsPort := ports.GetOrAllocatePort("metrics")

	config := getConfig(serverPort, metricsPort)

	u, err := driver.NewDoltUser()
	require.NoError(t, err)
	t.Cleanup(func() {
		u.Cleanup()
	})

	rs, err := u.MakeRepoStore()
	require.NoError(t, err)
	repo, err := rs.MakeRepo("max_conns_test")
	require.NoError(t, err)

	f, err := os.CreateTemp("", "config-*.yaml")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(f.Name())
	})

	_, err = f.WriteString(config)
	require.NoError(t, err)

	args := []string{"--config", f.Name()}
	srvSettings := &driver.Server{
		Args:        args,
		DynamicPort: "server",
	}

	t.Log("Starting server with config:\n" + config)
	MakeServer(t, repo, srvSettings, &ports)

	// hack to wait for server to start before making metrics call
	time.Sleep(1 * time.Second)

	return metricsPort
}

func testNoMetricsAuth(t *testing.T) {
	t.Parallel()

	getConfig := func(serverPort, metricsPort int) string {
		return fmt.Sprintf(`
listener:
  host: localhost
  port: %d

metrics:
  host: localhost
  port: %d
  jwt_required_for_localhost: true
`, serverPort, metricsPort)
	}

	metricsPort := startServerWithMetrics(t, getConfig)
	resp := makeMetricsCall(t, metricsPort, "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func testMissingMetricsAuth(t *testing.T) {
	t.Parallel()

	getConfig := func(serverPort, metricsPort int) string {
		return fmt.Sprintf(`listener:
  host: localhost
  port: %d

metrics:
  host: localhost
  port: %d
  jwks:
    name: jwksname
    location_url: http://127.0.0.1:%d/jwks.json
    claims:
      alg: RS256
      iss: dolthub.com
      sub: test_sub
      aud: test_aud
  jwt_required_for_localhost: true
`, serverPort, metricsPort, jwksPort)
	}

	metricsPort := startServerWithMetrics(t, getConfig)
	resp := makeMetricsCall(t, metricsPort, "")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func testValidMetricsAuth(t *testing.T) {
	t.Parallel()

	getConfig := func(serverPort, metricsPort int) string {
		return fmt.Sprintf(`listener:
  host: localhost
  port: %d

metrics:
  host: localhost
  port: %d
  jwks:
    name: jwksname
    location_url: http://127.0.0.1:%d/jwks.json
    claims:
      iss: dolthub.com
      sub: test_sub
      aud: test_aud
  jwt_required_for_localhost: true
`, serverPort, metricsPort, jwksPort)
	}

	metricsPort := startServerWithMetrics(t, getConfig)
	jwt := createJWT(t, "dolthub.com", "test_aud", "test_sub")
	resp := makeMetricsCall(t, metricsPort, jwt)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func testBadIssuerClaim(t *testing.T) {
	t.Parallel()

	getConfig := func(serverPort, metricsPort int) string {
		return fmt.Sprintf(`listener:
  host: localhost
  port: %d

metrics:
  host: localhost
  port: %d
  jwks:
    name: jwksname
    location_url: http://127.0.0.1:%d/jwks.json
    claims:
      iss: dolthub.com
      sub: test_sub
      aud: test_aud
  jwt_required_for_localhost: true
`, serverPort, metricsPort, jwksPort)
	}

	metricsPort := startServerWithMetrics(t, getConfig)
	jwt := createJWT(t, "badissuer.com", "test_aud", "test_sub")
	resp := makeMetricsCall(t, metricsPort, jwt)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func testBadAudienceClaim(t *testing.T) {
	t.Parallel()

	getConfig := func(serverPort, metricsPort int) string {
		return fmt.Sprintf(`listener:
  host: localhost
  port: %d

metrics:
  host: localhost
  port: %d
  jwks:
    name: jwksname
    location_url: http://127.0.0.1:%d/jwks.json
    claims:
      iss: dolthub.com
      sub: test_sub
      aud: test_aud
  jwt_required_for_localhost: true
`, serverPort, metricsPort, jwksPort)
	}

	metricsPort := startServerWithMetrics(t, getConfig)
	jwt := createJWT(t, "dolthub.com", "bad_aud", "test_sub")
	resp := makeMetricsCall(t, metricsPort, jwt)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func testBadSubjectClaim(t *testing.T) {
	t.Parallel()

	getConfig := func(serverPort, metricsPort int) string {
		return fmt.Sprintf(`listener:
  host: localhost
  port: %d

metrics:
  host: localhost
  port: %d
  jwks:
    name: jwksname
    location_url: http://127.0.0.1:%d/jwks.json
    claims:
      iss: dolthub.com
      sub: test_sub
      aud: test_aud
  jwt_required_for_localhost: true
`, serverPort, metricsPort, jwksPort)
	}

	metricsPort := startServerWithMetrics(t, getConfig)
	jwt := createJWT(t, "dolthub.com", "test_aud", "bad_sub")
	resp := makeMetricsCall(t, metricsPort, jwt)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
