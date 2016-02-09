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

package oauth

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"net/url"
	"testing"
)

func parseURL(urlStr string) *url.URL {
	u, err := url.Parse(urlStr)
	if err != nil {
		panic(err)
	}
	return u
}

var oauthTests = []struct {
	method    string
	url       *url.URL
	appParams url.Values
	nonce     string
	timestamp string

	clientCredentials Credentials
	credentials       Credentials

	signatureMethod SignatureMethod

	base   string
	header string
}{
	{
		// Simple example from Twitter OAuth tool
		method:            "GET",
		url:               parseURL("https://api.twitter.com/1/"),
		appParams:         url.Values{"page": {"10"}},
		nonce:             "8067e8abc6bdca2006818132445c8f4c",
		timestamp:         "1355795903",
		clientCredentials: Credentials{"kMViZR2MHk2mM7hUNVw9A", "56Fgl58yOfqXOhHXX0ybvOmSnPQFvR2miYmm30A"},
		credentials:       Credentials{"10212-JJ3Zc1A49qSMgdcAO2GMOpW9l7A348ESmhjmOBOU", "yF75mvq4LZMHj9O0DXwoC3ZxUnN1ptvieThYuOAYM"},
		base:              `GET&https%3A%2F%2Fapi.twitter.com%2F1%2F&oauth_consumer_key%3DkMViZR2MHk2mM7hUNVw9A%26oauth_nonce%3D8067e8abc6bdca2006818132445c8f4c%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1355795903%26oauth_token%3D10212-JJ3Zc1A49qSMgdcAO2GMOpW9l7A348ESmhjmOBOU%26oauth_version%3D1.0%26page%3D10`,
		header:            `OAuth oauth_consumer_key="kMViZR2MHk2mM7hUNVw9A", oauth_nonce="8067e8abc6bdca2006818132445c8f4c", oauth_signature="o5cx1ggJrY9ognZuVVeUwglKV8U%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1355795903", oauth_token="10212-JJ3Zc1A49qSMgdcAO2GMOpW9l7A348ESmhjmOBOU", oauth_version="1.0"`,
	},
	{
		// Test case and port insensitivity.
		method:            "GeT",
		url:               parseURL("https://apI.twItter.com:443/1/"),
		appParams:         url.Values{"page": {"10"}},
		nonce:             "8067e8abc6bdca2006818132445c8f4c",
		timestamp:         "1355795903",
		clientCredentials: Credentials{"kMViZR2MHk2mM7hUNVw9A", "56Fgl58yOfqXOhHXX0ybvOmSnPQFvR2miYmm30A"},
		credentials:       Credentials{"10212-JJ3Zc1A49qSMgdcAO2GMOpW9l7A348ESmhjmOBOU", "yF75mvq4LZMHj9O0DXwoC3ZxUnN1ptvieThYuOAYM"},
		base:              `GET&https%3A%2F%2Fapi.twitter.com%2F1%2F&oauth_consumer_key%3DkMViZR2MHk2mM7hUNVw9A%26oauth_nonce%3D8067e8abc6bdca2006818132445c8f4c%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1355795903%26oauth_token%3D10212-JJ3Zc1A49qSMgdcAO2GMOpW9l7A348ESmhjmOBOU%26oauth_version%3D1.0%26page%3D10`,
		header:            `OAuth oauth_consumer_key="kMViZR2MHk2mM7hUNVw9A", oauth_nonce="8067e8abc6bdca2006818132445c8f4c", oauth_signature="o5cx1ggJrY9ognZuVVeUwglKV8U%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1355795903", oauth_token="10212-JJ3Zc1A49qSMgdcAO2GMOpW9l7A348ESmhjmOBOU", oauth_version="1.0"`,
	},
	{
		// Example generated using the Netflix OAuth tool.
		method:            "GET",
		url:               parseURL("http://api-public.netflix.com/catalog/titles"),
		appParams:         url.Values{"term": {"Dark Knight"}, "count": {"2"}},
		nonce:             "1234",
		timestamp:         "1355850443",
		clientCredentials: Credentials{"apiKey001", "sharedSecret002"},
		credentials:       Credentials{"accessToken003", "accessSecret004"},
		base:              `GET&http%3A%2F%2Fapi-public.netflix.com%2Fcatalog%2Ftitles&count%3D2%26oauth_consumer_key%3DapiKey001%26oauth_nonce%3D1234%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1355850443%26oauth_token%3DaccessToken003%26oauth_version%3D1.0%26term%3DDark%2520Knight`,
		header:            `OAuth oauth_consumer_key="apiKey001", oauth_nonce="1234", oauth_signature="0JAoaqt6oz6TJx8N%2B06XmhPjcOs%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1355850443", oauth_token="accessToken003", oauth_version="1.0"`,
	},
	{
		// Special characters in form values.
		method:            "GET",
		url:               parseURL("http://PHOTOS.example.net:8001/Photos"),
		appParams:         url.Values{"photo size": {"300%"}, "title": {"Back of $100 Dollars Bill"}},
		nonce:             "kllo~9940~pd9333jh",
		timestamp:         "1191242096",
		clientCredentials: Credentials{"dpf43f3++p+#2l4k3l03", "secret01"},
		credentials:       Credentials{"nnch734d(0)0sl2jdk", "secret02"},
		base:              "GET&http%3A%2F%2Fphotos.example.net%3A8001%2FPhotos&oauth_consumer_key%3Ddpf43f3%252B%252Bp%252B%25232l4k3l03%26oauth_nonce%3Dkllo~9940~pd9333jh%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1191242096%26oauth_token%3Dnnch734d%25280%25290sl2jdk%26oauth_version%3D1.0%26photo%2520size%3D300%2525%26title%3DBack%2520of%2520%2524100%2520Dollars%2520Bill",
		header:            `OAuth oauth_consumer_key="dpf43f3%2B%2Bp%2B%232l4k3l03", oauth_nonce="kllo~9940~pd9333jh", oauth_signature="n1UAoQy2PoIYizZUiWvkdCxM3P0%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1191242096", oauth_token="nnch734d%280%290sl2jdk", oauth_version="1.0"`,
	},
	{
		// Special characters in path, multiple values for same key in form.
		method:            "GET",
		url:               parseURL("http://EXAMPLE.COM:80/Space%20Craft"),
		appParams:         url.Values{"name": {"value", "value"}},
		nonce:             "Ix4U1Ei3RFL",
		timestamp:         "1327384901",
		clientCredentials: Credentials{"abcd", "efgh"},
		credentials:       Credentials{"ijkl", "mnop"},
		base:              "GET&http%3A%2F%2Fexample.com%2FSpace%2520Craft&name%3Dvalue%26name%3Dvalue%26oauth_consumer_key%3Dabcd%26oauth_nonce%3DIx4U1Ei3RFL%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1327384901%26oauth_token%3Dijkl%26oauth_version%3D1.0",
		header:            `OAuth oauth_consumer_key="abcd", oauth_nonce="Ix4U1Ei3RFL", oauth_signature="TZZ5u7qQorLnmKs%2Biqunb8gqkh4%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1327384901", oauth_token="ijkl", oauth_version="1.0"`,
	},
	{
		// Query string in URL.
		method:            "GET",
		url:               parseURL("http://EXAMPLE.COM:80/Space%20Craft?name=value"),
		appParams:         url.Values{"name": {"value"}},
		nonce:             "Ix4U1Ei3RFL",
		timestamp:         "1327384901",
		clientCredentials: Credentials{"abcd", "efgh"},
		credentials:       Credentials{"ijkl", "mnop"},
		base:              "GET&http%3A%2F%2Fexample.com%2FSpace%2520Craft&name%3Dvalue%26name%3Dvalue%26oauth_consumer_key%3Dabcd%26oauth_nonce%3DIx4U1Ei3RFL%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1327384901%26oauth_token%3Dijkl%26oauth_version%3D1.0",
		header:            `OAuth oauth_consumer_key="abcd", oauth_nonce="Ix4U1Ei3RFL", oauth_signature="TZZ5u7qQorLnmKs%2Biqunb8gqkh4%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1327384901", oauth_token="ijkl", oauth_version="1.0"`,
	},
	{
		// "/" in form value.
		method:            "POST",
		url:               parseURL("https://stream.twitter.com/1.1/statuses/filter.json"),
		appParams:         url.Values{"track": {"example.com/abcd"}},
		nonce:             "bf2cb6d611e59f99103238fc9a3bb8d8",
		timestamp:         "1362434376",
		clientCredentials: Credentials{"consumer_key", "consumer_secret"},
		credentials:       Credentials{"token", "secret"},
		base:              "POST&https%3A%2F%2Fstream.twitter.com%2F1.1%2Fstatuses%2Ffilter.json&oauth_consumer_key%3Dconsumer_key%26oauth_nonce%3Dbf2cb6d611e59f99103238fc9a3bb8d8%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1362434376%26oauth_token%3Dtoken%26oauth_version%3D1.0%26track%3Dexample.com%252Fabcd",
		header:            `OAuth oauth_consumer_key="consumer_key", oauth_nonce="bf2cb6d611e59f99103238fc9a3bb8d8", oauth_signature="LcxylEOnNdgoKSJi7jX07mxcvfM%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1362434376", oauth_token="token", oauth_version="1.0"`,
	},
	{
		// "/" in query string
		method:            "POST",
		url:               parseURL("https://stream.twitter.com/1.1/statuses/filter.json?track=example.com/query"),
		appParams:         url.Values{},
		nonce:             "884275759fbab914654b50ae643c563a",
		timestamp:         "1362435218",
		clientCredentials: Credentials{"consumer_key", "consumer_secret"},
		credentials:       Credentials{"token", "secret"},
		base:              "POST&https%3A%2F%2Fstream.twitter.com%2F1.1%2Fstatuses%2Ffilter.json&oauth_consumer_key%3Dconsumer_key%26oauth_nonce%3D884275759fbab914654b50ae643c563a%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1362435218%26oauth_token%3Dtoken%26oauth_version%3D1.0%26track%3Dexample.com%252Fquery",
		header:            `OAuth oauth_consumer_key="consumer_key", oauth_nonce="884275759fbab914654b50ae643c563a", oauth_signature="OAldqvRrKDXRGZ9BqSi2CqeVH0g%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1362435218", oauth_token="token", oauth_version="1.0"`,
	},
	{
		// QuickBooks query string
		method:            "GET",
		url:               parseURL("https://qb.sbfinance.intuit.com/v3/company/1273852765/query"),
		appParams:         url.Values{"query": {"select * from account"}},
		nonce:             "12345678",
		timestamp:         "1409876517",
		clientCredentials: Credentials{"consumer_key", "consumer_secret"},
		credentials:       Credentials{"token", "secret"},
		base:              "GET&https%3A%2F%2Fqb.sbfinance.intuit.com%2Fv3%2Fcompany%2F1273852765%2Fquery&oauth_consumer_key%3Dconsumer_key%26oauth_nonce%3D12345678%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1409876517%26oauth_token%3Dtoken%26oauth_version%3D1.0%26query%3Dselect%2520%252A%2520from%2520account",
		header:            `OAuth oauth_consumer_key="consumer_key", oauth_nonce="12345678", oauth_signature="7crYee%2BJLvg7dksQiHbarUHN3rY%3D", oauth_signature_method="HMAC-SHA1", oauth_timestamp="1409876517", oauth_token="token", oauth_version="1.0"`,
	},
	{
		// Plain text signature method
		signatureMethod:   PLAINTEXT,
		method:            "GET",
		url:               parseURL("http://example.com/"),
		clientCredentials: Credentials{"key", "secret"},
		credentials:       Credentials{"accesskey", "accesssecret"},
		header:            `OAuth oauth_consumer_key="key", oauth_signature="secret%26accesssecret", oauth_signature_method="PLAINTEXT", oauth_token="accesskey", oauth_version="1.0"`,
	},
	{
		// RSA-SHA1 signature method
		signatureMethod:   RSASHA1,
		method:            "GET",
		url:               parseURL("http://term.ie/oauth/example/echo_api.php"),
		appParams:         url.Values{"method": {"foo%20bar"}, "bar": {"baz"}},
		nonce:             "a7da4d14579d61886be9d596d1a6a720",
		timestamp:         "1420240290",
		clientCredentials: Credentials{Token: "key"},
		credentials:       Credentials{Token: "accesskey"},
		base:              `GET&http%3A%2F%2Fterm.ie%2Foauth%2Fexample%2Fecho_api.php&bar%3Dbaz%26method%3Dfoo%252520bar%26oauth_consumer_key%3Dkey%26oauth_nonce%3Da7da4d14579d61886be9d596d1a6a720%26oauth_signature_method%3DRSA-SHA1%26oauth_timestamp%3D1420240290%26oauth_token%3Daccesskey%26oauth_version%3D1.0`,
		header:            `OAuth oauth_consumer_key="key", oauth_nonce="a7da4d14579d61886be9d596d1a6a720", oauth_signature="jPun728OkfFo7BjZiaQ5UBVChwk6tf0uKNFDmNKVb%2Bd6aWYEzsDVkqqjcgTrCRNabK8ubAnhyprafk0mk3zEJe%2BxGb9GKauqwUJ6ZZoGJNYYZg3BZUQvdxSKFs1M4MUMv3fxntmD%2BoyE8jPbrVM2zD1G1AAPm79sX%2B8XE25tBE8%3D", oauth_signature_method="RSA-SHA1", oauth_timestamp="1420240290", oauth_token="accesskey", oauth_version="1.0"`,
	},
}

func TestBaseString(t *testing.T) {
	for _, ot := range oauthTests {
		if ot.signatureMethod == PLAINTEXT {
			// PLAINTEXT signature does not use the base string.
			continue
		}
		oauthParams := map[string]string{
			"oauth_consumer_key":     ot.clientCredentials.Token,
			"oauth_nonce":            ot.nonce,
			"oauth_signature_method": ot.signatureMethod.String(),
			"oauth_timestamp":        ot.timestamp,
			"oauth_token":            ot.credentials.Token,
			"oauth_version":          "1.0",
		}
		var buf bytes.Buffer
		writeBaseString(&buf, ot.method, ot.url, ot.appParams, oauthParams)
		base := buf.String()
		if base != ot.base {
			t.Errorf("base string for %s %s\n    = %q,\n want %q", ot.method, ot.url, base, ot.base)
		}
	}
}

var pemPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQC0YjCwIfYoprq/FQO6lb3asXrxLlJFuCvtinTF5p0GxvQGu5O3
gYytUvtC2JlYzypSRjVxwxrsuRcP3e641SdASwfrmzyvIgP08N4S0IFzEURkV1wp
/IpH7kH41EtbmUmrXSwfNZsnQRE5SYSOhh+LcK2wyQkdgcMv11l4KoBkcwIDAQAB
AoGAWFlbZXlM2r5G6z48tE+RTKLvB1/btgAtq8vLw/5e3KnnbcDD6fZO07m4DRaP
jRryrJdsp8qazmUdcY0O1oK4FQfpprknDjP+R1XHhbhkQ4WEwjmxPstZMUZaDWF5
8d3otc23mCzwh3YcUWFu09KnMpzZsK59OfyjtkS44EDWpbECQQDXgN0ODboKsuEA
VAhAtPUqspU9ivRa6yLai9kCnPb9GcztrsJZQm4NHcKVbmD2F2L4pDRx4Pmglhfl
V7G/a6T7AkEA1kfU0+DkXc6I/jXHJ6pDLA5s7dBHzWgDsBzplSdkVQbKT3MbeYje
ByOxzXhulOWLBQW/vxmW4HwU95KTRlj06QJASPoBYY3yb0cN/J94P/lHgJMDCNky
UEuJ/PoYndLrrN/8zow8kh91xwlJ6HJ9cTiQMmTgwaOOxPuu0eI1df4M2wJBAJJS
WrKUT1z/O+zbLDOZwGTFNPzvzRgmft4z4A1J6OlmyZ+XKpvDKloVtcRpCJoEZPn5
AwaroquID4k/PfI7rIECQHeWa6+kPADv9IrK/92mujujS0MSEiynDw5NjTnHAH0v
8TrXzs+LCWDN/gbOCKPfnWRkgwgOeC8NN3h0zUIIUtA=
-----END RSA PRIVATE KEY-----
`

func TestAuthorizationHeader(t *testing.T) {
	originalTestHook := testHook
	defer func() {
		testHook = originalTestHook
	}()

	block, _ := pem.Decode([]byte(pemPrivateKey))
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	for _, ot := range oauthTests {
		testHook = func(p map[string]string) {
			if _, ok := p["oauth_nonce"]; ok {
				p["oauth_nonce"] = ot.nonce
			}
			if _, ok := p["oauth_timestamp"]; ok {
				p["oauth_timestamp"] = ot.timestamp
			}
		}
		c := Client{Credentials: ot.clientCredentials, SignatureMethod: ot.signatureMethod, PrivateKey: privateKey}
		header, err := c.authorizationHeader(&ot.credentials, ot.method, ot.url, ot.appParams)
		if err != nil {
			t.Errorf("authorizationHeader(&cred, %q, %q, %v) returned error %v", ot.method, ot.url.String(), ot.appParams, err)
			continue
		}
		if header != ot.header {
			t.Errorf("authorizationHeader(&cred, %q, %q, %v) =\n      %s\nwant: %s", ot.method, ot.url.String(), ot.appParams, header, ot.header)
		}
	}
}

func TestNonce(t *testing.T) {
	// This test is flaky, but failures should be very rare.
	n := nonce()
	if len(n) < 8 {
		t.Fatalf("nonce is %s, exected something longer", n)
	}
}
