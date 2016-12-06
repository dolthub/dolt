// +build !go1.5

package v4_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/stretchr/testify/assert"
)

func TestStandaloneSign(t *testing.T) {
	creds := unit.Session.Config.Credentials
	signer := v4.NewSigner(creds)

	for _, c := range standaloneSignCases {
		host := fmt.Sprintf("%s.%s.%s.amazonaws.com",
			c.SubDomain, c.Region, c.Service)

		req, err := http.NewRequest("GET", fmt.Sprintf("https://%s", host), nil)
		assert.NoError(t, err)

		req.URL.Path = c.OrigURI
		req.URL.RawQuery = c.OrigQuery
		req.URL.Opaque = fmt.Sprintf("//%s%s", host, c.EscapedURI)
		opaqueURI := req.URL.Opaque

		_, err = signer.Sign(req, nil, c.Service, c.Region, time.Unix(0, 0))
		assert.NoError(t, err)

		actual := req.Header.Get("Authorization")
		assert.Equal(t, c.ExpSig, actual)
		assert.Equal(t, c.OrigURI, req.URL.Path)
		assert.Equal(t, opaqueURI, req.URL.Opaque)
	}
}
