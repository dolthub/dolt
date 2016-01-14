package session_test

import (
	"net/http"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/session"
)

func TestNewDefaultSession(t *testing.T) {
	s := session.New(&aws.Config{Region: aws.String("region")})

	assert.Equal(t, "region", *s.Config.Region)
	assert.Equal(t, http.DefaultClient, s.Config.HTTPClient)
	assert.NotNil(t, s.Config.Logger)
	assert.Equal(t, aws.LogOff, *s.Config.LogLevel)
}
