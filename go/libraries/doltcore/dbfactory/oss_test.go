package dbfactory

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_getOSSClient(t *testing.T) {
	_, err := getOSSClient()
	assert.Error(t, err)
	os.Setenv(ossEndpointEnvKey, "testendpoint")
	_, err = getOSSClient()
	assert.Error(t, err)

	os.Setenv(ossAccessKeyIDEnvKey, "testAccesskey")
	_, err = getOSSClient()
	assert.Error(t, err)

	os.Setenv(ossAccessKeySecretEnvKey, "testAccessSecret")
	_, err = getOSSClient()
	assert.Nil(t, err)
}
