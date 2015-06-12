package store

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

type MockS3 map[string][]byte

func (m *MockS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	result, ok := (*m)[*input.Key]
	if !ok {
		return nil, errors.New("not here")
	}

	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(result)),
	}, nil
}

func (m *MockS3) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	bytes, _ := ioutil.ReadAll(input.Body)
	(*m)[*input.Key] = bytes
	return nil, nil
}

func TestS3StorePut(t *testing.T) {
	assert := assert.New(t)

	input := "abc"

	s := S3Store{
		"bucket",
		&MockS3{},
	}

	w := s.Put()
	_, err := w.Write([]byte(input))
	assert.NoError(err)

	r1, err := w.Ref()
	assert.NoError(err)

	// See http://www.di-mgt.com.au/sha_testvectors.html
	assert.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", r1.String())

	// And reading it via the API should work...
	reader, err := s.Get(r1)
	assert.NoError(err)

	data, err := ioutil.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(input, string(data))

	// Reading a non-existing ref fails
	hash := ref.NewHash()
	hash.Write([]byte("Non-existent"))
	reader, err = s.Get(ref.FromHash(hash))
	assert.Error(err)
}
