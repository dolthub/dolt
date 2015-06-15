package store

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

type mockS3 map[string][]byte

func (m *mockS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	result, ok := (*m)[*input.Key]
	if !ok {
		return nil, errors.New("not here")
	}

	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(result)),
	}, nil
}

func (m *mockS3) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	bytes, _ := ioutil.ReadAll(input.Body)
	(*m)[*input.Key] = bytes
	return nil, nil
}

func TestS3StorePut(t *testing.T) {
	assert := assert.New(t)

	input := "abc"

	s := S3Store{
		"bucket",
		"table",
		&mockS3{},
		nil,
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

type mockAWSError string

func (m mockAWSError) Error() string   { return string(m) }
func (m mockAWSError) Code() string    { return string(m) }
func (m mockAWSError) Message() string { return string(m) }
func (m mockAWSError) OrigErr() error  { return nil }

type mockDDB string

func (m *mockDDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if *m == "" {
		return &dynamodb.GetItemOutput{
			Item: map[string]*dynamodb.AttributeValue{},
		}, nil
	}

	return &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"name": {S: aws.String("root")},
			"sha1": {S: aws.String(string(*m))},
		},
	}, nil
}

func (m *mockDDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	initial := *(input.ConditionExpression) == "attribute_not_exists(sha1)"

	if (initial && string(*m) != "") || (!initial && string(*m) != *(input.ExpressionAttributeValues[":prev"].S)) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	*m = mockDDB(*(input.Item["sha1"].S))
	return &dynamodb.PutItemOutput{}, nil
}

func TestS3StoreRoot(t *testing.T) {
	assert := assert.New(t)

	m := mockDDB("")

	s := S3Store{
		"bucket",
		"table",
		nil,
		&m,
	}

	oldRoot := s.Root()
	assert.Equal(oldRoot, ref.Ref{})

	bogusRoot, err := ref.Parse("sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5") // sha1("Bogus, Dude")
	assert.NoError(err)
	newRoot, err := ref.Parse("sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd") // sha1("Hello, World")
	assert.NoError(err)

	// Try to update root with bogus oldRoot
	result := s.UpdateRoot(newRoot, bogusRoot)
	assert.False(result)
	assert.Equal(ref.Ref{}, s.Root())

	// No do a valid update
	result = s.UpdateRoot(newRoot, oldRoot)
	assert.True(result)
	assert.Equal(s.Root(), newRoot)

	// Now that there is a valid root, try to start a new lineage
	result = s.UpdateRoot(bogusRoot, ref.Ref{})
	assert.False(result)
	assert.Equal(s.Root(), newRoot)
}
