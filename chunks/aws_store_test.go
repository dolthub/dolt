package chunks

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/s3"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
)

func TestAWSStoreTestSuite(t *testing.T) {
	suite.Run(t, &AWSStoreTestSuite{})
}

type AWSStoreTestSuite struct {
	ChunkStoreTestSuite
	s3svc *mockS3
}

func (suite *AWSStoreTestSuite) SetupTest() {
	suite.s3svc = &mockS3{data: map[string][]byte{}}

	m := mockDDB("")

	suite.store = AWSStore{
		"bucket",
		"table",
		suite.s3svc,
		&m,
	}

	suite.putCountFn = func() int {
		return suite.s3svc.numPuts
	}
}

func (suite *AWSStoreTestSuite) TearDownTest() {
}

type mockS3 struct {
	data    map[string][]byte
	numPuts int
}

func (m *mockS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	result, ok := m.data[*input.Key]
	if !ok {
		return nil, errors.New("not here")
	}

	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(result)),
	}, nil
}

func (m *mockS3) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if _, ok := m.data[*input.Key]; ok {
		return &s3.HeadObjectOutput{}, nil
	} else {
		return nil, errors.New("not here")
	}
}

func (m *mockS3) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	bytes, _ := ioutil.ReadAll(input.Body)
	m.data[*input.Key] = bytes
	m.numPuts += 1
	return nil, nil
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
			"name":    {S: aws.String("root")},
			"hashRef": {S: aws.String(string(*m))},
		},
	}, nil
}

func (m *mockDDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	initial := *(input.ConditionExpression) == "attribute_not_exists(hashRef)"

	if (initial && string(*m) != "") || (!initial && string(*m) != *(input.ExpressionAttributeValues[":prev"].S)) {
		return nil, mockAWSError("ConditionalCheckFailedException")
	}

	*m = mockDDB(*(input.Item["hashRef"].S))
	return &dynamodb.PutItemOutput{}, nil
}
