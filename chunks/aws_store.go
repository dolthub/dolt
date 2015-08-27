package chunks

import (
	"bytes"
	"flag"
	"fmt"
	"io"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/s3"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

var (
	rootTablePrimaryKey      = "name"
	rootTablePrimaryKeyValue = "root"
	rootTableRef             = "hashRef"
	refNotExistsExpression   = fmt.Sprintf("attribute_not_exists(%s)", rootTableRef)
	refEqualsExpression      = fmt.Sprintf("%s = :prev", rootTableRef)
)

type awsSvc interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

type ddbsvc interface {
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

type AWSStore struct {
	bucket, table string
	awsSvc        awsSvc
	ddbsvc        ddbsvc
}

func NewAWSStore(bucket, table, region, key, secret string) AWSStore {
	creds := defaults.DefaultConfig.Credentials

	if key != "" {
		creds = credentials.NewStaticCredentials(key, secret, "")
	}

	return AWSStore{
		bucket,
		table,
		s3.New(&aws.Config{Region: &region, Credentials: creds}),
		dynamodb.New(&aws.Config{Region: &region, Credentials: creds}),
	}
}

func (s AWSStore) Root() ref.Ref {
	result, err := s.ddbsvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]*dynamodb.AttributeValue{
			rootTablePrimaryKey: {S: aws.String(rootTablePrimaryKeyValue)},
		},
	})
	d.Exp.NoError(err)

	if len(result.Item) == 0 {
		return ref.Ref{}
	}

	d.Chk.Equal(len(result.Item), 2)
	return ref.Parse(*(result.Item[rootTableRef].S))
}

func (s AWSStore) UpdateRoot(current, last ref.Ref) bool {
	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]*dynamodb.AttributeValue{
			rootTablePrimaryKey: {S: aws.String(rootTablePrimaryKeyValue)},
			rootTableRef:        {S: aws.String(current.String())},
		},
	}

	if (last == ref.Ref{}) {
		putArgs.ConditionExpression = aws.String(refNotExistsExpression)
	} else {
		putArgs.ConditionExpression = aws.String(refEqualsExpression)
		putArgs.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":prev": {S: aws.String(last.String())},
		}
	}

	_, err := s.ddbsvc.PutItem(&putArgs)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ConditionalCheckFailedException" {
				return false
			}

			d.Chk.NoError(awsErr)
		} else {
			d.Chk.NoError(err)
		}
	}

	return true
}

func (s AWSStore) Get(ref ref.Ref) io.ReadCloser {
	result, err := s.awsSvc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.String()),
	})

	// TODO: S3 storage is eventually consistent, so in theory, we could fail to read a value by ref which hasn't propogated yet. Implement existence checks & retry.
	if err != nil {
		return nil
	}

	return result.Body
}

func (s AWSStore) Has(ref ref.Ref) bool {
	_, err := s.awsSvc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.String()),
	})
	return err == nil
}

func (s AWSStore) Put() ChunkWriter {
	return newChunkWriter(s.write)
}

func (s AWSStore) write(ref ref.Ref, buff *bytes.Buffer) {
	if s.Has(ref) {
		return
	}

	_, err := s.awsSvc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.String()),
		Body:   bytes.NewReader(buff.Bytes()),
	})
	d.Chk.NoError(err)
}

type awsStoreFlags struct {
	awsBucket   *string
	dynamoTable *string
	awsRegion   *string
	authFromEnv *bool
	awsKey      *string
	awsSecret   *string
}

func awsFlags(prefix string) awsStoreFlags {
	return awsStoreFlags{
		flag.String(prefix+"aws-bucket", "", "aws bucket to create an aws-based chunkstore in"),
		flag.String(prefix+"aws-dynamo-table", "noms-root", "dynamodb table to store the root of the aws-based chunkstore in"),
		flag.String(prefix+"aws-region", "us-west-2", "aws region to put the aws-based chunkstore in"),
		flag.Bool(prefix+"aws-auth-from-env", false, "creates the aws-based chunkstore from authorization found in the environment. This is typically used in production to get keys from IAM profile. If not specified, then -aws-key and aws-secret must be specified instead"),
		flag.String(prefix+"aws-key", "", "aws key to use to create the aws-based chunkstore"),
		flag.String(prefix+"aws-secret", "", "aws secret to use to create the aws-based chunkstore"),
	}
}

func (f awsStoreFlags) createStore() ChunkStore {
	if *f.awsBucket == "" || *f.awsRegion == "" || *f.dynamoTable == "" {
		return nil
	}

	if !*f.authFromEnv {
		if *f.awsKey == "" || *f.awsSecret == "" {
			return nil
		}
	}

	return NewAWSStore(*f.awsBucket, *f.dynamoTable, *f.awsRegion, *f.awsKey, *f.awsSecret)
}
