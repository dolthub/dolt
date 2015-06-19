package chunks

import (
	"flag"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"

	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	rootTablePrimaryKey      = "name"
	rootTablePrimaryKeyValue = "root"
	rootTableRef             = "hashRef"
	refNotExistsExpression   = fmt.Sprintf("attribute_not_exists(%s)", rootTableRef)
	refEqualsExpression      = fmt.Sprintf("%s = :prev", rootTableRef)
)

type s3svc interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

type ddbsvc interface {
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

type S3Store struct {
	bucket, table string
	s3svc         s3svc
	ddbsvc        ddbsvc
}

func NewS3Store(bucket, table, region, key, secret string) S3Store {
	creds := aws.DefaultConfig.Credentials

	if key != "" {
		creds = credentials.NewStaticCredentials(key, secret, "")
	}

	return S3Store{
		bucket,
		table,
		s3.New(&aws.Config{Region: region, Credentials: creds}),
		dynamodb.New(&aws.Config{Region: region, Credentials: creds}),
	}
}

func (s S3Store) Root() ref.Ref {
	result, err := s.ddbsvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]*dynamodb.AttributeValue{
			rootTablePrimaryKey: {S: aws.String(rootTablePrimaryKeyValue)},
		},
	})
	Chk.NoError(err)

	if len(result.Item) == 0 {
		return ref.Ref{}
	}

	Chk.Equal(len(result.Item), 2)
	return ref.MustParse(*(result.Item[rootTableRef].S))
}

func (s S3Store) UpdateRoot(current, last ref.Ref) bool {
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

			Chk.NoError(awsErr)
		} else {
			Chk.NoError(err)
		}
	}

	return true
}

func (s S3Store) Get(ref ref.Ref) (io.ReadCloser, error) {
	result, err := s.s3svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.String()),
	})

	// TODO(rafael): S3 storage is eventually consistent, so in theory, we could fail to read a value by ref which hasn't propogated yet. Implement existence checks & retry.
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

func (s S3Store) Put() ChunkWriter {
	return &s3ChunkWriter{
		store: s,
		hash:  ref.NewHash(),
	}
}

type s3ChunkWriter struct {
	store  S3Store
	file   *os.File
	writer io.Writer
	hash   hash.Hash
}

func (w *s3ChunkWriter) Write(data []byte) (int, error) {
	if w.file == nil {
		f, err := ioutil.TempFile(os.TempDir(), "")
		Chk.NoError(err)
		w.file = f
		w.writer = io.MultiWriter(f, w.hash)
	}
	return w.writer.Write(data)
}

func (w *s3ChunkWriter) Ref() (ref.Ref, error) {
	ref := ref.FromHash(w.hash)

	w.file.Close()
	f, err := os.Open(w.file.Name())
	w.file = f

	_, err = w.store.s3svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(w.store.bucket),
		Key:    aws.String(ref.String()),
		Body:   w.file,
	})
	Chk.NoError(err)

	return ref, nil
}

func (w *s3ChunkWriter) Close() error {
	w.file.Close()
	os.Remove(w.file.Name())
	w.file = nil
	return nil
}

type s3StoreFlags struct {
	s3Bucket    *string
	dynamoTable *string
	awsRegion   *string
	authFromEnv *bool
	awsKey      *string
	awsSecret   *string
}

func s3Flags() s3StoreFlags {
	return s3StoreFlags{
		flag.String("aws-store-bucket", "", "s3 bucket to create an aws-based chunkstore in"),
		flag.String("aws-store-dynamo-table", "noms-root", "dynamodb table to store the root of the aws-based chunkstore in"),
		flag.String("aws-store-region", "us-west-2", "aws region to put the aws-based chunkstore in"),
		flag.Bool("aws-store-auth-from-env", false, "creates the aws-based chunkstore from authorization found in the environment. This is typically used in production to get keys from IAM profile. If not specified, then -aws-store-key and aws-store-secret must be specified instead"),
		flag.String("aws-store-key", "", "aws key to use to create the aws-based chunkstore"),
		flag.String("aws-store-secret", "", "aws secret to use to create the aws-based chunkstore"),
	}
}

func (f s3StoreFlags) createStore() ChunkStore {
	if *f.s3Bucket == "" || *f.awsRegion == "" || *f.dynamoTable == "" {
		return nil
	}

	if !*f.authFromEnv {
		if *f.awsKey == "" || *f.awsSecret == "" {
			return nil
		}
	}

	return NewS3Store(*f.s3Bucket, *f.dynamoTable, *f.awsRegion, *f.awsKey, *f.awsSecret)
}
