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
	creds := aws.DefaultConfig.Credentials

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
	Chk.NoError(err)

	if len(result.Item) == 0 {
		return ref.Ref{}
	}

	Chk.Equal(len(result.Item), 2)
	return ref.MustParse(*(result.Item[rootTableRef].S))
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

			Chk.NoError(awsErr)
		} else {
			Chk.NoError(err)
		}
	}

	return true
}

func (s AWSStore) Get(ref ref.Ref) (io.ReadCloser, error) {
	result, err := s.awsSvc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.String()),
	})

	// TODO: S3 storage is eventually consistent, so in theory, we could fail to read a value by ref which hasn't propogated yet. Implement existence checks & retry.
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

func (s AWSStore) Put() ChunkWriter {
	f, err := ioutil.TempFile(os.TempDir(), "")
	Chk.NoError(err)
	h := ref.NewHash()
	return &awsChunkWriter{
		store:  s,
		file:   f,
		writer: io.MultiWriter(f, h),
		hash:   h,
	}
}

type awsChunkWriter struct {
	store  AWSStore
	file   *os.File
	writer io.Writer
	hash   hash.Hash
}

func (w *awsChunkWriter) Write(data []byte) (int, error) {
	Chk.NotNil(w.file, "Write() cannot be called after Ref() or Close().")
	return w.writer.Write(data)
}

func (w *awsChunkWriter) Ref() (ref.Ref, error) {
	Chk.NoError(w.Close())
	return ref.FromHash(w.hash), nil
}

func (w *awsChunkWriter) Close() error {
	if w.file == nil {
		return nil
	}
	Chk.NoError(w.file.Sync())
	_, err := w.file.Seek(0, 0)
	Chk.NoError(err)

	bucket := aws.String(w.store.bucket)
	key := aws.String(ref.FromHash(w.hash).String())

	_, err = w.store.awsSvc.HeadObject(&s3.HeadObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err == nil {
		// Nothing to do, s3 already has this chunk
		return nil
	}

	_, err = w.store.awsSvc.PutObject(&s3.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   w.file,
	})
	Chk.NoError(err)

	Chk.NoError(w.file.Close())
	os.Remove(w.file.Name())
	w.file = nil
	return nil
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
