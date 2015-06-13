package store

import (
	"flag"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"strings"

	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	awsAuthFlag = flag.String("aws-auth", "", "aws credentials: 'KEY:SECRET', or 'ENV' which will retrieve from IAM policy or ~/.aws/credentials")
	regionFlag  = flag.String("aws-region", "us-west-2", "aws region")
	bucketFlag  = flag.String("s3-bucket", "atticlabs", "s3 bucket which contains chunks")
)

type svc interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

type S3Store struct {
	bucket string
	svc    svc
}

func NewS3Store(bucket, region, awsAuth string) S3Store {
	Chk.NotEmpty(awsAuth)
	creds := aws.DefaultConfig.Credentials

	if awsAuth != "ENV" {
		toks := strings.Split(awsAuth, ":")
		Chk.Equal(2, len(toks))
		creds = credentials.NewStaticCredentials(toks[0], toks[1], "")
	}

	return S3Store{
		bucket,
		s3.New(&aws.Config{Region: region, Credentials: creds}),
	}
}

func NewS3StoreFromFlags() S3Store {
	return NewS3Store(*bucketFlag, *regionFlag, *awsAuthFlag)
}

func (s S3Store) Get(ref ref.Ref) (io.ReadCloser, error) {
	result, err := s.svc.GetObject(&s3.GetObjectInput{
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

	_, err = w.store.svc.PutObject(&s3.PutObjectInput{
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
