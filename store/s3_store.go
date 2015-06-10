package store

import (
	"crypto/sha1"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
	"hash"
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

/*
 * S3Store assumes that credentials are received from the environment. In
 * prod, this will be an IAM Policy attached to the running EC2 instance.
 * For dev, the easiest thing to do put credentials in ~/.aws/credentials, e.g.
 * ___
 * aws_access_key_id = <id>
 * aws_secret_access_key = <secret>
 * EOF
 */

type S3Store struct {
	bucket string
	svc    *s3.S3
}

func NewS3Store() S3Store {
	return S3Store{
		"atticlabs",
		s3.New(&aws.Config{Region: "us-west-2"}),
	}
}

func (s S3Store) Get(ref ref.Ref) (io.ReadCloser, error) {
	result, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.String()),
	})

	// TODO(rafael): S3 storage is eventually consistent, so in theory, we could
	// fail to read a value by ref which hasn't propogated yet. Implement
	// existence checks & retry.
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

func (s S3Store) Put() ChunkWriter {
	return &s3ChunkWriter{
		s,
		nil,
		nil,
		sha1.New(),
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
	digest := ref.Sha1Digest{}
	w.hash.Sum(digest[:0])
	ref := ref.New(digest)

	w.file.Close()
	f2, err := os.Open(w.file.Name())
	w.file = f2

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
