// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type mockAWSError string

func (m mockAWSError) Error() string   { return string(m) }
func (m mockAWSError) Code() string    { return string(m) }
func (m mockAWSError) Message() string { return string(m) }
func (m mockAWSError) OrigErr() error  { return nil }

func makeFakeS3(a *assert.Assertions) *fakeS3 {
	return &fakeS3{
		assert:     a,
		data:       map[string][]byte{},
		inProgress: map[string]fakeS3Multipart{},
		parts:      map[string][]byte{},
	}
}

type fakeS3 struct {
	assert *assert.Assertions

	mu                sync.Mutex
	data              map[string][]byte
	inProgressCounter int
	inProgress        map[string]fakeS3Multipart // Key -> {UploadId, Etags...}
	parts             map[string][]byte          // ETag -> data
	getCount          int
}

type fakeS3Multipart struct {
	uploadID string
	etags    []string
}

func (m *fakeS3) readerForTable(name addr) chunkReader {
	m.mu.Lock()
	defer m.mu.Unlock()
	if buff, present := m.data[name.String()]; present {
		return newTableReader(parseTableIndex(buff), bytes.NewReader(buff), s3ReadAmpThresh)
	}
	return nil
}

func (m *fakeS3) AbortMultipartUpload(input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
	m.assert.NotNil(input.Bucket, "Bucket is a required field")
	m.assert.NotNil(input.Key, "Key is a required field")
	m.assert.NotNil(input.UploadId, "UploadId is a required field")

	m.mu.Lock()
	defer m.mu.Unlock()
	m.assert.Equal(m.inProgress[*input.Key].uploadID, *input.UploadId)
	for _, etag := range m.inProgress[*input.Key].etags {
		delete(m.parts, etag)
	}
	delete(m.inProgress, *input.Key)
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (m *fakeS3) CreateMultipartUpload(input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	m.assert.NotNil(input.Bucket, "Bucket is a required field")
	m.assert.NotNil(input.Key, "Key is a required field")

	out := &s3.CreateMultipartUploadOutput{
		Bucket: input.Bucket,
		Key:    input.Key,
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	uploadID := strconv.Itoa(m.inProgressCounter)
	out.UploadId = aws.String(uploadID)
	m.inProgress[*input.Key] = fakeS3Multipart{uploadID, nil}
	m.inProgressCounter++
	return out, nil
}

func (m *fakeS3) UploadPart(input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	m.assert.NotNil(input.Bucket, "Bucket is a required field")
	m.assert.NotNil(input.Key, "Key is a required field")
	m.assert.NotNil(input.PartNumber, "PartNumber is a required field")
	m.assert.NotNil(input.UploadId, "UploadId is a required field")
	m.assert.NotNil(input.Body, "Body is a required field")

	data, err := ioutil.ReadAll(input.Body)
	m.assert.NoError(err)

	m.mu.Lock()
	defer m.mu.Unlock()
	etag := hash.Of(data).String() + time.Now().String()
	m.parts[etag] = data

	inProgress, present := m.inProgress[*input.Key]
	m.assert.True(present)
	m.assert.Equal(inProgress.uploadID, *input.UploadId)
	inProgress.etags = append(inProgress.etags, etag)
	m.inProgress[*input.Key] = inProgress
	return &s3.UploadPartOutput{ETag: aws.String(etag)}, nil
}

func (m *fakeS3) CompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	m.assert.NotNil(input.Bucket, "Bucket is a required field")
	m.assert.NotNil(input.Key, "Key is a required field")
	m.assert.NotNil(input.UploadId, "UploadId is a required field")
	m.assert.NotNil(input.MultipartUpload, "MultipartUpload is a required field")
	m.assert.True(len(input.MultipartUpload.Parts) > 0)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.assert.Equal(m.inProgress[*input.Key].uploadID, *input.UploadId)
	for idx, part := range input.MultipartUpload.Parts {
		m.assert.EqualValues(idx+1, *part.PartNumber) // Part numbers are 1-indexed
		m.data[*input.Key] = append(m.data[*input.Key], m.parts[*part.ETag]...)
		delete(m.parts, *part.ETag)
	}
	delete(m.inProgress, *input.Key)

	return &s3.CompleteMultipartUploadOutput{Bucket: input.Bucket, Key: input.Key}, nil
}

func (m *fakeS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	m.getCount++
	m.assert.NotNil(input.Bucket, "Bucket is a required field")
	m.assert.NotNil(input.Key, "Key is a required field")

	obj, present := m.data[*input.Key]
	if !present {
		return nil, mockAWSError("NoSuchKey")
	}
	if input.Range != nil {
		start, end := parseRange(*input.Range, len(obj))
		obj = obj[start:end]
	}

	return &s3.GetObjectOutput{
		Body:          ioutil.NopCloser(bytes.NewReader(obj)),
		ContentLength: aws.Int64(int64(len(obj))),
	}, nil
}

func parseRange(hdr string, total int) (start, end int) {
	d.PanicIfFalse(len(hdr) > len(s3RangePrefix))
	hdr = hdr[len(s3RangePrefix):]
	d.PanicIfFalse(hdr[0] == '=')
	hdr = hdr[1:]
	if hdr[0] == '-' {
		// negative range
		fromEnd, err := strconv.Atoi(hdr[1:])
		d.PanicIfError(err)
		return total - fromEnd, total
	}
	ends := strings.Split(hdr, "-")
	d.PanicIfFalse(len(ends) == 2)
	start, err := strconv.Atoi(ends[0])
	d.PanicIfError(err)
	end, err = strconv.Atoi(ends[1])
	d.PanicIfError(err)
	return start, end + 1 // insanely, the HTTP range header specifies ranges inclusively.
}

func (m *fakeS3) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	m.assert.NotNil(input.Bucket, "Bucket is a required field")
	m.assert.NotNil(input.Key, "Key is a required field")

	buff := &bytes.Buffer{}
	io.Copy(buff, input.Body)
	m.data[*input.Key] = buff.Bytes()

	return &s3.PutObjectOutput{}, nil
}
