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

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type mockAWSError string

func (m mockAWSError) Error() string   { return string(m) }
func (m mockAWSError) Code() string    { return string(m) }
func (m mockAWSError) Message() string { return string(m) }
func (m mockAWSError) OrigErr() error  { return nil }

type fakeS3 struct {
	data   map[string][]byte
	assert *assert.Assertions
}

func makeFakeS3(a *assert.Assertions) *fakeS3 {
	return &fakeS3{data: map[string][]byte{}, assert: a}
}

func (m *fakeS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
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
