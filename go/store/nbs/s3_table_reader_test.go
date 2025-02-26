// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3TableReaderAt(t *testing.T) {
	s3 := makeFakeS3(t)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h, err := buildTable(chunks)
	require.NoError(t, err)
	s3.data[h.String()] = tableData

	t.Run("TolerateFailingReads", func(t *testing.T) {
		assert := assert.New(t)

		baseline := s3.getCount
		tra := &s3TableReaderAt{&s3ObjectReader{makeFlakyS3(s3), "bucket", nil, ""}, h.String()}
		scratch := make([]byte, len(tableData))
		_, err := tra.ReadAtWithStats(context.Background(), scratch, 0, &Stats{})
		require.NoError(t, err)
		// constructing the table reader should have resulted in 2 reads
		assert.Equal(2, s3.getCount-baseline)
		assert.Equal(tableData, scratch)
	})
}

func TestS3TableReaderAtNamespace(t *testing.T) {
	assert := assert.New(t)

	s3 := makeFakeS3(t)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	ns := "a-prefix-here"

	tableData, h, err := buildTable(chunks)
	require.NoError(t, err)
	s3.data["a-prefix-here/"+h.String()] = tableData

	tra := &s3TableReaderAt{&s3ObjectReader{s3, "bucket", nil, ns}, h.String()}
	scratch := make([]byte, len(tableData))
	_, err = tra.ReadAtWithStats(context.Background(), scratch, 0, &Stats{})
	require.NoError(t, err)
	assert.Equal(tableData, scratch)
}

type flakyS3 struct {
	s3iface.S3API
	alreadyFailed map[string]struct{}
}

func makeFlakyS3(svc s3iface.S3API) *flakyS3 {
	return &flakyS3{svc, map[string]struct{}{}}
}

func (fs3 *flakyS3) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	output, err := fs3.S3API.GetObjectWithContext(ctx, input)

	if err != nil {
		return nil, err
	}

	if _, ok := fs3.alreadyFailed[*input.Key]; !ok {
		fs3.alreadyFailed[*input.Key] = struct{}{}
		output.Body = io.NopCloser(resettingReader{})
	}

	return output, nil
}

type resettingReader struct{}

func (rr resettingReader) Read(p []byte) (n int, err error) {
	return 0, &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}
}
