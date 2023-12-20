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

package blobstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	assert "github.com/stretchr/testify/require"

	"testing"
)

func TestGetUploadInfo(t *testing.T) {
	buf := make([]byte, 35*1024)
	_, err := rand.Read(buf)
	assert.NoError(t, err)

	r := bytes.NewReader(buf)

	partSize := 55
	numParts, totalSize, nr, err := getUploadInfo(partSize, maxPartNum, r)
	assert.NoError(t, err)
	assert.NotNil(t, nr)
	assert.NotZero(t, numParts)
	assert.NotZero(t, totalSize)

	total := 0
	newBuf := make([]byte, 35*1024)
	for {
		n, err := nr.Read(newBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		total += n
	}
	assert.Equal(t, total, 35*1024)
	assert.Equal(t, len(buf), len(newBuf))
}

func TestGetDownloadInfo(t *testing.T) {
	tests := []struct {
		description    string
		totalSize      int64
		br             BlobRange
		expectedSize   int64
		expectedOffset int64
	}{
		{
			description:    "all range",
			totalSize:      10 * 1024,
			br:             NewBlobRange(0, 0),
			expectedSize:   10 * 1024,
			expectedOffset: 0,
		},
		{
			description:    "positive offset, no length",
			totalSize:      10 * 1024,
			br:             NewBlobRange(1234, 0),
			expectedSize:   (10 * 1024) - 1234,
			expectedOffset: 1234,
		},
		{
			description:    "positive offset, over total size",
			totalSize:      10 * 1024,
			br:             NewBlobRange(20*1024, 0),
			expectedSize:   0,
			expectedOffset: (10 * 1024) - 1,
		},
		{
			description:    "positive offset, specific length",
			totalSize:      10 * 1024,
			br:             NewBlobRange(251, 333),
			expectedSize:   333,
			expectedOffset: 251,
		},
		{
			description:    "zero offset, specific length",
			totalSize:      10 * 1024,
			br:             NewBlobRange(0, 555),
			expectedSize:   555,
			expectedOffset: 0,
		},
		{
			description:    "negative offset, no length",
			totalSize:      10 * 1024,
			br:             NewBlobRange(-2000, 0),
			expectedSize:   2000,
			expectedOffset: (10 * 1024) - 2000,
		},
		{
			description:    "negative offset, specific length",
			totalSize:      10 * 1024,
			br:             NewBlobRange(-5432, 1000),
			expectedSize:   1000,
			expectedOffset: (10 * 1024) - 5432,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			requestedSize, total, offset, err := getDownloadInfo(test.br, &test.totalSize)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedSize, requestedSize)
			assert.Equal(t, test.totalSize, total)
			assert.Equal(t, test.expectedOffset, offset)
		})
	}
}

type testUploadPart struct {
	b  []byte
	cp objectstorage.CommitMultipartUploadPartDetails
}

type uploadCollector struct {
	m              *sync.Mutex
	completedParts []testUploadPart
}

func (c *uploadCollector) Add(part testUploadPart) {
	c.m.Lock()
	defer c.m.Unlock()
	c.completedParts = append(c.completedParts, part)
}

func (c *uploadCollector) Parts() []testUploadPart {
	c.m.Lock()
	defer c.m.Unlock()
	sort.Slice(c.completedParts, func(i, j int) bool {
		return *c.completedParts[i].cp.PartNum < *c.completedParts[j].cp.PartNum
	})
	return c.completedParts
}

func TestUploadParts(t *testing.T) {
	buf := make([]byte, 35*1024*1024)
	_, err := rand.Read(buf)
	assert.NoError(t, err)

	r := bytes.NewReader(buf)

	partSize := 1024
	numParts, totalSize, nr, err := getUploadInfo(partSize, maxPartNum, r)
	assert.NoError(t, err)

	collector := &uploadCollector{
		m:              &sync.Mutex{},
		completedParts: make([]testUploadPart, 0),
	}

	f := func(ctx context.Context, objectName, uploadID string, partNumber int, contentLength int64, reader io.Reader) (objectstorage.CommitMultipartUploadPartDetails, error) {
		etag := uuid.NewString()
		b, err := io.ReadAll(reader)
		if err != nil {
			return objectstorage.CommitMultipartUploadPartDetails{}, err
		}
		cp := objectstorage.CommitMultipartUploadPartDetails{
			PartNum: &partNumber,
			Etag:    &etag,
		}
		part := testUploadPart{
			b:  b,
			cp: cp,
		}
		collector.Add(part)
		return cp, nil
	}

	completedParts, err := uploadParts(context.Background(), "test-object", "test-upload-id", numParts, 3, totalSize, 55*1024, nr, f)
	assert.NoError(t, err)
	assert.NotNil(t, completedParts)
	assert.Equal(t, len(completedParts), numParts)
	assert.Equal(t, len(completedParts), len(collector.Parts()))

	actual := collector.Parts()
	allData := make([]byte, 0)
	for _, tp := range actual {
		allData = append(allData, tp.b...)
	}

	assert.Equal(t, buf, allData)
}

type testDownloadPart struct {
	b  []byte
	cp *toDownload
}

type downloadCollector struct {
	m              *sync.Mutex
	completedParts []testDownloadPart
}

func (c *downloadCollector) Add(part testDownloadPart) {
	c.m.Lock()
	defer c.m.Unlock()
	c.completedParts = append(c.completedParts, part)
}

func (c *downloadCollector) Parts() []testDownloadPart {
	c.m.Lock()
	defer c.m.Unlock()
	sort.Slice(c.completedParts, func(i, j int) bool {
		return c.completedParts[i].cp.partNum < c.completedParts[j].cp.partNum
	})
	return c.completedParts
}

type concurrentRead struct {
	m *sync.Mutex
	b []byte
}

func (c *concurrentRead) Read(start, end int64) []byte {
	c.m.Lock()
	defer c.m.Unlock()
	if end > int64(len(c.b)) {
		return c.b[start:len(c.b)]
	}
	return c.b[start:end]
}

func TestDownloadParts(t *testing.T) {
	contentLength := int64(35 * 1024 * 1024)
	buf := make([]byte, contentLength)
	_, err := rand.Read(buf)
	assert.NoError(t, err)

	ccr := &concurrentRead{
		m: &sync.Mutex{},
		b: buf,
	}

	br := NewBlobRange(0, 0)
	requestedSize, totalSize, offset, err := getDownloadInfo(br, &contentLength)
	assert.NoError(t, err)

	collector := &downloadCollector{
		m:              &sync.Mutex{},
		completedParts: make([]testDownloadPart, 0),
	}

	f := func(ctx context.Context, objectName, namespace, etag string, partNumber int, br BlobRange) (*toDownload, error) {
		b := ccr.Read(br.offset, br.offset+br.length)
		cp := &toDownload{
			partNum: partNumber,
			br:      br,
		}
		part := testDownloadPart{
			b:  b,
			cp: cp,
		}
		collector.Add(part)
		return cp, nil
	}

	completedParts, err := downloadParts(
		context.Background(),
		"test-object",
		"test-namespace",
		"test-etag",
		requestedSize,
		totalSize,
		3,
		offset,
		25*1024,
		maxPartNum,
		3,
		f)

	assert.NoError(t, err)
	assert.NotNil(t, completedParts)
	assert.Equal(t, len(completedParts), len(collector.Parts()))

	actual := collector.Parts()
	allData := make([]byte, 0)
	for _, tp := range actual {
		allData = append(allData, tp.b...)
	}

	assert.Equal(t, buf, allData)
}

func TestAssembleParts(t *testing.T) {
	contentLength := int64(35 * 1024 * 1024)
	buf := make([]byte, contentLength)
	_, err := rand.Read(buf)
	assert.NoError(t, err)

	ccr := &concurrentRead{
		m: &sync.Mutex{},
		b: buf,
	}

	partSize := int64(12 * 1024)
	parts := make([]*toDownload, 0)
	partNum := 1
	offset := int64(0)
	for offset < contentLength {
		f, err := os.CreateTemp("", "")
		assert.NoError(t, err)

		nextOffset := offset + partSize

		remaining := partSize
		if nextOffset > contentLength {
			remaining = contentLength - offset
		}

		p := &toDownload{
			path:    f.Name(),
			partNum: partNum,
			br:      NewBlobRange(offset, remaining),
		}

		data := ccr.Read(offset, offset+remaining)
		_, err = f.Write(data)
		assert.NoError(t, err)

		parts = append(parts, p)
		offset = nextOffset
		assert.NoError(t, f.Close())
	}

	assert.NotNil(t, parts)
	assert.Equal(t, len(parts), int(math.Ceil(float64(contentLength)/float64(partSize))))

	// this will remove the completed parts files after they're assembled
	assembledReader, err := assembleParts(parts)
	assert.NoError(t, err)
	defer assembledReader.Close()

	actualData, err := io.ReadAll(assembledReader)
	assert.NoError(t, err)
	assert.Equal(t, actualData, buf)
}
