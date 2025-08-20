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
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	assert "github.com/stretchr/testify/require"

	"testing"
)

type testUploadPart struct {
	cp objectstorage.CommitMultipartUploadPartDetails
	b  []byte
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

	totalSize := int64(len(buf))
	partSize := int64(1024)
	numParts, _ := getNumPartsAndPartSize(totalSize, partSize, maxPartNum)

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

	completedParts, err := uploadParts(context.Background(), "test-object", "test-upload-id", numParts, 3, totalSize, 55*1024, r, f)
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
