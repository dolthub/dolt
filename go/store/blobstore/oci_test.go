package blobstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	assert "github.com/stretchr/testify/require"
	"io"
	"sort"
	"sync"

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

func TestUploadParts(t *testing.T) {
	buf := make([]byte, 35*1024*1024)
	_, err := rand.Read(buf)
	assert.NoError(t, err)

	r := bytes.NewReader(buf)

	partSize := 1024
	numParts, totalSize, nr, err := getUploadInfo(partSize, maxPartNum, r)
	assert.NoError(t, err)

	collector := &collector{
		m:              &sync.Mutex{},
		completedParts: make([]testPart, 0),
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
		part := testPart{
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

type testPart struct {
	b  []byte
	cp objectstorage.CommitMultipartUploadPartDetails
}

type collector struct {
	m              *sync.Mutex
	completedParts []testPart
}

func (c *collector) Add(part testPart) {
	c.m.Lock()
	defer c.m.Unlock()
	c.completedParts = append(c.completedParts, part)
}

func (c *collector) Parts() []testPart {
	c.m.Lock()
	defer c.m.Unlock()
	sort.Slice(c.completedParts, func(i, j int) bool {
		return *c.completedParts[i].cp.PartNum < *c.completedParts[j].cp.PartNum
	})
	return c.completedParts
}
