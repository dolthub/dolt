// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/sizecache"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const defaultS3PartSize = 5 * 1 << 20 // 5MiB, smallest allowed by S3

type s3TablePersister struct {
	s3         s3svc
	bucket     string
	partSize   int
	indexCache *s3IndexCache
}

func (s3p s3TablePersister) Open(name addr, chunkCount uint32) chunkSource {
	return newS3TableReader(s3p.s3, s3p.bucket, name, chunkCount, s3p.indexCache)
}

type s3UploadedPart struct {
	idx  int64
	etag string
}

func (s3p s3TablePersister) Compact(mt *memTable, haver chunkReader) chunkSource {
	name, data, chunkCount := mt.write(haver)

	if chunkCount > 0 {
		result, err := s3p.s3.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(s3p.bucket),
			Key:    aws.String(name.String()),
		})
		d.PanicIfError(err)
		uploadID := *result.UploadId

		multipartUpload, err := s3p.uploadParts(data, name.String(), uploadID)
		if err != nil {
			_, abrtErr := s3p.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(s3p.bucket),
				Key:      aws.String(name.String()),
				UploadId: aws.String(uploadID),
			})
			d.Chk.NoError(abrtErr)
			panic(err) // TODO: Better error handling here
		}

		_, err = s3p.s3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(s3p.bucket),
			Key:             aws.String(name.String()),
			MultipartUpload: multipartUpload,
			UploadId:        aws.String(uploadID),
		})
		d.Chk.NoError(err)
		s3tr := &s3TableReader{s3: s3p.s3, bucket: s3p.bucket, h: name}

		index := parseTableIndex(data)
		if s3p.indexCache != nil {
			s3p.indexCache.put(name, index)
		}
		s3tr.tableReader = newTableReader(index, s3tr, s3ReadAmpThresh)
		return s3tr
	}
	return emptyChunkSource{}
}

func (s3p s3TablePersister) uploadParts(data []byte, key, uploadID string) (*s3.CompletedMultipartUpload, error) {
	sent, failed, done := make(chan s3UploadedPart), make(chan error), make(chan struct{})

	numParts := getNumParts(len(data), s3p.partSize)
	var wg sync.WaitGroup
	wg.Add(numParts)
	sendPart := func(partNum int) {
		defer wg.Done()

		// Check if upload has been terminated
		select {
		case <-done:
			return
		default:
		}
		// Upload the desired part
		start, end := (partNum-1)*s3p.partSize, partNum*s3p.partSize
		if partNum == numParts { // If this is the last part, make sure it includes any overflow
			end = len(data)
		}
		result, err := s3p.s3.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(s3p.bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int64(int64(partNum)),
			UploadId:   aws.String(uploadID),
			Body:       bytes.NewReader(data[start:end]),
		})
		if err != nil {
			failed <- err
			return
		}
		// Try to send along part info. In the case that the upload was aborted, reading from done allows this worker to exit correctly.
		select {
		case sent <- s3UploadedPart{int64(partNum), *result.ETag}:
		case <-done:
			return
		}
	}
	for i := 1; i <= numParts; i++ {
		go sendPart(i)
	}
	go func() {
		wg.Wait()
		close(sent)
		close(failed)
	}()

	multipartUpload := &s3.CompletedMultipartUpload{}
	var lastFailure error
	for cont := true; cont; {
		select {
		case sentPart, open := <-sent:
			if open {
				multipartUpload.Parts = append(multipartUpload.Parts, &s3.CompletedPart{
					ETag:       aws.String(sentPart.etag),
					PartNumber: aws.Int64(sentPart.idx),
				})
			}
			cont = open

		case err := <-failed:
			if err != nil { // nil err may happen when failed gets closed
				lastFailure = err
				close(done)
			}
		}
	}

	if lastFailure == nil {
		close(done)
	}
	sort.Sort(partsByPartNum(multipartUpload.Parts))
	return multipartUpload, lastFailure
}

func getNumParts(dataLen, partSize int) int {
	numParts := dataLen / partSize
	if numParts == 0 {
		numParts = 1
	}
	return numParts
}

type partsByPartNum []*s3.CompletedPart

func (s partsByPartNum) Len() int {
	return len(s)
}

func (s partsByPartNum) Less(i, j int) bool {
	return *s[i].PartNumber < *s[j].PartNumber
}

func (s partsByPartNum) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type s3IndexCache struct {
	cache *sizecache.SizeCache
}

// Returns an indexCache which will burn roughly |size| bytes of memory
func newS3IndexCache(size uint64) *s3IndexCache {
	return &s3IndexCache{sizecache.New(size)}
}

func (sic s3IndexCache) get(name addr) (tableIndex, bool) {
	idx, found := sic.cache.Get(name)
	if found {
		return idx.(tableIndex), true
	}

	return tableIndex{}, false
}

func (sic s3IndexCache) put(name addr, idx tableIndex) {
	indexSize := uint64(idx.chunkCount) * (addrSize + ordinalSize + lengthSize + uint64Size)
	sic.cache.Add(name, indexSize, idx)
}
