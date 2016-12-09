// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const defaultS3PartSize = 5 * 1 << 20 // 5MiB, smallest allowed by S3

func newS3TableSet(s3 s3svc, bucket string) tableSet {
	return tableSet{p: s3TablePersister{s3, bucket, defaultS3PartSize}}
}

func newFSTableSet(dir string) tableSet {
	return tableSet{p: fsTablePersister{dir}}
}

// tableSet is an immutable set of persistable chunkSources.
type tableSet struct {
	chunkSources
	p tablePersister
}

// Prepend adds a memTable to an existing tableSet, compacting |mt| and
// returning a new tableSet with newly compacted table added.
func (ts tableSet) Prepend(mt *memTable) tableSet {
	if tableHash, chunkCount := ts.p.Compact(mt, ts); chunkCount > 0 {
		newTables := make(chunkSources, len(ts.chunkSources)+1)
		newTables[0] = ts.p.Open(tableHash, chunkCount)
		copy(newTables[1:], ts.chunkSources)
		return tableSet{newTables, ts.p}
	}
	return ts
}

// Union returns a new tableSet holding the union of the tables managed by
// |ts| and those specified by |specs|.
func (ts tableSet) Union(specs []tableSpec) tableSet {
	newTables := make(chunkSources, len(ts.chunkSources))
	known := map[addr]struct{}{}
	for i, t := range ts.chunkSources {
		known[t.hash()] = struct{}{}
		newTables[i] = ts.chunkSources[i]
	}

	for _, t := range specs {
		if _, present := known[t.name]; !present {
			newTables = append(newTables, ts.p.Open(t.name, t.chunkCount))
		}
	}
	return tableSet{newTables, ts.p}
}

func (ts tableSet) ToSpecs() []tableSpec {
	tableSpecs := make([]tableSpec, len(ts.chunkSources))
	for i, src := range ts.chunkSources {
		tableSpecs[i] = tableSpec{src.hash(), src.count()}
	}
	return tableSpecs
}

func (ts tableSet) Close() (err error) {
	for _, t := range ts.chunkSources {
		err = t.close() // TODO: somehow coalesce these errors??
	}
	return
}

type tablePersister interface {
	Compact(mt *memTable, haver chunkReader) (name addr, chunkCount uint32)
	Open(name addr, chunkCount uint32) chunkSource
}

type s3TablePersister struct {
	s3       s3svc
	bucket   string
	partSize int
}

func (s3p s3TablePersister) Open(name addr, chunkCount uint32) chunkSource {
	return newS3TableReader(s3p.s3, s3p.bucket, name, chunkCount)
}

type s3UploadedPart struct {
	idx  int64
	etag string
}

func (s3p s3TablePersister) Compact(mt *memTable, haver chunkReader) (name addr, chunkCount uint32) {
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
	}
	return name, chunkCount
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

type fsTablePersister struct {
	dir string
}

func (ftp fsTablePersister) Compact(mt *memTable, haver chunkReader) (name addr, chunkCount uint32) {
	tempName, name, chunkCount := func() (string, addr, uint32) {
		temp, err := ioutil.TempFile(ftp.dir, "nbs_table_")
		d.PanicIfError(err)
		defer checkClose(temp)

		name, data, chunkCount := mt.write(haver)
		io.Copy(temp, bytes.NewReader(data))
		return temp.Name(), name, chunkCount
	}()
	if chunkCount > 0 {
		err := os.Rename(tempName, filepath.Join(ftp.dir, name.String()))
		d.PanicIfError(err)
	} else {
		os.Remove(tempName)
	}
	return name, chunkCount
}

func (ftp fsTablePersister) Open(name addr, chunkCount uint32) chunkSource {
	return newMmapTableReader(ftp.dir, name, chunkCount)
}
