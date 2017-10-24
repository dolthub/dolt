// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	minS3PartSize = 5 * 1 << 20  // 5MiB
	maxS3PartSize = 64 * 1 << 20 // 64MiB
	maxS3Parts    = 10000

	maxDynamoChunks   = 64
	maxDynamoItemSize = 400 * (1 << 10) // 400k

	defaultS3PartSize = minS3PartSize // smallest allowed by S3 allows for most throughput
)

type awsTablePersister struct {
	s3         s3svc
	bucket     string
	rl         chan struct{}
	tc         tableCache
	ddb        *ddbTableStore
	limits     awsLimits
	indexCache *indexCache
}

type awsLimits struct {
	partTarget, partMin, partMax uint64
	itemMax                      int
	chunkMax                     uint32
}

func (al awsLimits) tableFitsInDynamo(name addr, dataLen int, chunkCount uint32) bool {
	calcItemSize := func(n addr, dataLen int) int {
		return len(dbAttr) + len(tablePrefix) + len(n.String()) + len(dataAttr) + dataLen
	}
	return chunkCount <= al.chunkMax && calcItemSize(name, dataLen) < al.itemMax
}

func (al awsLimits) tableMayBeInDynamo(chunkCount uint32) bool {
	return chunkCount <= al.chunkMax
}

func (s3p awsTablePersister) Open(name addr, chunkCount uint32, stats *Stats) chunkSource {
	return newAWSChunkSource(
		s3p.ddb,
		&s3ObjectReader{s3: s3p.s3, bucket: s3p.bucket, readRl: s3p.rl, tc: s3p.tc},
		s3p.limits,
		name,
		chunkCount,
		s3p.indexCache,
		stats,
	)
}

type s3UploadedPart struct {
	idx  int64
	etag string
}

func (s3p awsTablePersister) Persist(mt *memTable, haver chunkReader, stats *Stats) chunkSource {
	name, data, chunkCount := mt.write(haver, stats)
	if chunkCount == 0 {
		return emptyChunkSource{}
	}
	if s3p.limits.tableFitsInDynamo(name, len(data), chunkCount) {
		s3p.ddb.Write(name, data)
		return s3p.newReaderFromIndexData(data, name, &dynamoTableReaderAt{ddb: s3p.ddb, h: name})
	}

	if s3p.tc != nil {
		go s3p.tc.store(name, bytes.NewReader(data), uint64(len(data)))
	}
	s3p.multipartUpload(data, name.String())
	tra := &s3TableReaderAt{&s3ObjectReader{s3: s3p.s3, bucket: s3p.bucket, readRl: s3p.rl, tc: s3p.tc}, name}
	return s3p.newReaderFromIndexData(data, name, tra)
}

func (s3p awsTablePersister) newReaderFromIndexData(idxData []byte, name addr, tra tableReaderAt) chunkSource {
	index := parseTableIndex(idxData)
	if s3p.indexCache != nil {
		s3p.indexCache.lockEntry(name)
		defer s3p.indexCache.unlockEntry(name)
		s3p.indexCache.put(name, index)
	}
	return &awsChunkSource{newTableReader(index, tra, s3BlockSize), name}
}

func (s3p awsTablePersister) multipartUpload(data []byte, key string) {
	uploadID := s3p.startMultipartUpload(key)
	multipartUpload, err := s3p.uploadParts(data, key, uploadID)
	if err != nil {
		s3p.abortMultipartUpload(key, uploadID)
		d.PanicIfError(err) // TODO: Better error handling here
	}
	s3p.completeMultipartUpload(key, uploadID, multipartUpload)
}

func (s3p awsTablePersister) startMultipartUpload(key string) string {
	result, err := s3p.s3.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(s3p.bucket),
		Key:    aws.String(key),
	})
	d.PanicIfError(err)
	return *result.UploadId
}

func (s3p awsTablePersister) abortMultipartUpload(key, uploadID string) {
	_, abrtErr := s3p.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s3p.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	d.PanicIfError(abrtErr)
}

func (s3p awsTablePersister) completeMultipartUpload(key, uploadID string, mpu *s3.CompletedMultipartUpload) {
	_, err := s3p.s3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s3p.bucket),
		Key:             aws.String(key),
		MultipartUpload: mpu,
		UploadId:        aws.String(uploadID),
	})
	d.PanicIfError(err)
}

func (s3p awsTablePersister) uploadParts(data []byte, key, uploadID string) (*s3.CompletedMultipartUpload, error) {
	sent, failed, done := make(chan s3UploadedPart), make(chan error), make(chan struct{})

	numParts := getNumParts(uint64(len(data)), s3p.limits.partTarget)
	d.PanicIfTrue(numParts > maxS3Parts) // TODO: BUG 3433: handle > 10k parts
	var wg sync.WaitGroup
	sendPart := func(partNum, start, end uint64) {
		if s3p.rl != nil {
			s3p.rl <- struct{}{}
			defer func() { <-s3p.rl }()
		}
		defer wg.Done()

		// Check if upload has been terminated
		select {
		case <-done:
			return
		default:
		}
		// Upload the desired part
		if partNum == numParts { // If this is the last part, make sure it includes any overflow
			end = uint64(len(data))
		}
		etag, err := s3p.uploadPart(data[start:end], key, uploadID, int64(partNum))
		if err != nil {
			failed <- err
			return
		}
		// Try to send along part info. In the case that the upload was aborted, reading from done allows this worker to exit correctly.
		select {
		case sent <- s3UploadedPart{int64(partNum), etag}:
		case <-done:
			return
		}
	}
	for i := uint64(0); i < numParts; i++ {
		wg.Add(1)
		partNum := i + 1 // Parts are 1-indexed
		start, end := i*s3p.limits.partTarget, (i+1)*s3p.limits.partTarget
		go sendPart(partNum, start, end)
	}
	go func() {
		wg.Wait()
		close(sent)
		close(failed)
	}()

	multipartUpload := &s3.CompletedMultipartUpload{}
	var firstFailure error
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
			if err != nil && firstFailure == nil { // nil err may happen when failed gets closed
				firstFailure = err
				close(done)
			}
		}
	}

	if firstFailure == nil {
		close(done)
	}
	sort.Sort(partsByPartNum(multipartUpload.Parts))
	return multipartUpload, firstFailure
}

func getNumParts(dataLen, minPartSize uint64) uint64 {
	numParts := dataLen / minPartSize
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

func (s3p awsTablePersister) ConjoinAll(sources chunkSources, stats *Stats) chunkSource {
	plan := planConjoin(sources, stats)
	if plan.chunkCount == 0 {
		return emptyChunkSource{}
	}
	t1 := time.Now()
	name := nameFromSuffixes(plan.suffixes())
	s3p.executeCompactionPlan(plan, name.String())
	verbose.Log("Compacted table of %d Kb in %s", plan.totalCompressedData/1024, time.Since(t1))

	if s3p.tc != nil {
		go s3p.loadIntoCache(name) // load conjoined table to the cache
	}
	tra := &s3TableReaderAt{&s3ObjectReader{s3: s3p.s3, bucket: s3p.bucket, readRl: s3p.rl, tc: s3p.tc}, name}
	return s3p.newReaderFromIndexData(plan.mergedIndex, name, tra)
}

func (s3p awsTablePersister) loadIntoCache(name addr) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s3p.bucket),
		Key:    aws.String(name.String()),
	}
	result, err := s3p.s3.GetObject(input)
	d.PanicIfError(err)

	s3p.tc.store(name, result.Body, uint64(*result.ContentLength))
}

func (s3p awsTablePersister) executeCompactionPlan(plan compactionPlan, key string) {
	uploadID := s3p.startMultipartUpload(key)
	multipartUpload, err := s3p.assembleTable(plan, key, uploadID)
	if err != nil {
		s3p.abortMultipartUpload(key, uploadID)
		d.PanicIfError(err) // TODO: Better error handling here
	}
	s3p.completeMultipartUpload(key, uploadID, multipartUpload)
}

func (s3p awsTablePersister) assembleTable(plan compactionPlan, key, uploadID string) (*s3.CompletedMultipartUpload, error) {
	d.PanicIfTrue(len(plan.sources) > maxS3Parts) // TODO: BUG 3433: handle > 10k parts

	// Separate plan.sources by amount of chunkData. Tables with >5MB of chunk data (copies) can be added to the new table using S3's multipart upload copy feature. Smaller tables with <5MB of chunk data (manuals) must be read, assembled into |buff|, and then re-uploaded in parts that are larger than 5MB.
	copies, manuals, buff := dividePlan(plan, uint64(s3p.limits.partMin), uint64(s3p.limits.partMax))

	// Concurrently read data from small tables into |buff|
	var readWg sync.WaitGroup
	for _, man := range manuals {
		readWg.Add(1)
		go func(m manualPart) {
			defer readWg.Done()
			n, _ := m.srcR.Read(buff[m.dstStart:m.dstEnd])
			d.PanicIfTrue(int64(n) < m.dstEnd-m.dstStart)
		}(man)
	}
	readWg.Wait()

	// sendPart calls |doUpload| to send part |partNum|, forwarding errors over |failed| or success over |sent|. Closing (or sending) on |done| will cancel all in-progress calls to sendPart.
	sent, failed, done := make(chan s3UploadedPart), make(chan error), make(chan struct{})
	var uploadWg sync.WaitGroup
	type uploadFn func() (etag string, err error)
	sendPart := func(partNum int64, doUpload uploadFn) {
		if s3p.rl != nil {
			s3p.rl <- struct{}{}
			defer func() { <-s3p.rl }()
		}
		defer uploadWg.Done()

		// Check if upload has been terminated
		select {
		case <-done:
			return
		default:
		}

		etag, err := doUpload()
		if err != nil {
			failed <- err
			return
		}
		// Try to send along part info. In the case that the upload was aborted, reading from done allows this worker to exit correctly.
		select {
		case sent <- s3UploadedPart{int64(partNum), etag}:
		case <-done:
			return
		}
	}

	// Concurrently begin sending all parts using sendPart().
	// First, kick off sending all the copyable parts.
	partNum := int64(1) // Part numbers are 1-indexed
	for _, cp := range copies {
		uploadWg.Add(1)
		go func(cp copyPart, partNum int64) {
			sendPart(partNum, func() (etag string, err error) {
				return s3p.uploadPartCopy(cp.name, cp.srcOffset, cp.srcLen, key, uploadID, partNum)
			})
		}(cp, partNum)
		partNum++
	}

	// Then, split buff (data from |manuals| and index) into parts and upload those concurrently.
	numManualParts := getNumParts(uint64(len(buff)), s3p.limits.partTarget) // TODO: What if this is too big?
	for i := uint64(0); i < numManualParts; i++ {
		start, end := i*s3p.limits.partTarget, (i+1)*s3p.limits.partTarget
		if i+1 == numManualParts { // If this is the last part, make sure it includes any overflow
			end = uint64(len(buff))
		}
		uploadWg.Add(1)
		go func(data []byte, partNum int64) {
			sendPart(partNum, func() (etag string, err error) {
				return s3p.uploadPart(data, key, uploadID, partNum)
			})
		}(buff[start:end], partNum)
		partNum++
	}

	// When all the uploads started above are done, close |sent| and |failed| so that the code below will correctly detect that we're done sending parts and move forward.
	go func() {
		uploadWg.Wait()
		close(sent)
		close(failed)
	}()

	// Watch |sent| and |failed| for the results of part uploads. If ever one fails, close |done| to stop all the in-progress or pending sendPart() calls and then bail.
	multipartUpload := &s3.CompletedMultipartUpload{}
	var firstFailure error
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
			if err != nil && firstFailure == nil { // nil err may happen when failed gets closed
				firstFailure = err
				close(done)
			}
		}
	}

	// If there was any failure detected above, |done| is already closed
	if firstFailure == nil {
		close(done)
	}
	sort.Sort(partsByPartNum(multipartUpload.Parts)) // S3 requires that these be in part-order
	return multipartUpload, firstFailure
}

type copyPart struct {
	name              string
	srcOffset, srcLen int64
}

type manualPart struct {
	srcR             io.Reader
	dstStart, dstEnd int64
}

// dividePlan assumes that plan.sources (which is of type chunkSourcesByDescendingDataSize) is correctly sorted by descending data size.
func dividePlan(plan compactionPlan, minPartSize, maxPartSize uint64) (copies []copyPart, manuals []manualPart, buff []byte) {
	// NB: if maxPartSize < 2*minPartSize, splitting large copies apart isn't solvable. S3's limits are plenty far enough apart that this isn't a problem in production, but we could violate this in tests.
	d.PanicIfTrue(maxPartSize < 2*minPartSize)

	buffSize := uint64(len(plan.mergedIndex))
	i := 0
	for ; i < len(plan.sources); i++ {
		sws := plan.sources[i]
		if sws.dataLen < minPartSize {
			// since plan.sources is sorted in descending chunk-data-length order, we know that sws and all members after it are too small to copy.
			break
		}
		if sws.dataLen <= maxPartSize {
			copies = append(copies, copyPart{sws.source.hash().String(), 0, int64(sws.dataLen)})
			continue
		}

		// Now, we need to break the data into some number of parts such that for all parts minPartSize <= size(part) <= maxPartSize. This code tries to split the part evenly, such that all new parts satisfy the previous inequality. This gets tricky around edge cases. Consider min = 5b and max = 10b and a data length of 101b. You need to send 11 parts, but you can't just send 10 parts of 10 bytes and 1 part of 1 byte -- the last is too small. You also can't send 10 parts of 9 bytes each and 1 part of 11 bytes, because the last is too big. You have to distribute the extra bytes across all the parts so that all of them fall into the proper size range.
		lens := splitOnMaxSize(sws.dataLen, maxPartSize)

		var srcStart int64
		for _, length := range lens {
			copies = append(copies, copyPart{sws.source.hash().String(), srcStart, length})
			srcStart += length
		}
	}
	var offset int64
	for ; i < len(plan.sources); i++ {
		sws := plan.sources[i]
		manuals = append(manuals, manualPart{sws.source.reader(), offset, offset + int64(sws.dataLen)})
		offset += int64(sws.dataLen)
		buffSize += sws.dataLen
	}
	buff = make([]byte, buffSize)
	copy(buff[buffSize-uint64(len(plan.mergedIndex)):], plan.mergedIndex)
	return
}

// Splits |dataLen| into the maximum number of roughly-equal part sizes such that each is <= maxPartSize.
func splitOnMaxSize(dataLen, maxPartSize uint64) []int64 {
	numParts := dataLen / maxPartSize
	if dataLen%maxPartSize > 0 {
		numParts++
	}
	baseSize := int64(dataLen / numParts)
	extraBytes := dataLen % numParts
	sizes := make([]int64, numParts)
	for i := range sizes {
		sizes[i] = baseSize
		if extraBytes > 0 {
			sizes[i]++
			extraBytes--
		}
	}
	return sizes
}

func (s3p awsTablePersister) uploadPartCopy(src string, srcStart, srcEnd int64, key, uploadID string, partNum int64) (etag string, err error) {
	res, err := s3p.s3.UploadPartCopy(&s3.UploadPartCopyInput{
		// TODO: Use url.PathEscape() once we're on go 1.8
		CopySource:      aws.String(url.QueryEscape(s3p.bucket + "/" + src)),
		CopySourceRange: aws.String(s3RangeHeader(srcStart, srcEnd)),
		Bucket:          aws.String(s3p.bucket),
		Key:             aws.String(key),
		PartNumber:      aws.Int64(int64(partNum)),
		UploadId:        aws.String(uploadID),
	})
	if err == nil {
		etag = *res.CopyPartResult.ETag
	}
	return
}

func (s3p awsTablePersister) uploadPart(data []byte, key, uploadID string, partNum int64) (etag string, err error) {
	res, err := s3p.s3.UploadPart(&s3.UploadPartInput{
		Bucket:     aws.String(s3p.bucket),
		Key:        aws.String(key),
		PartNumber: aws.Int64(int64(partNum)),
		UploadId:   aws.String(uploadID),
		Body:       bytes.NewReader(data),
	})
	if err == nil {
		etag = *res.ETag
	}
	return
}
