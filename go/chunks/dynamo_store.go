// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	flag "github.com/juju/gnuflag"
)

const (
	dynamoMaxGetCount   = 100
	dynamoMaxPutCount   = 25
	dynamoMaxPutSize    = 400 * 1024 // 400K
	dynamoWriteUnitSize = 1024       // 1K

	readBufferSize  = 1 << 12 // 4k
	writeBufferSize = dynamoMaxPutCount

	dynamoTableName = "noms"
	refAttr         = "ref"
	chunkAttr       = "chunk"
	compAttr        = "comp"
	numAttr         = "num"
	noneValue       = "none"
	gzipValue       = "gzip"
)

var (
	dynamoRootKey            = []byte("root")
	dynamoVersionKey         = []byte("vers")
	valueNotExistsExpression = fmt.Sprintf("attribute_not_exists(%s)", chunkAttr)
	valueEqualsExpression    = fmt.Sprintf("%s = :prev", chunkAttr)
)

type ddbsvc interface {
	BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

// DynamoStore implements ChunkStore by storing data to DynamoDB and, if needed, S3. It assumes the existence of a DynamoDB table whose primary partition key is in Binary format and named `ref`.
type DynamoStore struct {
	table           string
	namespace       []byte
	namespaceLen    int
	rootKey         []byte
	versionKey      []byte
	versionSetOnce  sync.Once
	ddbsvc          ddbsvc
	writeTime       int64
	writeBatchCount uint64
	writeCount      uint64
	writeTotal      uint64
	writeCompTotal  uint64
	readTime        int64
	readBatchCount  int64
	readQueue       chan ReadRequest
	writeQueue      chan Chunk
	finishedChan    chan struct{}
	requestWg       *sync.WaitGroup
	workerWg        *sync.WaitGroup
	unwrittenPuts   *unwrittenPutCache
	showStats       bool
}

// NewDynamoStore returns a new DynamoStore instance pointed at a DynamoDB table in the given region. All keys used to access items are prefixed with the given namespace.
// Uses credential from the AWS config parameter.
func NewDynamoStore(table, namespace string, config *aws.Config, showStats bool) *DynamoStore {
	sess := session.New(config)

	return newDynamoStoreFromDDBsvc(table, namespace, dynamodb.New(sess), showStats)
}

func newDynamoStoreFromDDBsvc(table, namespace string, ddb ddbsvc, showStats bool) *DynamoStore {
	store := &DynamoStore{
		table:         table,
		namespace:     []byte(namespace),
		rootKey:       append([]byte(namespace), dynamoRootKey...),
		versionKey:    append([]byte(namespace), dynamoVersionKey...),
		ddbsvc:        ddb,
		readQueue:     make(chan ReadRequest, readBufferSize),
		writeQueue:    make(chan Chunk, writeBufferSize),
		finishedChan:  make(chan struct{}),
		requestWg:     &sync.WaitGroup{},
		workerWg:      &sync.WaitGroup{},
		unwrittenPuts: newUnwrittenPutCache(),
	}
	store.namespaceLen = len(store.namespace)
	store.batchGetRequests()
	store.batchPutRequests()
	store.showStats = showStats
	return store
}

func (s *DynamoStore) Get(h hash.Hash) Chunk {
	pending := s.unwrittenPuts.Get(h)
	if !pending.IsEmpty() {
		return pending
	}

	ch := make(chan Chunk)
	s.requestWg.Add(1)
	s.readQueue <- GetRequest{h, ch}
	return <-ch
}

func (s *DynamoStore) Has(h hash.Hash) bool {
	pending := s.unwrittenPuts.Get(h)
	if !pending.IsEmpty() {
		return true
	}

	ch := make(chan bool)
	s.requestWg.Add(1)
	s.readQueue <- HasRequest{h, ch}
	return <-ch
}

func (s *DynamoStore) PutMany(chunks []Chunk) (e BackpressureError) {
	for i, c := range chunks {
		if s.unwrittenPuts.Has(c) {
			continue
		}
		select {
		case s.writeQueue <- c:
			s.requestWg.Add(1)
			s.unwrittenPuts.Add(c)
		default:
			notPut := chunks[i:]
			e = make(BackpressureError, len(notPut))
			for j, np := range notPut {
				e[j] = np.Hash()
			}
			return
		}
	}
	return
}

func (s *DynamoStore) Put(c Chunk) {
	if !s.unwrittenPuts.Add(c) {
		return
	}

	s.requestWg.Add(1)
	s.writeQueue <- c
}

func (s *DynamoStore) batchGetRequests() {
	s.workerWg.Add(1)
	go func() {
		defer s.workerWg.Done()

		for done := false; !done; {
			select {
			case req := <-s.readQueue:
				s.sendGetRequests(req)
			case <-s.finishedChan:
				done = true
			}
		}
	}()
}

func (s *DynamoStore) sendGetRequests(req ReadRequest) {
	n := time.Now().UnixNano()
	defer func() {
		s.readBatchCount++
		s.readTime += time.Now().UnixNano() - n
	}()
	batch := ReadBatch{}
	refs := map[hash.Hash]bool{}

	addReq := func(req ReadRequest) {
		r := req.Hash()
		batch[r] = append(batch[r], req.Outstanding())
		refs[r] = true
		s.requestWg.Done()
	}
	addReq(req)

	for drained := false; !drained && len(refs) < dynamoMaxGetCount; {
		select {
		case req := <-s.readQueue:
			addReq(req)
		default:
			drained = true
		}
	}

	requestItems := s.buildRequestItems(refs)
	for hasUnprocessedKeys := true; hasUnprocessedKeys; {
		out, err := s.ddbsvc.BatchGetItem(&dynamodb.BatchGetItemInput{
			RequestItems: requestItems,
		})

		if err == nil {
			s.processResponses(out.Responses[s.table], batch)
		} else if err.(awserr.Error).Code() != "ProvisionedThroughputExceededException" {
			d.Chk.NoError(err, "Errors from BatchGetItem() other than throughput exceeded are fatal")
		}

		hasUnprocessedKeys = len(out.UnprocessedKeys) != 0
		requestItems = out.UnprocessedKeys
	}
	batch.Close()
}

func (s *DynamoStore) buildRequestItems(hashes map[hash.Hash]bool) map[string]*dynamodb.KeysAndAttributes {
	makeKeysAndAttrs := func() *dynamodb.KeysAndAttributes {
		out := &dynamodb.KeysAndAttributes{ConsistentRead: aws.Bool(true)} // This doubles the cost :-(
		for r := range hashes {
			out.Keys = append(out.Keys, map[string]*dynamodb.AttributeValue{refAttr: {B: s.makeNamespacedKey(r)}})
		}
		return out
	}
	return map[string]*dynamodb.KeysAndAttributes{s.table: makeKeysAndAttrs()}
}

func (s *DynamoStore) processResponses(responses []map[string]*dynamodb.AttributeValue, batch ReadBatch) {
	for _, item := range responses {
		p := item[refAttr]
		d.PanicIfFalse(p != nil)
		r := hash.FromSlice(s.removeNamespace(p.B))
		p = item[chunkAttr]
		d.PanicIfFalse(p != nil)
		b := p.B
		if p = item[compAttr]; p != nil && *p.S == gzipValue {
			gr, err := gzip.NewReader(bytes.NewReader(b))
			d.Chk.NoError(err)
			buf := &bytes.Buffer{}
			_, err = io.Copy(buf, gr)
			d.Chk.NoError(err)
			b = buf.Bytes()
		}
		c := NewChunkWithHash(r, b)
		for _, reqChan := range batch[r] {
			reqChan.Satisfy(c)
		}
		delete(batch, r)
	}
}

func (s *DynamoStore) batchPutRequests() {
	s.workerWg.Add(1)
	go func() {
		defer s.workerWg.Done()

		for done := false; !done; {
			select {
			case chunk := <-s.writeQueue:
				s.sendWriteRequests(chunk)
			case <-s.finishedChan:
				done = true
			}
		}
	}()
}

func (s *DynamoStore) sendWriteRequests(first Chunk) {
	s.versionSetOnce.Do(s.setVersIfUnset)
	n := time.Now().UnixNano()
	defer func() {
		s.writeBatchCount++
		s.writeTime += time.Now().UnixNano() - n
	}()
	chunks := []Chunk{}
	addReqIfFits := func(c Chunk) {
		size := chunkItemSize(c)
		if size > dynamoMaxPutSize {
			s.writeLargeChunk(c)
			return
		}
		chunks = append(chunks, c)
		return
	}

	addReqIfFits(first)
	for drained := false; !drained && len(chunks) < dynamoMaxPutCount; {
		select {
		case c := <-s.writeQueue:
			addReqIfFits(c)
		default:
			drained = true
		}
	}

	requestItems := s.buildWriteRequests(chunks)
	for hasUnprocessedItems := true; hasUnprocessedItems; {
		out, err := s.ddbsvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: requestItems,
		})

		if err != nil && err.(awserr.Error).Code() != "ProvisionedThroughputExceededException" {
			d.Chk.NoError(err, "Errors from BatchGetItem() other than throughput exceeded are fatal")
		}

		hasUnprocessedItems = len(out.UnprocessedItems) != 0
		requestItems = out.UnprocessedItems
	}

	s.unwrittenPuts.Clear(chunks)
	s.requestWg.Add(-len(chunks))
}

func chunkItemSize(c Chunk) int {
	r := c.Hash()
	return len(refAttr) + len(r.DigestSlice()) + len(chunkAttr) + len(c.Data()) + len(compAttr) + len(noneValue)
}

func (s *DynamoStore) buildWriteRequests(chunks []Chunk) map[string][]*dynamodb.WriteRequest {
	chunkToItem := func(c Chunk) map[string]*dynamodb.AttributeValue {
		chunkData := c.Data()
		chunkDataLen := uint64(len(chunkData))
		compDataLen := chunkDataLen
		compression := noneValue
		if chunkItemSize(c) > dynamoWriteUnitSize {
			compression = gzipValue
			buf := &bytes.Buffer{}
			gw := gzip.NewWriter(buf)
			_, err := io.Copy(gw, bytes.NewReader(chunkData))
			d.Chk.NoError(err)
			gw.Close()
			chunkData = buf.Bytes()
			compDataLen = uint64(buf.Len())
		}
		s.writeCount++
		s.writeTotal += chunkDataLen
		s.writeCompTotal += compDataLen
		return map[string]*dynamodb.AttributeValue{
			refAttr:   {B: s.makeNamespacedKey(c.Hash())},
			chunkAttr: {B: chunkData},
			compAttr:  {S: aws.String(compression)},
		}
	}
	var requests []*dynamodb.WriteRequest
	for _, c := range chunks {
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: chunkToItem(c)},
		})
	}
	return map[string][]*dynamodb.WriteRequest{s.table: requests}
}

func (s *DynamoStore) writeLargeChunk(c Chunk) {
	d.Chk.Fail("Unsupported!")
}

func (s *DynamoStore) Close() error {
	s.requestWg.Wait()

	close(s.finishedChan)
	s.workerWg.Wait()

	close(s.readQueue)
	close(s.writeQueue)

	if s.showStats {
		if s.readBatchCount > 0 {
			fmt.Printf("Read batch count: %d, Read batch latency: %dms\n", s.readBatchCount, s.readTime/s.readBatchCount/1e6)
		}
		if s.writeBatchCount > 0 {
			fmt.Printf("Write batch count: %d, Write batch latency: %dms\n", s.writeBatchCount, uint64(s.writeTime)/s.writeBatchCount/1e6)
		}
		if s.writeCount > 0 {
			fmt.Printf("Write chunk count: %d, Avg chunk size: %.3fK\n", s.writeCount, float64(s.writeTotal)/float64(s.writeCount)/1024.0)
			fmt.Printf("Avg compression ratio: %.2fx, Avg compressed chunk size: %.3fK\n", float64(s.writeTotal)/float64(s.writeCompTotal), float64(s.writeCompTotal)/float64(s.writeCount)/1024.0)
		}
	}
	return nil
}

func (s *DynamoStore) Version() string {
	result, err := s.ddbsvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]*dynamodb.AttributeValue{
			refAttr: {B: s.versionKey},
		},
	})
	d.Chk.NoError(err)

	itemLen := len(result.Item)
	if itemLen == 0 {
		return constants.NomsVersion
	}
	d.PanicIfFalse(itemLen == 2, "Version should have 2 attributes on it: %+v", result.Item)
	d.PanicIfFalse(result.Item[numAttr] != nil)
	d.PanicIfFalse(result.Item[numAttr].S != nil)
	return aws.StringValue(result.Item[numAttr].S)
}

func (s *DynamoStore) setVersIfUnset() {
	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]*dynamodb.AttributeValue{
			refAttr: {B: s.versionKey},
			numAttr: {S: aws.String(constants.NomsVersion)},
		},
		ConditionExpression: aws.String(valueNotExistsExpression),
	}

	if _, err := s.ddbsvc.PutItem(&putArgs); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() != "ConditionalCheckFailedException" {
				d.Chk.NoError(awsErr)
			}
		} else {
			d.Chk.NoError(err)
		}
	}
}

func (s *DynamoStore) Root() hash.Hash {
	result, err := s.ddbsvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]*dynamodb.AttributeValue{
			refAttr: {B: s.rootKey},
		},
	})
	d.PanicIfError(err)

	itemLen := len(result.Item)
	if itemLen == 0 {
		return hash.Hash{}
	}
	d.PanicIfFalse(itemLen == 2 || itemLen == 3, "Root should have 2 or three attributes on it: %+v", result.Item)
	if itemLen == 3 {
		d.PanicIfFalse(result.Item[compAttr] != nil)
		d.PanicIfFalse(result.Item[compAttr].S != nil)
		d.PanicIfFalse(noneValue == *result.Item[compAttr].S)
	}
	return hash.FromSlice(result.Item[chunkAttr].B)
}

func (s *DynamoStore) UpdateRoot(current, last hash.Hash) bool {
	s.versionSetOnce.Do(s.setVersIfUnset)
	s.requestWg.Wait()

	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]*dynamodb.AttributeValue{
			refAttr:   {B: s.rootKey},
			chunkAttr: {B: current.DigestSlice()},
			compAttr:  {S: aws.String(noneValue)},
		},
	}

	if last.IsEmpty() {
		putArgs.ConditionExpression = aws.String(valueNotExistsExpression)
	} else {
		putArgs.ConditionExpression = aws.String(valueEqualsExpression)
		putArgs.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":prev": {B: last.DigestSlice()},
		}
	}

	_, err := s.ddbsvc.PutItem(&putArgs)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ConditionalCheckFailedException" {
				return false
			}
			d.Chk.NoError(awsErr)
		} else {
			d.Chk.NoError(err)
		}
	}

	return true
}

func (s *DynamoStore) makeNamespacedKey(h hash.Hash) []byte {
	// This is semantically `return append(s.namespace, r.DigestSlice()...)`, but it seemed like we'd be doing this a LOT, and we know how much space we're going to need anyway. So, pre-allocate a slice and then copy into it.
	hashSlice := h.DigestSlice()
	key := make([]byte, s.namespaceLen+len(hashSlice))
	copy(key, s.namespace)
	copy(key[s.namespaceLen:], hashSlice)
	return key
}

func (s *DynamoStore) removeNamespace(namespaced []byte) []byte {
	return namespaced[len(s.namespace):]
}

type DynamoStoreFlags struct {
	dynamoStats *bool
	dynamoTable *string
	awsRegion   *string
	authFromEnv *bool
	awsKey      *string
	awsSecret   *string
}

func DynamoFlags(prefix string) DynamoStoreFlags {
	return DynamoStoreFlags{
		flag.Bool(prefix+"dynamo-stats", false, "On each DynamoStore close, print read and write stats. Can be quite verbose"),
		flag.String(prefix+"dynamo-table", dynamoTableName, "dynamodb table to store the values of the chunkstore in. You probably don't want to change this."),
		flag.String(prefix+"aws-region", "us-west-2", "aws region to put the aws-based chunkstore in"),
		flag.Bool(prefix+"aws-auth-from-env", false, "creates the aws-based chunkstore from authorization found in the environment. This is typically used in production to get keys from IAM profile. If not specified, then -aws-key and aws-secret must be specified instead"),
		flag.String(prefix+"aws-key", "", "aws key to use to create the aws-based chunkstore"),
		flag.String(prefix+"aws-secret", "", "aws secret to use to create the aws-based chunkstore"),
	}
}

func (f DynamoStoreFlags) CreateStore(ns string) ChunkStore {
	if f.check() {
		config := aws.NewConfig().WithRegion(*f.awsRegion)
		if *f.awsKey != "" {
			config = config.WithCredentials(credentials.NewStaticCredentials(*f.awsKey, *f.awsSecret, ""))
		}
		return NewDynamoStore(*f.dynamoTable, ns, config, *f.dynamoStats)
	}
	return nil
}

func (f DynamoStoreFlags) Shutter() {}

func (f DynamoStoreFlags) CreateFactory() (factree Factory) {
	if f.check() {
		factree = f
	}
	return
}

func (f DynamoStoreFlags) check() bool {
	if *f.dynamoTable == "" || *f.awsRegion == "" {
		return false
	}
	if !*f.authFromEnv {
		if *f.awsKey == "" || *f.awsSecret == "" {
			return false
		}
	}
	return true
}
