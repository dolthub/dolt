package chunks

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/session"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	dynamoMaxGetCount = 100
	dynamoMaxPutCount = 25
	dynamoMaxPutSize  = 400 * 1024 // 400K

	refAttr   = "ref"
	chunkAttr = "chunk"
)

var (
	dynamoRootKey            = []byte("root")
	valueNotExistsExpression = fmt.Sprintf("attribute_not_exists(%s)", chunkAttr)
	valueEqualsExpression    = fmt.Sprintf("%s = :prev", chunkAttr)
)

type ddbsvc interface {
	BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

type DynamoStore struct {
	table           string
	ddbsvc          ddbsvc
	writeTime       int64
	writeCount      int64
	readTime        int64
	readCount       int64
	readQueue       chan readRequest
	writeQueue      chan Chunk
	finishedChan    chan struct{}
	requestWg       *sync.WaitGroup
	workerWg        *sync.WaitGroup
	unwrittenPuts   map[ref.Ref]Chunk
	unwrittenPutsMu *sync.Mutex
}

func NewDynamoStore(table, region, key, secret string) *DynamoStore {
	creds := defaults.CredChain(defaults.Config(), defaults.Handlers())

	if key != "" {
		creds = credentials.NewStaticCredentials(key, secret, "")
	}

	sess := session.New(&aws.Config{Region: aws.String(region), Credentials: creds})

	return newDynamoStoreFromDDBsvc(table, dynamodb.New(sess))
}

func newDynamoStoreFromDDBsvc(table string, ddb ddbsvc) *DynamoStore {
	store := &DynamoStore{
		table:           table,
		ddbsvc:          ddb,
		readQueue:       make(chan readRequest, readBufferSize),
		writeQueue:      make(chan Chunk, writeBufferSize),
		finishedChan:    make(chan struct{}),
		requestWg:       &sync.WaitGroup{},
		workerWg:        &sync.WaitGroup{},
		unwrittenPuts:   map[ref.Ref]Chunk{},
		unwrittenPutsMu: &sync.Mutex{},
	}
	store.batchGetRequests()
	store.batchPutRequests()
	return store
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

func (s *DynamoStore) addUnwrittenPut(c Chunk) bool {
	s.unwrittenPutsMu.Lock()
	defer s.unwrittenPutsMu.Unlock()
	if _, ok := s.unwrittenPuts[c.Ref()]; !ok {
		s.unwrittenPuts[c.Ref()] = c
		return true
	}

	return false
}

func (s *DynamoStore) getUnwrittenPut(r ref.Ref) Chunk {
	s.unwrittenPutsMu.Lock()
	defer s.unwrittenPutsMu.Unlock()
	if c, ok := s.unwrittenPuts[r]; ok {
		return c
	}
	return EmptyChunk
}

func (s *DynamoStore) clearUnwrittenPuts(chunks []Chunk) {
	s.unwrittenPutsMu.Lock()
	defer s.unwrittenPutsMu.Unlock()
	for _, c := range chunks {
		delete(s.unwrittenPuts, c.Ref())
	}
}

func (s *DynamoStore) Get(r ref.Ref) Chunk {
	pending := s.getUnwrittenPut(r)
	if !pending.IsEmpty() {
		return pending
	}

	ch := make(chan Chunk)
	s.requestWg.Add(1)
	s.readQueue <- getRequest{r, ch}
	return <-ch
}

func (s *DynamoStore) Has(r ref.Ref) bool {
	pending := s.getUnwrittenPut(r)
	if !pending.IsEmpty() {
		return true
	}

	ch := make(chan bool)
	s.requestWg.Add(1)
	s.readQueue <- hasRequest{r, ch}
	return <-ch
}

func (s *DynamoStore) Put(c Chunk) {
	if !s.addUnwrittenPut(c) {
		return
	}

	s.requestWg.Add(1)
	s.writeQueue <- c
}

func (s *DynamoStore) sendGetRequests(req readRequest) {
	n := time.Now().UnixNano()
	defer func() {
		s.readCount++
		s.readTime += time.Now().UnixNano() - n
	}()
	batch := readBatch{}
	refs := map[ref.Ref]bool{}

	addReq := func(req readRequest) {
		r := req.Ref()
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

	requestItems := buildRequestItems(s.table, refs)
	for hasUnprocessedKeys := true; hasUnprocessedKeys; {
		out, err := s.ddbsvc.BatchGetItem(&dynamodb.BatchGetItemInput{
			RequestItems: requestItems,
		})

		if err == nil {
			processResponses(out.Responses[s.table], batch)
		} else if err.(awserr.Error).Code() != "ProvisionedThroughputExceededException" {
			d.Chk.NoError(err, "Errors from BatchGetItem() other than throughput exceeded are fatal")
		}

		hasUnprocessedKeys = len(out.UnprocessedKeys) != 0
		requestItems = out.UnprocessedKeys
	}
	batch.Close()
}

func buildRequestItems(table string, refs map[ref.Ref]bool) map[string]*dynamodb.KeysAndAttributes {
	makeKeysAndAttrs := func() *dynamodb.KeysAndAttributes {
		out := &dynamodb.KeysAndAttributes{ConsistentRead: aws.Bool(true)} // This doubles the cost :-(
		for r := range refs {
			out.Keys = append(out.Keys, map[string]*dynamodb.AttributeValue{refAttr: {B: r.DigestSlice()}})
		}
		return out
	}
	return map[string]*dynamodb.KeysAndAttributes{table: makeKeysAndAttrs()}
}

func processResponses(responses []map[string]*dynamodb.AttributeValue, batch readBatch) {
	for _, item := range responses {
		p := item[refAttr]
		d.Chk.NotNil(p)
		r := ref.FromSlice(p.B)
		p = item[chunkAttr]
		d.Chk.NotNil(p)
		c := NewChunkWithRef(r, p.B)
		for _, reqChan := range batch[r] {
			reqChan.Satisfy(c)
		}
		delete(batch, r)
	}
}

func (s *DynamoStore) sendWriteRequests(first Chunk) {
	n := time.Now().UnixNano()
	defer func() {
		s.writeCount++
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

	requestItems := buildWriteRequests(s.table, chunks)
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

	s.clearUnwrittenPuts(chunks)
	s.requestWg.Add(-len(chunks))
}

func chunkItemSize(c Chunk) int {
	r := c.Ref()
	return len(refAttr) + len(r.DigestSlice()) + len(chunkAttr) + len(c.Data())
}

func buildWriteRequests(table string, chunks []Chunk) map[string][]*dynamodb.WriteRequest {
	chunkToItem := func(c Chunk) map[string]*dynamodb.AttributeValue {
		return map[string]*dynamodb.AttributeValue{
			refAttr:   {B: c.Ref().DigestSlice()},
			chunkAttr: {B: c.Data()},
		}
	}
	var requests []*dynamodb.WriteRequest
	for _, c := range chunks {
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: chunkToItem(c)},
		})
	}
	return map[string][]*dynamodb.WriteRequest{table: requests}
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

	if s.readCount > 0 {
		fmt.Printf("Read batch count: %d, Read batch latency: %d\n", s.readCount, s.readTime/s.readCount/1e6)
	}
	if s.writeCount > 0 {
		fmt.Printf("Write batch count: %d, Write batch latency: %d\n", s.writeCount, s.writeTime/s.writeCount/1e6)
	}
	return nil
}

func (s *DynamoStore) Root() ref.Ref {
	result, err := s.ddbsvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]*dynamodb.AttributeValue{
			refAttr: {B: dynamoRootKey},
		},
	})
	d.Exp.NoError(err)

	if len(result.Item) == 0 {
		return ref.Ref{}
	}

	d.Chk.Equal(len(result.Item), 2)
	return ref.FromSlice(result.Item[chunkAttr].B)
}

func (s *DynamoStore) UpdateRoot(current, last ref.Ref) bool {
	s.requestWg.Wait()

	putArgs := dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]*dynamodb.AttributeValue{
			refAttr:   {B: dynamoRootKey},
			chunkAttr: {B: current.DigestSlice()},
		},
	}

	if (last == ref.Ref{}) {
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

type DynamoStoreFlags struct {
	dynamoTable *string
	awsRegion   *string
	authFromEnv *bool
	awsKey      *string
	awsSecret   *string
}

func DynamoFlags(prefix string) DynamoStoreFlags {
	return DynamoStoreFlags{
		flag.String(prefix+"dynamo-table", "noms-values", "dynamodb table to store the values of the chunkstore in"),
		flag.String(prefix+"aws-region", "us-west-2", "aws region to put the aws-based chunkstore in"),
		flag.Bool(prefix+"aws-auth-from-env", false, "creates the aws-based chunkstore from authorization found in the environment. This is typically used in production to get keys from IAM profile. If not specified, then -aws-key and aws-secret must be specified instead"),
		flag.String(prefix+"aws-key", "", "aws key to use to create the aws-based chunkstore"),
		flag.String(prefix+"aws-secret", "", "aws secret to use to create the aws-based chunkstore"),
	}
}

func (f DynamoStoreFlags) CreateStore() ChunkStore {
	if *f.dynamoTable == "" || *f.awsRegion == "" {
		return nil
	}

	if !*f.authFromEnv {
		if *f.awsKey == "" || *f.awsSecret == "" {
			return nil
		}
	}

	return NewDynamoStore(*f.dynamoTable, *f.awsRegion, *f.awsKey, *f.awsSecret)
}
