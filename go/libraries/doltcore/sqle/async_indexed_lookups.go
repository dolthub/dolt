package sqle

import (
	"context"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
)

type lookupResult struct {
	r   sql.Row
	err error
}

type resultChanWithBacklog struct {
	writeCount      uint64
	lookupCount     uint64
	isFullyEnqueued uint64

	//id         string
	mu         *sync.Mutex
	chBuffSize int
	resChan    chan lookupResult
	backlog    []lookupResult
	backlogPos int
}

func newResultChanWithBacklog(resChanBuffSize, backlogSize int) *resultChanWithBacklog {
	return &resultChanWithBacklog{
		//id:         uuid.New().String(),
		mu:         &sync.Mutex{},
		backlog:    make([]lookupResult, backlogSize),
		chBuffSize: resChanBuffSize,
	}
}

func (r *resultChanWithBacklog) LookupEnqueued(done bool) {
	atomic.AddUint64(&r.lookupCount, 1)

	//log.Println(r.id, "lookup_count:", lookupCount)

	if done {
		atomic.StoreUint64(&r.isFullyEnqueued, 1)
		//log.Println(r.id, "fully_enqueued: 1")
	}
}

func (r *resultChanWithBacklog) Reset() {
	//log.Println(r.id, "reset")
	atomic.StoreUint64(&r.writeCount, 0)
	atomic.StoreUint64(&r.lookupCount, 0)
	atomic.StoreUint64(&r.isFullyEnqueued, 0)

	r.resChan = make(chan lookupResult, r.chBuffSize)
	r.backlog = r.backlog[:0]
	r.backlogPos = 0
}

func (r *resultChanWithBacklog) Write(result lookupResult) {
	select {
	case r.resChan <- result:
	default:
		r.mu.Lock()
		defer r.mu.Unlock()

		r.backlog = append(r.backlog, result)
	}

	written := atomic.AddUint64(&r.writeCount, 1)
	fullyEnqueued := atomic.LoadUint64(&r.isFullyEnqueued)

	//log.Println(r.id, "written:", written, "fully_enqueued:", fullyEnqueued)
	if fullyEnqueued == 1 {
		lookupCount := atomic.LoadUint64(&r.lookupCount)

		//log.Println(r.id, "lookup_count:", lookupCount, "written:", written)
		if lookupCount == written {
			close(r.resChan)
		}
	}
}

func (r *resultChanWithBacklog) Read(ctx context.Context) (lookupResult, error) {
	//log.Println(r.id, "read")
	select {
	case res, ok := <-r.resChan:
		if ok {
			//log.Println(r.id, "read from channel")
			return res, nil
		}

	case <-ctx.Done():
		//log.Println(r.id, "ctx.Done() with error:", ctx.Err())
		return lookupResult{}, ctx.Err()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.backlog) > r.backlogPos {
		res := r.backlog[r.backlogPos]
		r.backlogPos++
		//log.Println("read from backlog")
		return res, nil
	}

	//log.Println(r.id, "eof")
	return lookupResult{}, io.EOF
}

type toLookup struct {
	t          types.Tuple
	tupleToRow func(types.Tuple) (sql.Row, error)
	resChan    *resultChanWithBacklog
}

type asyncLookups struct {
	ctx        context.Context
	toLookupCh chan<- toLookup
}

var resChPool = &sync.Pool{
	New: func() interface{} {
		return newResultChanWithBacklog(16, 128)
	},
}

func newAsyncLookups(numWorkers, bufferSize int) *asyncLookups {
	toLookupCh := make(chan toLookup, bufferSize)
	art := &asyncLookups{toLookupCh: toLookupCh}

	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				toLookup, ok := <-toLookupCh

				if !ok {
					panic("async lookup channel closed")
				} else {
					//log.Println(toLookup.resChan.id, "processing lookup")
				}

				r, err := toLookup.tupleToRow(toLookup.t)
				toLookup.resChan.Write(lookupResult{r: r, err: err})
			}
		}()
	}

	return art
}

var lookups *asyncLookups

func init() {
	lookups = newAsyncLookups(1, runtime.NumCPU()*256)
}
