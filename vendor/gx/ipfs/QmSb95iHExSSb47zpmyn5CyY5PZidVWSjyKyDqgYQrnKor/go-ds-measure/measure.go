// Package measure provides a Datastore wrapper that records metrics
// using github.com/ipfs/go-metrics-interface
package measure

import (
	"io"
	"time"

	"gx/ipfs/QmRg1gKTHzc3CZXSKzem8aR4E3TubFhbgXwfVuWnSK5CC5/go-metrics-interface"
	"gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	"gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

var (
	// sort latencies in buckets with following upper bounds in seconds
	datastoreLatencyBuckets = []float64{1e-4, 1e-3, 1e-2, 1e-1}

	// sort sizes in buckets with following upper bounds in bytes
	datastoreSizeBuckets = []float64{1 << 6, 1 << 12, 1 << 18, 1 << 24}
)

// New wraps the datastore, providing metrics on the operations. The
// metrics are registered with names starting with prefix and a dot.
func New(prefix string, ds datastore.Datastore) *measure {
	m := &measure{
		backend: ds,

		putNum: metrics.New(prefix+".put_total", "Total number of Datastore.Put calls").Counter(),
		putErr: metrics.New(prefix+".put.errors_total", "Number of errored Datastore.Put calls").Counter(),
		putLatency: metrics.New(prefix+".put.latency_seconds",
			"Latency distribution of Datastore.Put calls").Histogram(datastoreLatencyBuckets),
		putSize: metrics.New(prefix+".put.size_bytes",
			"Size distribution of stored byte slices").Histogram(datastoreSizeBuckets),

		getNum: metrics.New(prefix+".get_total", "Total number of Datastore.Get calls").Counter(),
		getErr: metrics.New(prefix+".get.errors_total", "Number of errored Datastore.Get calls").Counter(),
		getLatency: metrics.New(prefix+".get.latency_seconds",
			"Latency distribution of Datastore.Get calls").Histogram(datastoreLatencyBuckets),
		getSize: metrics.New(prefix+".get.size_bytes",
			"Size distribution of retrieved byte slices").Histogram(datastoreSizeBuckets),

		hasNum: metrics.New(prefix+".has_total", "Total number of Datastore.Has calls").Counter(),
		hasErr: metrics.New(prefix+".has.errors_total", "Number of errored Datastore.Has calls").Counter(),
		hasLatency: metrics.New(prefix+".has.latency_seconds",
			"Latency distribution of Datastore.Has calls").Histogram(datastoreLatencyBuckets),

		deleteNum: metrics.New(prefix+".delete_total", "Total number of Datastore.Delete calls").Counter(),
		deleteErr: metrics.New(prefix+".delete.errors_total", "Number of errored Datastore.Delete calls").Counter(),
		deleteLatency: metrics.New(prefix+".delete.latency_seconds",
			"Latency distribution of Datastore.Delete calls").Histogram(datastoreLatencyBuckets),

		queryNum: metrics.New(prefix+".query_total", "Total number of Datastore.Query calls").Counter(),
		queryErr: metrics.New(prefix+".query.errors_total", "Number of errored Datastore.Query calls").Counter(),
		queryLatency: metrics.New(prefix+".query.latency_seconds",
			"Latency distribution of Datastore.Query calls").Histogram(datastoreLatencyBuckets),
	}
	return m
}

type measure struct {
	backend datastore.Datastore

	putNum     metrics.Counter
	putErr     metrics.Counter
	putLatency metrics.Histogram
	putSize    metrics.Histogram

	getNum     metrics.Counter
	getErr     metrics.Counter
	getLatency metrics.Histogram
	getSize    metrics.Histogram

	hasNum     metrics.Counter
	hasErr     metrics.Counter
	hasLatency metrics.Histogram

	deleteNum     metrics.Counter
	deleteErr     metrics.Counter
	deleteLatency metrics.Histogram

	queryNum     metrics.Counter
	queryErr     metrics.Counter
	queryLatency metrics.Histogram
}

func recordLatency(h metrics.Histogram, start time.Time) {
	elapsed := time.Since(start)
	h.Observe(elapsed.Seconds())
}

func (m *measure) Put(key datastore.Key, value interface{}) error {
	defer recordLatency(m.putLatency, time.Now())
	m.putNum.Inc()
	if b, ok := value.([]byte); ok {
		m.putSize.Observe(float64(len(b)))
	}
	err := m.backend.Put(key, value)
	if err != nil {
		m.putErr.Inc()
	}
	return err
}

func (m *measure) Get(key datastore.Key) (value interface{}, err error) {
	defer recordLatency(m.getLatency, time.Now())
	m.getNum.Inc()
	value, err = m.backend.Get(key)
	if err != nil {
		m.getErr.Inc()
	} else {
		if b, ok := value.([]byte); ok {
			m.getSize.Observe(float64(len(b)))
		}
	}
	return value, err
}

func (m *measure) Has(key datastore.Key) (exists bool, err error) {
	defer recordLatency(m.hasLatency, time.Now())
	m.hasNum.Inc()
	exists, err = m.backend.Has(key)
	if err != nil {
		m.hasErr.Inc()
	}
	return exists, err
}

func (m *measure) Delete(key datastore.Key) error {
	defer recordLatency(m.deleteLatency, time.Now())
	m.deleteNum.Inc()
	err := m.backend.Delete(key)
	if err != nil {
		m.deleteErr.Inc()
	}
	return err
}

func (m *measure) Query(q query.Query) (query.Results, error) {
	defer recordLatency(m.queryLatency, time.Now())
	m.queryNum.Inc()
	res, err := m.backend.Query(q)
	if err != nil {
		m.queryErr.Inc()
	}
	return res, err
}

type measuredBatch struct {
	puts    int
	deletes int

	putts datastore.Batch
	delts datastore.Batch

	m *measure
}

func (m *measure) Batch() (datastore.Batch, error) {
	bds, ok := m.backend.(datastore.Batching)
	if !ok {
		return nil, datastore.ErrBatchUnsupported
	}
	pb, err := bds.Batch()
	if err != nil {
		return nil, err
	}

	db, err := bds.Batch()
	if err != nil {
		return nil, err
	}

	return &measuredBatch{
		putts: pb,
		delts: db,

		m: m,
	}, nil
}

func (mt *measuredBatch) Put(key datastore.Key, val interface{}) error {
	mt.puts++
	valb, ok := val.([]byte)
	if ok {
		mt.m.putSize.Observe(float64(len(valb)))
	}
	return mt.putts.Put(key, val)
}

func (mt *measuredBatch) Delete(key datastore.Key) error {
	mt.deletes++
	return mt.delts.Delete(key)
}

func (mt *measuredBatch) Commit() error {
	err := logBatchCommit(mt.delts, mt.deletes, mt.m.deleteNum, mt.m.deleteErr, mt.m.deleteLatency)
	if err != nil {
		return err
	}

	err = logBatchCommit(mt.putts, mt.puts, mt.m.putNum, mt.m.putErr, mt.m.putLatency)
	if err != nil {
		return err
	}

	return nil
}

func logBatchCommit(b datastore.Batch, n int, num, errs metrics.Counter, lat metrics.Histogram) error {
	if n > 0 {
		before := time.Now()
		err := b.Commit()
		took := time.Since(before) / time.Duration(n)
		num.Add(float64(n))
		for i := 0; i < n; i++ {
			lat.Observe(took.Seconds())
		}
		if err != nil {
			errs.Inc()
			return err
		}
	}
	return nil
}

func (m *measure) Close() error {
	if c, ok := m.backend.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
