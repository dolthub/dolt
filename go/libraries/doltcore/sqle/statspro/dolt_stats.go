package statspro

import (
	"fmt"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/dolt/go/store/hash"
)

type DoltStats struct {
	mu *sync.Mutex
	// Chunks is a list of addresses for the histogram fanout level
	Chunks []hash.Hash
	// Active maps a chunk/bucket address to its position in
	// the histogram. 1-indexed to differentiate from an empty
	// field on disk
	Active map[hash.Hash]int

	RowCount      uint64
	DistinctCount uint64
	NullCount     uint64
	AvgSize       uint64
	Qual          sql.StatQualifier
	CreatedAt     time.Time
	Histogram     DoltHistogram
	Columns       []string
	Types         []sql.Type
	IdxClass      uint8
	LowerBound    sql.Row
	Fds           *sql.FuncDepSet
	ColSet        sql.ColSet
}

func NewDoltStats() *DoltStats {
	return &DoltStats{mu: &sync.Mutex{}, Active: make(map[hash.Hash]int)}
}

func DoltStatsFromSql(stat sql.Statistic) (*DoltStats, error) {
	hist, err := DoltHistFromSql(stat.Histogram(), stat.Types())
	if err != nil {
		return nil, err
	}
	return &DoltStats{
		mu:            &sync.Mutex{},
		Qual:          stat.Qualifier(),
		RowCount:      stat.RowCount(),
		DistinctCount: stat.DistinctCount(),
		NullCount:     stat.NullCount(),
		AvgSize:       stat.AvgSize(),
		CreatedAt:     stat.CreatedAt(),
		Histogram:     hist,
		Columns:       stat.Columns(),
		Types:         stat.Types(),
		IdxClass:      uint8(stat.IndexClass()),
		LowerBound:    stat.LowerBound(),
		Fds:           stat.FuncDeps(),
		ColSet:        stat.ColSet(),
	}, nil
}

func (s *DoltStats) UpdateActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	newActive := make(map[hash.Hash]int)
	for i, hash := range s.Chunks {
		newActive[hash] = i
	}
	s.Active = newActive
}

func (s *DoltStats) updateCounts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	var newDistinct uint64
	var newRows uint64
	var newNulls uint64
	for _, b := range s.Histogram {
		newDistinct += b.DistinctCount
		newRows += b.RowCount
		newNulls += b.NullCount
	}
	s.RowCount = newRows
	s.DistinctCount = newDistinct
	s.NullCount = newNulls
}

func (s *DoltStats) toSql() sql.Statistic {
	s.mu.Lock()
	defer s.mu.Unlock()
	typStrs := make([]string, len(s.Types))
	for i, typ := range s.Types {
		typStrs[i] = typ.String()
	}
	stat := stats.NewStatistic(s.RowCount, s.DistinctCount, s.NullCount, s.AvgSize, s.CreatedAt, s.Qual, s.Columns, s.Types, s.Histogram.toSql(), sql.IndexClass(s.IdxClass), s.LowerBound)
	return stat.WithColSet(s.ColSet).WithFuncDeps(s.Fds)
}

type DoltHistogram []DoltBucket

type DoltBucket struct {
	Chunk         hash.Hash
	RowCount      uint64
	DistinctCount uint64
	NullCount     uint64
	CreatedAt     time.Time
	Mcvs          []sql.Row
	McvCount      []uint64
	BoundCount    uint64
	UpperBound    sql.Row
}

func DoltHistFromSql(hist sql.Histogram, types []sql.Type) (DoltHistogram, error) {
	ret := make([]DoltBucket, len(hist))
	var err error
	for i, b := range hist {
		upperBound := make(sql.Row, len(b.UpperBound()))
		for i, v := range b.UpperBound() {
			upperBound[i], _, err = types[i].Convert(v)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
			}
		}
		mcvs := make([]sql.Row, len(b.Mcvs()))
		for i, mcv := range b.Mcvs() {
			for _, v := range mcv {
				conv, _, err := types[i].Convert(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert %v to type %s", v, types[i].String())
				}
				mcvs[i] = append(mcvs[i], conv)
			}
		}
		ret[i] = DoltBucket{
			RowCount:      b.RowCount(),
			DistinctCount: b.DistinctCount(),
			NullCount:     b.NullCount(),
			Mcvs:          mcvs,
			McvCount:      b.McvCounts(),
			BoundCount:    b.BoundCount(),
			UpperBound:    upperBound,
		}
	}
	return ret, nil
}

func (s DoltHistogram) toSql() []*stats.Bucket {
	ret := make([]*stats.Bucket, len(s))
	for i, b := range s {
		upperBound := make([]interface{}, len(b.UpperBound))
		copy(upperBound, b.UpperBound)
		ret[i] = stats.NewHistogramBucket(b.RowCount, b.DistinctCount, b.NullCount, b.BoundCount, upperBound, b.McvCount, b.Mcvs)
	}
	return ret
}
