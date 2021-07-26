package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"math/rand"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/hash"
)

var Dir = flag.String("dir", "/Users/aaronson/dolt_clone/city-populations", "directory of the repository")
var Branch = flag.String("branch", "master", "branch of the repository")
var Table = flag.String("table", "", "table to test against")
var Seed = flag.Int("seed", 1, "seed to use for rng key selector")
var Perc = flag.Int("perc", 1, "percentage of keys to measure write amplification for deleting")

func GetTableNames(ctx context.Context, dir, branch string) (*doltdb.DoltDB, *doltdb.RootValue, []string) {
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, "file://"+dir+"/.dolt/noms", "0.0.0-test_tuples")
	db := dEnv.DoltDB
	c, err := db.ResolveCommitRef(ctx, ref.NewBranchRef(branch))
	if err != nil {
		panic("could not resolve commit ref: " + err.Error())
	}
	r, err := c.GetRootValue()
	if err != nil {
		panic("could not resolve get root value: " + err.Error())
	}

	names, err := r.GetTableNames(ctx)
	if err != nil {
		panic("could not get table names: " + err.Error())
	}

	return db, r, names
}

func GetTableDataRef(ctx context.Context, r *doltdb.RootValue, table string) (*doltdb.Table, types.Map, types.Ref) {
	t, _, err := r.GetTable(ctx, table)
	if err != nil {
		panic("could not resolve get table \"" + table + "\": " + err.Error())
	}

	rd, err := t.GetRowData(ctx)
	if err != nil {
		panic("could not resolve get row data for table \"" + table + "\": " + err.Error())
	}

	mr, err := types.NewRef(rd, rd.Format())
	if err != nil {
		panic("could not make ref of map: " + err.Error())
	}

	return t, rd, mr
}

func main() {
	flag.Parse()

	ctx := context.Background()

	db, rv, tablenames := GetTableNames(ctx, *Dir, *Branch)
	if *Table == "" {
		for _, name := range tablenames {
			fmt.Println(name)
		}
		return
	}
	RunAmplificationTest(ctx, db, rv, *Table)
}

func RunAmplificationTest(ctx context.Context, db *doltdb.DoltDB, rv *doltdb.RootValue, table string) {
	fmt.Println("benchmarking table", table)

	_, tm, tdr := GetTableDataRef(ctx, rv, table)
	benchmark_ref(ctx, tm, tdr, db.ValueReadWriter())
}

func benchmark_ref(ctx context.Context, m types.Map, r types.Ref, vrw types.ValueReadWriter) {
	originalHashes := newHashset()
	next := get_leaves(ctx, []types.Ref{r}, vrw, originalHashes)
	fmt.Println("num leaves", len(next), "num chunks", len(originalHashes))

	var todelete []types.Tuple
	rd := rand.New(rand.NewSource(int64(*Seed)))

	leaves(ctx, next, vrw, func(m types.Map) {
		key := true
		m.WalkValues(ctx, func(v types.Value) error {
			if key && rd.Intn(100) < *Perc {
				t := v.(types.Tuple)
				todelete = append(todelete, t)
			}
			key = !key
			return nil
		})
	})

	var numchunks, chunksizes inthist

	fmt.Println("key tuples", len(todelete))
	for i, k := range todelete {
		nm, err := m.Edit().Remove(k).Map(ctx)
		if err != nil {
			panic(err)
		}
		var rs []types.Ref
		nm.WalkRefs(nm.Format(), func(r types.Ref) error {
			rs = append(rs, r)
			return nil
		})
		newchunks, newchunksizes := get_delta(ctx, rs, vrw, originalHashes)
		newchunks += 1
		newchunksizes += nm.EncodedLen()
		numchunks.add(newchunks)
		chunksizes.add(newchunksizes)
		if i % 100 == 99 {
			fmt.Println(i, "/", len(todelete))
		}
	}

	fmt.Printf("new chunks: p10: %d, p50: %d, p90: %d, p99: %d, p99.9: %d, p100: %d\n", numchunks.perc(.1), numchunks.perc(.5), numchunks.perc(.9), numchunks.perc(.99), numchunks.perc(.999), numchunks.perc(1))
	fmt.Printf("bytes written: p10: %d, p50: %d, p90: %d, p99: %d, p99.9: %d, p100: %d\n", chunksizes.perc(.1), chunksizes.perc(.5), chunksizes.perc(.9), chunksizes.perc(.99), chunksizes.perc(.999), chunksizes.perc(1))
}

type inthist struct {
	vs     []int
	sorted bool
}

func (h *inthist) add(i int) {
	h.vs = append(h.vs, i)
	h.sorted = false
}

func (h *inthist) perc(p float32) int {
	if !h.sorted {
		sort.Ints(h.vs)
		h.sorted = true
	}
	i := int(float32(len(h.vs)) * p)
	if i >= len(h.vs) {
		i = len(h.vs) - 1
	}
	return h.vs[i]
}

type hashset map[[20]byte]struct{}

func newHashset() hashset {
	return make(hashset)
}

func (s hashset) add(h hash.Hash) {
	s[h] = struct{}{}
}

func (s hashset) has(h hash.Hash) bool {
	_, b := s[h]
	return b
}

func get_delta(ctx context.Context, rs []types.Ref, vrw types.ValueReadWriter, oghs hashset) (int, int) {
	next := rs
	newchunks := 0
	newchunksizes := 0
	for len(next) > 0 {
		cur := next
		next = make([]types.Ref, 0)
		for _, r := range cur {
			if oghs.has(r.TargetHash()) {
				continue
			}
			v, err := r.TargetValue(ctx, vrw)
			if err != nil {
				panic(err)
			}

			newchunks += 1
			m := v.(types.Map)
			newchunksizes += m.EncodedLen()
			if r.Height() == 1 {
				continue
			}
			m.WalkRefs(m.Format(), func(r types.Ref) error {
				next = append(next, r)
				return nil
			})
		}
	}
	return newchunks, newchunksizes
}

func get_leaves(ctx context.Context, rs []types.Ref, vrw types.ValueReadWriter, hs hashset) []types.Ref {
	res := make([]types.Ref, 0)
	next := rs
	for len(next) > 0 {
		cur := next
		next = make([]types.Ref, 0)
		for _, r := range cur {
			hs.add(r.TargetHash())
			if r.Height() == 1 {
				res = append(res, r)
				continue
			}
			v, err := r.TargetValue(ctx, vrw)
			if err != nil {
				panic(err)
			}
			m := v.(types.Map)
			m.WalkRefs(m.Format(), func(r types.Ref) error {
				next = append(next, r)
				return nil
			})
		}
	}
	return res
}

func leaves(ctx context.Context, rs []types.Ref, vrw types.ValueReadWriter, cb func(types.Map)) {
	for _, r := range rs {
		v, err := r.TargetValue(ctx, vrw)
		if err != nil {
			panic(err)
		}
		cb(v.(types.Map))
	}
}
