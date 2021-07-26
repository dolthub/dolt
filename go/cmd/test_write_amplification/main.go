package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"math/rand"
	"runtime"
	"sync/atomic"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

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
var Perc = flag.Float64("perc", 0.01, "percentage of keys to measure write amplification for deleting")
var Rewrite = flag.Bool("rewrite", false, "if true, rewrite the map and run the test on the rewritten map")

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

	if *Rewrite {
		types.TestRewrite = true
	}

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

const MaxProcs = 32

func benchmark_ref(ctx context.Context, m types.Map, r types.Ref, vrw types.ValueReadWriter) {
	numprocs := runtime.GOMAXPROCS(0)

	if numprocs > MaxProcs {
		panic("error, GOMAXPROCS is greater than MaxProcs. Please increase MaxProcs")
	}

	originalHashes := newHashset()
	var ogchunksizes, rewritechunksizes inthist
	next := get_leaves(ctx, []types.Ref{r}, vrw, originalHashes, &ogchunksizes)
	fmt.Println("num leaves", len(next), "num chunks", len(originalHashes))

	var rewritemapeditor *types.MapEditor
	if *Rewrite {
		rewritemap, err := types.NewMap(ctx, vrw)
		if err != nil {
			panic(err)
		}
		rewritemapeditor = types.NewMapEditor(rewritemap)
	}

	rd := rand.New(rand.NewSource(int64(*Seed)))

	w := len(fmt.Sprintf("%d", len(next)))
	fmtstr := fmt.Sprintf("\033[G\033[K %%%dd / %%d", w)
	fmt.Println("sampling keys from leaves")

	i := 0
	var todelete []types.Tuple
	leaves(ctx, next, vrw, func(m types.Map) {
		ogchunksizes.add(m.EncodedLen())
		key := true
		var lastkey types.Tuple
		m.WalkValues(ctx, func(v types.Value) error {
			if key {
				lastkey = v.(types.Tuple)
				if rd.Float64() < *Perc {
					todelete = append(todelete, lastkey)
				}
			} else if *Rewrite {
				rewritemapeditor.Set(lastkey, v)
			}
			key = !key
			return nil
		})
		i += 1
		fmt.Printf(fmtstr, i, len(next))
	})
	fmt.Printf(fmtstr, i, len(next))
	fmt.Printf("\n")

	if *Rewrite {
		fmt.Println("flushing rewrite map editor")
		rewritemap, err := rewritemapeditor.Map(ctx)
		if err != nil {
			panic(err)
		}
		var rs []types.Ref
		rewritemap.WalkRefs(rewritemap.Format(), func(r types.Ref) error {
			rs = append(rs, r)
			return nil
		})
		originalHashes = newHashset()
		next = get_leaves(ctx, rs, vrw, originalHashes, &rewritechunksizes)
		i = 0
		fmt.Println("getting rewrite map chunk sizes")
		w := len(fmt.Sprintf("%d", len(next)))
		fmtstr := fmt.Sprintf("\033[G\033[K %%%dd / %%d", w)
		leaves(ctx, next, vrw, func(m types.Map) {
			rewritechunksizes.add(m.EncodedLen())
			i += 1
			fmt.Printf(fmtstr, i, len(next))
		})
		fmt.Printf(fmtstr, i, len(next))
		fmt.Printf("\n")

		m = rewritemap
	}

	var numchunksa, chunksizesa [MaxProcs]inthist
	var is [MaxProcs]int64
	eg, ctx := errgroup.WithContext(ctx)

	w = len(fmt.Sprintf("%d", len(todelete)))
	fmtstr = fmt.Sprintf("\033[G\033[K %%%dd / %%d", w)

	fmt.Println("key tuples", len(todelete))
	fmt.Println("running deletes and measuring deltas")

	numtodelete := len(todelete)

	stride := numtodelete / numprocs
	if stride < 16 {
		numprocs = 1
		stride = numtodelete
	}

	for j := 0; j < numprocs; j++ {
		ftodelete := todelete
		if j != numprocs - 1 {
			ftodelete = todelete[:stride]
			todelete = todelete[stride:]
		}
		fi := &is[j]
		numchunks := &numchunksa[j]
		chunksizes := &chunksizesa[j]

		eg.Go(func() error {
			for _, k := range ftodelete {
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
				atomic.AddInt64(fi, 1)
			}
			return nil
		})
	}

	geti := func() int {
		var res int64
		for j := 0; j < len(is); j++ {
			res += atomic.LoadInt64(&is[j])
		}
		return int(res)
	}

	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quit:
				i := geti()
				fmt.Printf(fmtstr, i, numtodelete)
				return
			case <-time.After(1 * time.Second):
				i := geti()
				fmt.Printf(fmtstr, i, numtodelete)
			}
		}
	}()

	eg.Wait()
	close(quit)
	wg.Wait()

	fmt.Printf("\n")

	for j := 1; j < numprocs; j++ {
		numchunksa[0].merge(numchunksa[j])
		chunksizesa[0].merge(chunksizesa[j])
	}

	if *Rewrite {
		fmt.Printf("rewrite chunk sizes: %s\n", rewritechunksizes.String())
	}
	fmt.Printf("og chunk sizes:      %s\n", ogchunksizes.String())
	fmt.Printf("new chunks:          %s\n", numchunksa[0].String())
	fmt.Printf("bytes written:       %s\n", chunksizesa[0].String())
}

type inthist struct {
	vs     []int
	sorted bool
}

func (h *inthist) add(i int) {
	h.vs = append(h.vs, i)
	h.sorted = false
}

func (h *inthist) merge(hp inthist) {
	h.vs = append(h.vs, hp.vs...)
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

func (h *inthist) avg() float64 {
	// we can just use an int64 here because these are byte sizes of things
	// that exist on a disk...
	var sum int64
	for _, v := range h.vs {
		sum += int64(v)
	}
	return float64(sum) / float64(len(h.vs))
}

func (h *inthist) String() string {
	return fmt.Sprintf("avg: %10.2f, p10: %10d, p50: %10d, p90: %10d, p99: %10d, p99.9: %10d, p100: %10d", h.avg(), h.perc(.1), h.perc(.5), h.perc(.9), h.perc(.99), h.perc(.999), h.perc(1))
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

func get_leaves(ctx context.Context, rs []types.Ref, vrw types.ValueReadWriter, hs hashset, chunksizes *inthist) []types.Ref {
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
			chunksizes.add(m.EncodedLen())
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
