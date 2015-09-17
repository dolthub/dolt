package sync

import (
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

func validateRefAsCommit(r ref.Ref, cs chunks.ChunkSource) datas.Commit {
	v := types.ReadValue(r, cs)

	d.Exp.NotNil(v, "%v cannot be found", r)

	// TODO: Replace this weird recover stuff below once we have a way to determine if a Value is an instance of a custom struct type. BUG #133
	defer func() {
		if r := recover(); r != nil {
			d.Exp.Fail("Not a Commit:", "%+v", v)
		}
	}()
	return datas.CommitFromVal(v)
}

// SetNewHead takes the Ref of the desired new Head of ds, the chunk for which should already exist in the Dataset. It validates that the Ref points to an existing chunk that decodes to the correct type of value and then commits it to ds, returning a new Dataset with newHeadRef set and ok set to true. In the event that the commit fails, ok is set to false and a new up-to-date Dataset is returned WITHOUT newHeadRef in it. The caller should try again using this new Dataset.
func SetNewHead(newHeadRef ref.Ref, ds dataset.Dataset) (dataset.Dataset, bool) {
	commit := validateRefAsCommit(newHeadRef, ds.Store())
	return ds.CommitWithParents(commit.Value(), datas.SetOfCommitFromVal(commit.Parents()))
}

// Copys all chunks reachable from (and including) |r| but excluding all chunks reachable from (and including) |exclude| in |source| to |sink|.
func CopyReachableChunksP(r, exclude ref.Ref, source chunks.ChunkSource, sink chunks.ChunkSink, concurrency int) {
	excludeRefs := map[ref.Ref]bool{}
	hasRef := func(r ref.Ref) bool {
		return excludeRefs[r]
	}

	if exclude != (ref.Ref{}) {
		refChan := make(chan ref.Ref, 1024)
		addRef := func(r ref.Ref) {
			refChan <- r
		}

		go func() {
			walk.AllP(exclude, source, addRef, concurrency)
			close(refChan)
		}()

		for r := range refChan {
			excludeRefs[r] = true
		}
	}

	tcs := &teeChunkSource{source, sink}
	walk.SomeP(r, tcs, hasRef, concurrency)
}

// teeChunkSource just serves the purpose of writing to |sink| every chunk that is read from |source|.
type teeChunkSource struct {
	source chunks.ChunkSource
	sink   chunks.ChunkSink
}

func (trs *teeChunkSource) Get(ref ref.Ref) io.ReadCloser {
	r := trs.source.Get(ref)
	if r == nil {
		return nil
	}

	w := trs.sink.Put()
	tr := io.TeeReader(r, w)
	return forwardCloser{tr, []io.Closer{r, w}}
}

func (trs *teeChunkSource) Has(ref ref.Ref) bool {
	return trs.source.Has(ref)
}

// forwardCloser closes multiple io.Closer objects.
type forwardCloser struct {
	io.Reader
	cs []io.Closer
}

func (fc forwardCloser) Close() error {
	for _, c := range fc.cs {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}
