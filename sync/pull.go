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

func validateRefAsSetOfCommit(r ref.Ref, cs chunks.ChunkSource) types.Value {
	v, err := types.ReadValue(r, cs)
	d.Exp.NotNil(v, "%v cannot be found", r)
	d.Exp.NoError(err)

	// TODO: Replace this weird recover stuff below once we have a way to determine if a Value is an instance of a custom struct type. BUG #133
	defer func() {
		if r := recover(); r != nil {
			d.Exp.Fail("Not a SetOfCommit:", "%+v", v)
		}
	}()
	datas.SetOfCommitFromVal(v)
	return v
}

// DiffHeadsByRef takes two Refs, validates that both refer to Heads in the given ChunkSource, and then returns the set of Refs that can be reached from 'big', but not 'small'.
func DiffHeadsByRef(small, big ref.Ref, cs chunks.ChunkSource) []ref.Ref {
	validateRefAsSetOfCommit(small, cs)
	validateRefAsSetOfCommit(big, cs)
	return walk.Difference(small, big, cs)
}

// CopyChunks reads each Ref in refs out of src and writes it into sink.
func CopyChunks(refs []ref.Ref, src chunks.ChunkSource, sink chunks.ChunkSink) {
	for _, ref := range refs {
		reader := src.Get(ref)
		d.Exp.NotNil(reader, "Attempt to copy ref which wasn't found: %+v", ref)

		writer := sink.Put()
		defer writer.Close()
		_, err := io.Copy(writer, reader)
		d.Exp.NoError(err)
	}
	return
}

// SetNewHeads takes the Ref of the desired new Heads of ds, the chunk for which should already exist in the Dataset. It validates that the Ref points to an existing chunk that decodes to the correct type of value and then commits it to ds.
func SetNewHeads(newHeadRef ref.Ref, ds dataset.Dataset) dataset.Dataset {
	v := validateRefAsSetOfCommit(newHeadRef, ds)
	return ds.Commit(datas.SetOfCommitFromVal(v))
}
