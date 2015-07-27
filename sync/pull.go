package sync

import (
	"fmt"
	"io"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

func validateRefAsSetOfCommit(r ref.Ref, cs chunks.ChunkSource) (v types.Value, err error) {
	v, err = types.ReadValue(r, cs)
	if v == nil {
		return nil, fmt.Errorf("%v cannot be found", r)
	} else if err != nil {
		return nil, err
	}
	// TODO: Replace this weird recover stuff below once we have a way to determine if
	// a Value is an instance of a custom struct type. BUG #133
	err = fmt.Errorf("%+v is not a SetOfCommit", v)
	defer func() { recover() }()
	datas.SetOfCommitFromVal(v) // If this panics the return value will be the error above
	return v, nil
}

// DiffHeadsByRef takes two Refs, validates that both refer to Heads in the given ChunkSource, and then returns the set of Refs that can be reached from 'big', but not 'small'.
func DiffHeadsByRef(small, big ref.Ref, cs chunks.ChunkSource) ([]ref.Ref, error) {
	if _, err := validateRefAsSetOfCommit(small, cs); err != nil {
		return nil, err
	}
	if _, err := validateRefAsSetOfCommit(big, cs); err != nil {
		return nil, err
	}
	return walk.Difference(small, big, cs), nil

}

// CopyChunks reads each Ref in refs out of src and writes it into sink.
func CopyChunks(refs []ref.Ref, src chunks.ChunkSource, sink chunks.ChunkSink) error {
	for _, ref := range refs {
		reader, err := src.Get(ref)
		if reader == nil {
			return fmt.Errorf("Got back nil for %+v", ref)
		} else if err != nil {
			return err
		}
		writer := sink.Put()
		defer writer.Close()
		_, err = io.Copy(writer, reader)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetNewHeads takes the Ref of the desired new Heads of ds, the chunk for which should already exist in the Dataset. It validates that the Ref points to an existing chunk that decodes to the correct type of value and then commits it to ds.
func SetNewHeads(newHeadRef ref.Ref, ds dataset.Dataset) (dataset.Dataset, error) {
	v, err := validateRefAsSetOfCommit(newHeadRef, ds)
	if err != nil {
		return ds, err
	}
	return ds.Commit(datas.SetOfCommitFromVal(v)), nil
}
