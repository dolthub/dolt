package doltdb

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

const (
	metaField      = "meta"
	parentsField   = "parents"
	rootValueField = "value"
)

// Commit contains information on a commit that was written to noms
type Commit struct {
	vrw      types.ValueReadWriter
	commitSt types.Struct
}

// HashOf returns the hash of the commit
func (c *Commit) HashOf() hash.Hash {
	return c.commitSt.Hash()
}

// GetCommitMeta gets the metadata associated with the commit
func (c *Commit) GetCommitMeta() *CommitMeta {
	metaVal := c.commitSt.Get(metaField)

	if metaVal != nil {
		if metaSt, ok := metaVal.(types.Struct); ok {
			cm, err := commitMetaFromNomsSt(metaSt)

			if err == nil {
				return cm
			}
		}
	}

	panic(c.HashOf().String() + " is a commit without the required metadata.")
}

func (c *Commit) getParents() types.Set {
	if parVal := c.commitSt.Get(parentsField); parVal != nil {
		return parVal.(types.Set)
	}

	return types.EmptySet
}

// NumParents gets the number of parents a commit has.
func (c *Commit) NumParents() int {
	parents := c.getParents()
	return int(parents.Len())
}

func (c *Commit) getParent(idx int) *types.Struct {
	parentSet := c.getParents()

	itr := parentSet.IteratorAt(uint64(idx))
	parentVal := itr.Next()

	if parentVal == nil {
		return nil
	}

	parentRef := parentVal.(types.Ref)
	parentSt := parentRef.TargetValue(c.vrw).(types.Struct)
	return &parentSt
}

// GetRootValue gets the RootValue of the commit.
func (c *Commit) GetRootValue() *RootValue {
	rootVal := c.commitSt.Get(rootValueField)

	if rootVal != nil {
		if rootSt, ok := rootVal.(types.Struct); ok {
			return newRootValue(c.vrw, rootSt)
		}
	}

	panic(c.HashOf().String() + " is a commit without a value.")
}
