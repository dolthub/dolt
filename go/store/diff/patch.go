// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"bytes"

	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
)

// Patch is a list of difference objects that can be applied to a graph
// using ApplyPatch(). Patch implements a sort order that is useful for
// applying the patch in an efficient way.
type Patch []Difference

func (r Patch) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r Patch) Len() int {
	return len(r)
}

var vals = map[types.DiffChangeType]int{types.DiffChangeRemoved: 0, types.DiffChangeModified: 1, types.DiffChangeAdded: 2}

func (r Patch) Less(i, j int) bool {
	if r[i].Path.Equals(r[j].Path) {
		return vals[r[i].ChangeType] < vals[r[j].ChangeType]
	}
	return pathIsLess(r[i].Path, r[j].Path)
}

// Utility methods on path
// TODO: Should these be on types.Path & types.PathPart?
func pathIsLess(p1, p2 types.Path) bool {
	for i, pp1 := range p1 {
		if len(p2) == i {
			return false // p1 > p2
		}
		switch pathPartCompare(pp1, p2[i]) {
		case -1:
			return true // p1 < p2
		case 1:
			return false // p1 > p2
		}
	}

	return len(p2) > len(p1) // if true p1 < p2, else p1 == p2
}

func fieldPathCompare(pp types.FieldPath, o types.PathPart) int {
	switch opp := o.(type) {
	case types.FieldPath:
		if pp.Name == opp.Name {
			return 0
		}
		if pp.Name < opp.Name {
			return -1
		}
		return 1
	case types.IndexPath:
		return -1
	case types.HashIndexPath:
		return -1
	}
	panic("unreachable")
}

func indexPathCompare(pp types.IndexPath, o types.PathPart) int {
	switch opp := o.(type) {
	case types.FieldPath:
		return 1
	case types.IndexPath:
		if pp.Index.Equals(opp.Index) {
			if pp.IntoKey == opp.IntoKey {
				return 0
			}
			if pp.IntoKey {
				return -1
			}
			return 1
		}
		if pp.Index.Less(opp.Index) {
			return -1
		}
		return 1
	case types.HashIndexPath:
		return -1
	}
	panic("unreachable")
}

func hashIndexPathCompare(pp types.HashIndexPath, o types.PathPart) int {
	switch opp := o.(type) {
	case types.FieldPath:
		return 1
	case types.IndexPath:
		return 1
	case types.HashIndexPath:
		switch bytes.Compare(pp.Hash[:], opp.Hash[:]) {
		case -1:
			return -1
		case 0:
			if pp.IntoKey == opp.IntoKey {
				return 0
			}
			if pp.IntoKey {
				return -1
			}
			return 1
		case 1:
			return 1
		}
	}
	panic("unreachable")
}

func pathPartCompare(pp, pp2 types.PathPart) int {
	switch pp1 := pp.(type) {
	case types.FieldPath:
		return fieldPathCompare(pp1, pp2)
	case types.IndexPath:
		return indexPathCompare(pp1, pp2)
	case types.HashIndexPath:
		return hashIndexPathCompare(pp1, pp2)
	}
	panic("unreachable")
}
