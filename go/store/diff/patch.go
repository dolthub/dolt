// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"bytes"

	"github.com/liquidata-inc/dolt/go/store/types"
)

// Patch is a list of difference objects that can be applied to a graph
// using ApplyPatch(). Patch implements a sort order that is useful for
// applying the patch in an efficient way.
type Patch []Difference

type PatchSort struct {
	patch Patch
	nbf   *types.NomsBinFormat
}

func (ps PatchSort) Swap(i, j int) {
	ps.patch[i], ps.patch[j] = ps.patch[j], ps.patch[i]
}

func (ps PatchSort) Len() int {
	return len(ps.patch)
}

var vals = map[types.DiffChangeType]int{types.DiffChangeRemoved: 0, types.DiffChangeModified: 1, types.DiffChangeAdded: 2}

func (ps PatchSort) Less(i, j int) bool {
	if ps.patch[i].Path.Equals(ps.patch[j].Path) {
		return vals[ps.patch[i].ChangeType] < vals[ps.patch[j].ChangeType]
	}
	return pathIsLess(ps.nbf, ps.patch[i].Path, ps.patch[j].Path)
}

// Utility methods on path
// TODO: Should these be on types.Path & types.PathPart?
func pathIsLess(nbf *types.NomsBinFormat, p1, p2 types.Path) bool {
	for i, pp1 := range p1 {
		if len(p2) == i {
			return false // p1 > p2
		}
		switch pathPartCompare(nbf, pp1, p2[i]) {
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

func indexPathCompare(nbf *types.NomsBinFormat, pp types.IndexPath, o types.PathPart) int {
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
		if pp.Index.Less(nbf, opp.Index) {
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

func pathPartCompare(nbf *types.NomsBinFormat, pp, pp2 types.PathPart) int {
	switch pp1 := pp.(type) {
	case types.FieldPath:
		return fieldPathCompare(pp1, pp2)
	case types.IndexPath:
		return indexPathCompare(nbf, pp1, pp2)
	case types.HashIndexPath:
		return hashIndexPathCompare(pp1, pp2)
	}
	panic("unreachable")
}
