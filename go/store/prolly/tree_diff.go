// Copyright 2021 Dolthub, Inc.
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

package prolly

import "context"


type DiffType byte

const (
	addedDiff    DiffType = 0
	modifiedDiff DiffType = 1
	removedDiff  DiffType = 2
)

type nodeDiffIter interface {
	next(ctx context.Context) (nodeDiff, error)
}

type nodeDiff struct {
	from, to Node
	typ      DiffType
}

func diffTrees(ctx context.Context, ns NodeStore, from, to Node, cmp compareFn) nodeDiffIter {
	return treeDiffer{}
}

type treeDiffer struct {
	from, to *nodeCursor
	cmp      compareFn
}

var _ nodeDiffIter = treeDiffer{}

func (td treeDiffer) next(ctx context.Context) (d nodeDiff, err error) {
	return
}
