// Copyright 2020 Liquidata, Inc.
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

package schema

import "strconv"

type InnerIndex interface {
	// GetColumn returns the column for the given tag and whether the column was found or not.
	GetColumn(tag uint64) (Column, bool)
	// Name returns the name of the inner index.
	Name() string
	// OuterIndex returns the full index that this inner index belongs to.
	OuterIndex() Index
	// PrimaryKeys returns the primary keys of the indexed table, in the order that they're stored for that table.
	PrimaryKeys() []uint64
	// Schema returns the schema for the internal index map. Can be used for table operations.
	Schema() Schema
	// Tags returns the tags of the columns in the index.
	Tags() []uint64
}

var _ InnerIndex = (*innerIndexImpl)(nil)

type innerIndexImpl struct {
	index     *indexImpl
	tagLength int
}

func (ix *innerIndexImpl) GetColumn(tag uint64) (Column, bool) {
	return ix.index.GetColumn(tag)
}

func (ix *innerIndexImpl) Name() string {
	return ix.index.Name() + strconv.FormatInt(int64(ix.tagLength), 10)
}

func (ix *innerIndexImpl) OuterIndex() Index {
	return ix.index
}

func (ix *innerIndexImpl) PrimaryKeys() []uint64 {
	return ix.index.PrimaryKeys()
}

func (ix *innerIndexImpl) Schema() Schema {
	return ix.index.Schema()
}

func (ix *innerIndexImpl) Tags() []uint64 {
	return ix.index.tags[:ix.tagLength]
}
