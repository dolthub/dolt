// Copyright 2026 Dolthub, Inc.
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

package diff

import (
	"bytes"
	"context"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// MovedMembers finds rows that a diff reports as modified only because their
// ENUM or SET members are stored at other positions.
//
// ALTER TABLE rewrites the stored values of a column whose members are
// reordered, so the bytes of each row change while its values do not.
type MovedMembers struct {
	ns     tree.NodeStore
	fromVD *val.TupleDesc
	toVD   *val.TupleDesc
	// fields holds the value fields whose members moved, in stored order. It
	// is empty when no members moved or the sides differ in layout.
	fields []movedMemberField
}

// movedMemberField is a value field whose members are stored at other
// positions on the to side.
type movedMemberField struct {
	// idx is the index of the field in the value tuples of both sides.
	idx int
	// members remaps the field from the from side to the to side.
	members sql.MemberMap
}

// NewMovedMembers returns the MovedMembers of a diff from |fromSch| to
// |toSch|, which reads rows from |ns|. The result is empty for keyless
// tables, for sides whose value columns differ in tag or encoding, and when
// no member moved.
func NewMovedMembers(ns tree.NodeStore, fromSch, toSch schema.Schema) MovedMembers {
	if schema.IsKeyless(fromSch) || schema.IsKeyless(toSch) {
		return MovedMembers{}
	}
	fromCols := fromSch.GetNonPKCols().GetStoredColumns()
	toCols := toSch.GetNonPKCols().GetStoredColumns()
	if len(fromCols) != len(toCols) {
		return MovedMembers{}
	}
	var fields []movedMemberField
	for i, from := range fromCols {
		to := toCols[i]
		if from.Tag != to.Tag || from.TypeInfo.Encoding() != to.TypeInfo.Encoding() {
			return MovedMembers{}
		}
		if members, ok := from.MapMembers(to); ok && !members.IsIdentity() {
			fields = append(fields, movedMemberField{idx: i, members: members})
		}
	}
	if len(fields) == 0 {
		return MovedMembers{}
	}
	_, fromVD := fromSch.GetMapDescriptors(ns)
	_, toVD := toSch.GetMapDescriptors(ns)
	return MovedMembers{ns: ns, fromVD: fromVD, toVD: toVD, fields: fields}
}

// HasMoved reports whether any members moved. Equal bytes then no longer mean
// equal values, so [prolly.DiffMaps] must consider every row modified, and
// each row must pass IsUnchanged.
func (m MovedMembers) HasMoved() bool {
	return len(m.fields) > 0
}

// IsUnchanged reports whether |d| is a modified row whose values are the same
// on both sides, with only the positions of its members moved.
func (m MovedMembers) IsUnchanged(ctx context.Context, d tree.Diff) (bool, error) {
	if !m.HasMoved() || d.Type != tree.ModifiedDiff {
		return false, nil
	}
	from, to := val.Tuple(d.From), val.Tuple(d.To)
	fields := m.fields
	for i := 0; i < m.toVD.Count(); i++ {
		if len(fields) > 0 && fields[0].idx == i {
			isSame, err := m.isSameMember(ctx, fields[0], from, to)
			if err != nil || !isSame {
				return false, err
			}
			fields = fields[1:]
			continue
		}
		if !bytes.Equal(m.fromVD.GetField(i, from), m.toVD.GetField(i, to)) {
			return false, nil
		}
	}
	return true, nil
}

// HasChange reports whether any row differs between |from| and |to|, which
// share one primary key.
//
// It compares map hashes unless members moved. Otherwise it walks every row
// and stops at the first change.
func (m MovedMembers) HasChange(ctx context.Context, from, to prolly.Map) (bool, error) {
	if !m.HasMoved() {
		return from.HashOf() != to.HashOf(), nil
	}
	hasChange := false
	err := prolly.DiffMaps(ctx, from, to, true, func(ctx context.Context, d tree.Diff) error {
		isUnchanged, err := m.IsUnchanged(ctx, d)
		if err != nil || isUnchanged {
			return err
		}
		hasChange = true
		return io.EOF
	})
	if err != nil && err != io.EOF {
		return false, err
	}
	return hasChange, nil
}

// isSameMember reports whether |field| of |from| and of |to| holds the same
// members, or is NULL in both.
func (m MovedMembers) isSameMember(ctx context.Context, field movedMemberField, from, to val.Tuple) (bool, error) {
	fromStored, err := tree.GetField(ctx, m.fromVD, field.idx, from, m.ns)
	if err != nil {
		return false, err
	}
	toStored, err := tree.GetField(ctx, m.toVD, field.idx, to, m.ns)
	if err != nil {
		return false, err
	}
	if fromStored == nil || toStored == nil {
		return fromStored == nil && toStored == nil, nil
	}
	remapped, ok := field.members.Remap(fromStored)
	return ok && remapped == toStored, nil
}
