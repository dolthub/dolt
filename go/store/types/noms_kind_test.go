// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNomsKind(t *testing.T) {
	assert := assert.New(t)

	t.Run("assert that all kinds are the right number", func(*testing.T) {
		assert.Equal(0, int(BoolKind))
		assert.Equal(1, int(FloatKind))
		assert.Equal(2, int(StringKind))
		assert.Equal(3, int(BlobKind))
		assert.Equal(4, int(ValueKind))
		assert.Equal(5, int(ListKind))
		assert.Equal(6, int(MapKind))
		assert.Equal(7, int(RefKind))
		assert.Equal(8, int(SetKind))
		assert.Equal(9, int(StructKind))
		assert.Equal(10, int(CycleKind))
		assert.Equal(11, int(TypeKind))
		assert.Equal(12, int(UnionKind))
		assert.Equal(13, int(hashKind))
		assert.Equal(14, int(UUIDKind))
		assert.Equal(15, int(IntKind))
		assert.Equal(16, int(UintKind))
		assert.Equal(17, int(NullKind))
		assert.Equal(18, int(TupleKind))
		assert.Equal(19, int(InlineBlobKind))
		assert.Equal(20, int(TimestampKind))
		assert.Equal(21, int(DecimalKind))
		assert.Equal(22, int(JSONKind))
		assert.Equal(23, int(GeometryKind))
		assert.Equal(24, int(PointKind))
		assert.Equal(25, int(LineStringKind))
		assert.Equal(26, int(PolygonKind))
		assert.Equal(27, int(SerialMessageKind))
		assert.Equal(28, int(MultiPointKind))
		assert.Equal(29, int(MultiLineStringKind))
		assert.Equal(30, int(MultiPolygonKind))
		assert.Equal(31, int(GeometryCollectionKind))
		assert.Equal(255, int(UnknownKind))
	})

}
