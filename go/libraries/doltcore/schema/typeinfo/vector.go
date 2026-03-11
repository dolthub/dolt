// Copyright 2025 Dolthub, Inc.
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

package typeinfo

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	vectorTypeParam_Length = "length"
)

// As a type, this is modeled more after MySQL's story for binary data. There, it's treated
// as a string that is interpreted as raw bytes, rather than as a bespoke data structure,
// and thus this is mirrored here in its implementation. This will minimize any differences
// that could arise.
//
// This type handles the BLOB types. BINARY and VARBINARY are handled by inlineBlobType.
type vectorType struct {
	sqlVectorType gmstypes.VectorType
}

var _ TypeInfo = (*vectorType)(nil)

// Equals implements TypeInfo interface.
func (ti *vectorType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*vectorType); ok {
		return ti.sqlVectorType.Dimensions == ti2.sqlVectorType.Dimensions
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *vectorType) NomsKind() types.NomsKind {
	return types.BlobKind
}

// String implements TypeInfo interface.
func (ti *vectorType) String() string {
	return fmt.Sprintf(`Vector(%v)`, ti.sqlVectorType.Dimensions)
}

// ToSqlType implements TypeInfo interface.
func (ti *vectorType) ToSqlType() sql.Type {
	return ti.sqlVectorType
}
