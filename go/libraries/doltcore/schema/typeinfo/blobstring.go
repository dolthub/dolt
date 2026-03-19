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

package typeinfo

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	blobStringTypeParam_Collate = "collate"
	blobStringTypeParam_Length  = "length"
)

// blobStringType handles the TEXT types. This was originally done in varStringType, however it did not properly
// handle large strings (such as strings over several hundred megabytes), and thus this type was created. Any
// repositories that were made before the introduction of blobStringType will still use varStringType for existing
// columns.
type blobStringType struct {
	sqlStringType sql.StringType
	enc           val.Encoding // 0 means use default based on UseAdaptiveEncoding
}

var _ TypeInfo = (*blobStringType)(nil)

var (
	TextType     TypeInfo = &blobStringType{sqlStringType: gmstypes.Text}
	LongTextType TypeInfo = &blobStringType{sqlStringType: gmstypes.LongText}
)

// Equals implements TypeInfo interface.
func (ti *blobStringType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*blobStringType); ok {
		return ti.sqlStringType.MaxCharacterLength() == ti2.sqlStringType.MaxCharacterLength() &&
				ti.sqlStringType.Collation().Equals(ti2.sqlStringType.Collation()) &&
				ti.Encoding() == ti2.Encoding()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *blobStringType) NomsKind() types.NomsKind {
	return types.BlobKind
}

// String implements TypeInfo interface.
func (ti *blobStringType) String() string {
	return fmt.Sprintf(`BlobString(%v, %v)`, ti.sqlStringType.Collation().String(), ti.sqlStringType.MaxCharacterLength())
}

// Encoding implements TypeInfo interface.
func (ti *blobStringType) Encoding() val.Encoding {
	if ti.enc != 0 {
		return ti.enc
	}
	if UseAdaptiveEncoding {
		return val.StringAdaptiveEnc
	}
	return val.StringAddrEnc
}

// WithEncoding implements TypeInfo interface.
func (ti *blobStringType) WithEncoding(enc val.Encoding) TypeInfo {
	switch enc {
	case val.StringAdaptiveEnc, val.StringAddrEnc:
	default:
		panic(fmt.Errorf("encoding %v is not valid for %T", enc, ti))
	}
	return &blobStringType{sqlStringType: ti.sqlStringType, enc: enc}
}

// ToSqlType implements TypeInfo interface.
func (ti *blobStringType) ToSqlType() sql.Type {
	return ti.sqlStringType
}
