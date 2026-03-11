// Copyright 2020 Dolthub, Inc.
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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	bitTypeParam_Bits = "bits"
)

// This is a dolt implementation of the MySQL type Bit, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type bitType struct {
	sqlBitType gmstypes.BitType
}

var _ TypeInfo = (*bitType)(nil)

var PseudoBoolType TypeInfo = &bitType{gmstypes.MustCreateBitType(1)}

func CreateBitTypeFromParams(params map[string]string) (TypeInfo, error) {
	if bitStr, ok := params[bitTypeParam_Bits]; ok {
		bitUint, err := strconv.ParseUint(bitStr, 10, 8)
		if err != nil {
			return nil, err
		}
		sqlBitType, err := gmstypes.CreateBitType(uint8(bitUint))
		if err != nil {
			return nil, err
		}
		return &bitType{sqlBitType}, nil
	} else {
		return nil, fmt.Errorf(`create bit type info is missing param "%v"`, bitTypeParam_Bits)
	}
}

// Equals implements TypeInfo interface.
func (ti *bitType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*bitType); ok {
		return ti.sqlBitType.NumberOfBits() == ti2.sqlBitType.NumberOfBits()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *bitType) NomsKind() types.NomsKind {
	return types.UintKind
}

// String implements TypeInfo interface.
func (ti *bitType) String() string {
	return fmt.Sprintf("Bit(%v)", ti.sqlBitType.NumberOfBits())
}

// ToSqlType implements TypeInfo interface.
func (ti *bitType) ToSqlType() sql.Type {
	return ti.sqlBitType
}
