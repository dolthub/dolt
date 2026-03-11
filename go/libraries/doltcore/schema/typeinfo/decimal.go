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
	decimalTypeParam_Precision = "prec"
	decimalTypeParam_Scale     = "scale"
)

// This is a dolt implementation of the MySQL type Decimal, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type decimalType struct {
	sqlDecimalType sql.DecimalType
}

var _ TypeInfo = (*decimalType)(nil)

func CreateDecimalTypeFromParams(params map[string]string) (TypeInfo, error) {
	var precision uint8
	if precisionStr, ok := params[decimalTypeParam_Precision]; ok {
		precisionUint, err := strconv.ParseUint(precisionStr, 10, 8)
		if err != nil {
			return nil, err
		}
		precision = uint8(precisionUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Precision)
	}
	var scale uint8
	if scaleStr, ok := params[decimalTypeParam_Scale]; ok {
		scaleUint, err := strconv.ParseUint(scaleStr, 10, 8)
		if err != nil {
			return nil, err
		}
		scale = uint8(scaleUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Scale)
	}
	sqlDecimalType, err := gmstypes.CreateColumnDecimalType(precision, scale)
	if err != nil {
		return nil, err
	}
	return &decimalType{sqlDecimalType}, nil
}

// Equals implements TypeInfo interface.
func (ti *decimalType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*decimalType); ok {
		return ti.sqlDecimalType.Precision() == ti2.sqlDecimalType.Precision() &&
			ti.sqlDecimalType.Scale() == ti2.sqlDecimalType.Scale()
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *decimalType) NomsKind() types.NomsKind {
	return types.DecimalKind
}

// String implements TypeInfo interface.
func (ti *decimalType) String() string {
	return fmt.Sprintf("Decimal(%v, %v)", ti.sqlDecimalType.Precision(), ti.sqlDecimalType.Scale())
}

// ToSqlType implements TypeInfo interface.
func (ti *decimalType) ToSqlType() sql.Type {
	return ti.sqlDecimalType
}
