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

package types

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/src-d/go-mysql-server/sql"

	dtypes "github.com/liquidata-inc/dolt/go/store/types"
)

type uuidType struct{}

func (uuidType) NomsKind() dtypes.NomsKind {
	return dtypes.UUIDKind
}

func (uuidType) SqlType() sql.Type {
	return sql.Text
}

func (uuidType) SqlTypes() []sql.Type {
	return []sql.Type{}
}

func (uuidType) GetValueToSql() ValueToSql {
	return func(val dtypes.Value) (interface{}, error) {
		if v, ok := val.(dtypes.UUID); ok {
			return v.String(), nil
		}
		return nil, fmt.Errorf("expected UUID, recevied %v", val.Kind())
	}
}

func (uuidType) GetSqlToValue() SqlToValue {
	return func(val interface{}) (dtypes.Value, error) {
		e, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("cannot convert SQL type <%T> val <%v> to uuid", val, val)
		}
		if u, err := uuid.Parse(e); err == nil {
			return dtypes.UUID(u), nil
		}
		return nil, fmt.Errorf("cannot convert SQL val <%v> to uuid", e)
	}
}

func (uuidType) SqlTypeString() string {
	return "CHAR(36)"
}

func (uuidType) String() string {
	return "uuidType"
}
