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

	"github.com/src-d/go-mysql-server/sql"

	dtypes "github.com/liquidata-inc/dolt/go/store/types"
)

type stringType struct{}

func (stringType) NomsKind() dtypes.NomsKind {
	return dtypes.StringKind
}

func (stringType) SqlType() sql.Type {
	return sql.Text
}

func (stringType) SqlTypes() []sql.Type {
	return []sql.Type{sql.Text}
}

func (stringType) GetValueToSql() ValueToSql {
	return func(val dtypes.Value) (interface{}, error) {
		if v, ok := val.(dtypes.String); ok {
			return string(v), nil
		}
		return nil, fmt.Errorf("expected String, recevied %v", val.Kind())
	}
}

func (stringType) GetSqlToValue() SqlToValue {
	return func(val interface{}) (dtypes.Value, error) {
		e, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("cannot convert SQL type <%T> val <%v> to string", val, val)
		}
		return dtypes.String(e), nil
	}
}

func (stringType) String() string {
	return "stringType"
}
