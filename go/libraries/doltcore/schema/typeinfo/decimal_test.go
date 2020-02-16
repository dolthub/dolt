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

package typeinfo

import (
	"strconv"
	"testing"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/stretchr/testify/require"
)

//TODO: add some basic tests once the storage format has been decided

func generateDecimalTypes(t *testing.T, numOfTypes uint16) []TypeInfo {
	var res []TypeInfo
	scaleMult := float64(sql.DecimalTypeMaxScale) / sql.DecimalTypeMaxPrecision
	loop(t, 1, sql.DecimalTypeMaxPrecision, numOfTypes, func(i int64) {
		if i%9 <= 5 {
			res = append(res, generateDecimalType(t, i, int64(float64(i)*scaleMult)))
		} else {
			res = append(res, generateDecimalType(t, i, 0))
		}
	})
	return res
}

func generateDecimalType(t *testing.T, precision int64, scale int64) *decimalType {
	typ, err := CreateDecimalTypeFromParams(map[string]string{
		decimalTypeParam_Precision: strconv.FormatInt(precision, 10),
		decimalTypeParam_Scale:     strconv.FormatInt(scale, 10),
	})
	require.NoError(t, err)
	realType, ok := typ.(*decimalType)
	require.True(t, ok)
	return realType
}
