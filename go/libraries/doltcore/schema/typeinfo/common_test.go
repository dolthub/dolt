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
	"math"
	"strconv"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/libraries/utils/mathutil"
)

func generateBitTypes(t *testing.T, numOfTypes uint16) []TypeInfo {
	var res []TypeInfo
	loop(t, 1, 64, numOfTypes, func(i int64) {
		res = append(res, generateBitType(t, uint8(i)))
	})
	return res
}

func generateBitType(t *testing.T, bits uint8) *bitType {
	typ, err := CreateBitTypeFromParams(map[string]string{
		bitTypeParam_Bits: strconv.FormatInt(int64(bits), 10),
	})
	require.NoError(t, err)
	realType, ok := typ.(*bitType)
	require.True(t, ok)
	return realType
}

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

func generateEnumTypes(t *testing.T, numOfTypes int64) []TypeInfo {
	res := make([]TypeInfo, numOfTypes)
	for i := int64(1); i <= numOfTypes; i++ {
		res[i-1] = generateEnumType(t, int(i))
	}
	return res
}

func generateEnumType(t *testing.T, numOfElements int) *enumType {
	require.True(t, numOfElements >= 1 && numOfElements <= sql.EnumTypeMaxElements)
	vals := make([]string, numOfElements)
	str := make([]byte, 4)
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < numOfElements; i++ {
		x := i
		for j := 2; j >= 0; j-- {
			x /= len(alphabet)
			str[j] = alphabet[x%len(alphabet)]
		}
		str[3] = alphabet[i%len(alphabet)]
		vals[i] = string(str)
	}
	return &enumType{sql.MustCreateEnumType(vals, sql.Collation_Default)}
}

func generateSetTypes(t *testing.T, numOfTypes int64) []TypeInfo {
	res := make([]TypeInfo, numOfTypes)
	for i := int64(1); i <= numOfTypes; i++ {
		res[i-1] = generateSetType(t, int(i))
	}
	return res
}

func generateSetType(t *testing.T, numOfElements int) *setType {
	require.True(t, numOfElements >= 1 && numOfElements <= sql.SetTypeMaxElements)
	vals := make([]string, numOfElements)
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	lenAlphabet := len(alphabet)
	for i := 0; i < numOfElements; i++ {
		vals[i] = string([]byte{alphabet[(i/lenAlphabet)%lenAlphabet], alphabet[i%lenAlphabet]})
	}
	return &setType{sql.MustCreateSetType(vals, sql.Collation_Default)}
}

func generateVarBinaryTypes(t *testing.T, numOfTypes uint16) []TypeInfo {
	var res []TypeInfo
	loop(t, 1, 500, numOfTypes, func(i int64) {
		pad := false
		if i%2 == 0 {
			pad = true
		}
		res = append(res, generateVarBinaryType(t, i, pad))
	})
	return res
}

func generateVarBinaryType(t *testing.T, length int64, pad bool) *varBinaryType {
	require.True(t, length > 0)
	if pad {
		t, err := sql.CreateBinary(sqltypes.Binary, length)
		if err == nil {
			return &varBinaryType{t}
		}
	}
	return &varBinaryType{sql.MustCreateBinary(sqltypes.VarBinary, length)}
}

func loop(t *testing.T, start int64, endInclusive int64, numOfSteps uint16, loopedFunc func(int64)) {
	require.True(t, endInclusive > start)
	maxNumOfSteps := endInclusive - start + 1
	trueNumOfSteps := mathutil.MinInt64(int64(numOfSteps), maxNumOfSteps) - 1
	inc := float64(maxNumOfSteps) / float64(trueNumOfSteps)
	fCurrentVal := float64(start)
	currentVal := int64(math.Round(fCurrentVal))
	fCurrentVal -= 1
	for currentVal <= endInclusive {
		loopedFunc(currentVal)
		fCurrentVal += inc
		currentVal = int64(math.Round(fCurrentVal))
	}
}
