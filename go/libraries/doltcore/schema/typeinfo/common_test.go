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
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
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
	scaleMult := float64(gmstypes.DecimalTypeMaxScale) / gmstypes.DecimalTypeMaxPrecision
	loop(t, 1, gmstypes.DecimalTypeMaxPrecision, numOfTypes, func(i int64) {
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
	require.True(t, numOfElements >= 1 && numOfElements <= gmstypes.EnumTypeMaxElements)
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
	return &enumType{gmstypes.MustCreateEnumType(vals, sql.Collation_Default)}
}

func generateSetTypes(t *testing.T, numOfTypes int64) []TypeInfo {
	res := make([]TypeInfo, numOfTypes)
	for i := int64(1); i <= numOfTypes; i++ {
		res[i-1] = generateSetType(t, int(i))
	}
	return res
}

func generateSetType(t *testing.T, numOfElements int) *setType {
	require.True(t, numOfElements >= 1 && numOfElements <= gmstypes.SetTypeMaxElements)
	vals := make([]string, numOfElements)
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	lenAlphabet := len(alphabet)
	for i := 0; i < numOfElements; i++ {
		vals[i] = string([]byte{alphabet[(i/lenAlphabet)%lenAlphabet], alphabet[i%lenAlphabet]})
	}
	return &setType{gmstypes.MustCreateSetType(vals, sql.Collation_Default)}
}

func generateInlineBlobTypes(t *testing.T, numOfTypes uint16) []TypeInfo {
	var res []TypeInfo
	loop(t, 1, 500, numOfTypes, func(i int64) {
		pad := false
		if i%2 == 0 {
			pad = true
		}
		res = append(res, generateInlineBlobType(t, i, pad))
	})
	return res
}

func generateInlineBlobType(t *testing.T, length int64, pad bool) *inlineBlobType {
	require.True(t, length > 0)
	if pad {
		t, err := gmstypes.CreateBinary(sqltypes.Binary, length)
		if err == nil {
			return &inlineBlobType{t}
		}
	}
	return &inlineBlobType{gmstypes.MustCreateBinary(sqltypes.VarBinary, length)}
}

func generateVarStringTypes(t *testing.T, numOfTypes uint16) []TypeInfo {
	var res []TypeInfo
	loop(t, 1, 500, numOfTypes, func(i int64) {
		rts := false
		if i%2 == 0 {
			rts = true
		}
		res = append(res, generateVarStringType(t, i, rts))
	})
	return res
}

func generateVarStringType(t *testing.T, length int64, rts bool) *varStringType {
	require.True(t, length > 0)
	if rts {
		t, err := gmstypes.CreateStringWithDefaults(sqltypes.Char, length)
		if err == nil {
			return &varStringType{t}
		}
	}
	return &varStringType{gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, length)}
}

func generateBlobStringType(t *testing.T, length int64) *blobStringType {
	require.True(t, length > 0)
	return &blobStringType{gmstypes.MustCreateStringWithDefaults(sqltypes.Text, length)}
}

func mustBlobString(t *testing.T, vrw types.ValueReadWriter, str string) types.Blob {
	blob, err := types.NewBlob(context.Background(), vrw, strings.NewReader(str))
	require.NoError(t, err)
	return blob
}

func mustBlobBytes(t *testing.T, b []byte) types.Blob {
	vrw := types.NewMemoryValueStore()
	blob, err := types.NewBlob(context.Background(), vrw, bytes.NewReader(b))
	require.NoError(t, err)
	return blob
}

func loop(t *testing.T, start int64, endInclusive int64, numOfSteps uint16, loopedFunc func(int64)) {
	require.True(t, endInclusive > start)
	maxNumOfSteps := endInclusive - start + 1
	trueNumOfSteps := min(int64(numOfSteps), maxNumOfSteps) - 1
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
