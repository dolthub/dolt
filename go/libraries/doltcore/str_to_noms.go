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

package doltcore

import (
	"errors"

	"github.com/liquidata-inc/dolt/go/store/types"
)

// StringToValue takes a string and a NomsKind and tries to convert the string to a noms Value.
func StringToValue(s string, kind types.NomsKind) (types.Value, error) {
	if !types.IsPrimitiveKind(kind) || kind == types.BlobKind {
		return nil, errors.New("Only primitive type support")
	}

	emptyStringType := types.KindToType[types.StringKind]

	marshalFunc, err := emptyStringType.GetMarshalFunc(kind)
	if err != nil {
		panic("Unsupported type " + kind.String())
	}

	return marshalFunc(types.String(s))
}
