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
	"context"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/store/types"
)

var UnhandledTypeConversion = errors.NewKind("`%s` does not know how to handle type conversions to `%s`")
var InvalidTypeConversion = errors.NewKind("`%s` cannot convert the value `%v` to `%s`")

// TypeConverter is a function that is used to convert a Noms value from one TypeInfo to another.
type TypeConverter func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error)

// wrapIsValid is a helper function that takes an IsValid function and returns a TypeConverter.
func wrapIsValid(isValid func(v types.Value) bool, srcTi TypeInfo, destTi TypeInfo) (tc TypeConverter, needsConversion bool, err error) {
	return func(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (types.Value, error) {
		if v == nil || v == types.NullValue {
			return types.NullValue, nil
		}
		if !isValid(v) {
			return nil, InvalidTypeConversion.New(srcTi.String(), v, destTi.String())
		}
		return v, nil
	}, false, nil
}
