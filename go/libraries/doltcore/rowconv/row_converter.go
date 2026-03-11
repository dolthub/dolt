// Copyright 2019 Dolthub, Inc.
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

package rowconv

// WarnFunction is a callback function that callers can optionally provide during row conversion
// to take an extra action when a value cannot be automatically converted to the output data type.
type WarnFunction func(int, string, ...interface{})

const DatatypeCoercionFailureWarning = "unable to coerce value from field '%s' into latest column schema"
const DatatypeCoercionFailureWarningCode int = 1105 // Since this our own custom warning we'll use 1105, the code for an unknown error

const TruncatedOutOfRangeValueWarning = "Truncated %v value: %v"
const TruncatedOutOfRangeValueWarningCode = 1292
