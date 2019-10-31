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
	"errors"
	"fmt"
)

type ConversionError struct {
	fromKind NomsKind
	toKind   NomsKind
	err      error
}

// CreateConversionError creates a special kind of error to return during issues with marshalling values.
func CreateConversionError(sourceKind NomsKind, targetKind NomsKind, err error) ConversionError {
	return ConversionError{
		fromKind: sourceKind,
		toKind:   targetKind,
		err:      err,
	}
}

// CreateNoConversionError creates a special kind of error to return when no marshal function is provided.
func CreateNoConversionError(sourceKind NomsKind, targetKind NomsKind) ConversionError {
	return ConversionError{
		fromKind: sourceKind,
		toKind:   targetKind,
		err:      errors.New("no marshalling function found"),
	}
}

func (ce ConversionError) Error() string {
	toKindStr := KindToString[ce.toKind]
	fromKindStr := KindToString[ce.fromKind]
	return fmt.Sprint("error converting", fromKindStr, "to", toKindStr, ": ", ce.err.Error())
}
