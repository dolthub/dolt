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

package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
)

// TransformRowFailure is an error implementation that stores the row that failed to transform, the transform that
// failed and some details of the error
type TransformRowFailure struct {
	Row           row.Row
	TransformName string
	Details       string
}

// Error returns a string containing details of the error that occurred
func (trf *TransformRowFailure) Error() string {
	return trf.TransformName + " failed processing"
}

// IsTransformFailure will return true if the error is an instance of a TransformRowFailure
func IsTransformFailure(err error) bool {
	_, ok := err.(*TransformRowFailure)
	return ok
}

// GetTransFailureTransName extracts the name of the transform that failed from an error that is an instance of a
// TransformRowFailure
func GetTransFailureTransName(err error) string {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.TransformName
}

// GetTransFailureRow extracts the row that failed from an error that is an instance of a TransformRowFailure
func GetTransFailureRow(err error) row.Row {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.Row

}

// GetTransFailureDetails extracts the details string from an error that is an instance of a TransformRowFailure
func GetTransFailureDetails(err error) string {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.Details
}
