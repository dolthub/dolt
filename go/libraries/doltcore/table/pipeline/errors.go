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
