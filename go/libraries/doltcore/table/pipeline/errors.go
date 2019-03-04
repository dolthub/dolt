package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
)

type TransformRowFailure struct {
	Row           row.Row
	TransformName string
	Details       string
}

func (trf *TransformRowFailure) Error() string {
	return trf.TransformName + " failed processing"
}

func IsTransformFailure(err error) bool {
	_, ok := err.(*TransformRowFailure)
	return ok
}

func GetTransFailureTransName(err error) string {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.TransformName
}

func GetTransFailureRow(err error) row.Row {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.Row

}
func GetTransFailureDetails(err error) string {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.Details
}
