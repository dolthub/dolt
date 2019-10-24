package types

import "fmt"

type ConversionError struct {
	fromKind NomsKind
	toKind   NomsKind
	err      error
}

func CreateConversionError(v Value, targetKind NomsKind, err error) ConversionError {
	return ConversionError{
		fromKind: v.Kind(),
		toKind:   targetKind,
		err:      err,
	}
}

func (ce ConversionError) Error() string {
	toKindStr := KindToString[ce.toKind]
	fromKindStr := KindToString[ce.fromKind]
	return fmt.Sprint("error converting", fromKindStr, "to", toKindStr, ": ", ce.err.Error())
}