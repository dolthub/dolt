// Package is a utility library that panics if an error is detected
package must

func Uint32(val uint32, err error) uint32 {
	panicIfError(err)
	return val
}

func Uint64(val uint64, err error) uint64 {
	panicIfError(err)
	return val
}

func Uint(val uint, err error) uint {
	panicIfError(err)
	return val
}

func Int32(val int32, err error) int32 {
	panicIfError(err)
	return val
}

func Int64(val int64, err error) int64 {
	panicIfError(err)
	return val
}

func Int(val int, err error) int {
	panicIfError(err)
	return val
}

func Float32(val float32, err error) float32 {
	panicIfError(err)
	return val
}

func Float64(val float64, err error) float64 {
	panicIfError(err)
	return val
}

func Bool(val bool, err error) bool {
	panicIfError(err)
	return val
}

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}
