package mathutil

var Max = MaxInt // alias
var Min = MinInt // alias

func MaxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUint(a, b uint) uint {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint(a, b uint) uint {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxFloat(a, b float32) float32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinFloat(a, b float32) float32 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxFloat64(a, b float64) float64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinFloat64(a, b float64) float64 {
	if a < b {
		return a
	} else {
		return b
	}
}
