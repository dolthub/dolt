// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "math"

func float64IsInt(f float64) bool {
	return math.Trunc(f) == f
}

// convert float64 to int64 where f == i * 2^exp
func float64ToIntExp(f float64) (int64, int) {
	if f == 0 {
		return 0, 0
	}

	isNegative := math.Signbit(f)
	f = math.Abs(f)

	frac, exp := math.Frexp(f)
	// frac is  [.5, 1)
	// Move frac up until it is an integer.
	for !float64IsInt(frac) {
		frac *= 2
		exp--
	}

	if isNegative {
		frac *= -1
	}

	return int64(frac), exp
}

// fracExpToFloat returns frac * 2 ** exp
func fracExpToFloat(frac int64, exp int) float64 {
	return float64(frac) * math.Pow(2, float64(exp))
}
