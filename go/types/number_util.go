// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "math"

const maxSafeInteger = float64(9007199254740991) // 2 ** 53 -1

func float64IsInt(f float64) bool {
	return math.Trunc(f) == f
}

// convert float64 to int64 where f == i / 2^exp
func float64ToIntExp(f float64) (int64, int) {
	if f == 0 {
		return 0, 0
	}

	isNegative := math.Signbit(f)
	f = math.Abs(f)

	exp := 0
	// Really large float, bring down to max safe integer so that it can be correctly represented by float64.
	for f > maxSafeInteger {
		f /= 2
		exp--
	}

	for !float64IsInt(f) {
		f *= 2
		exp++
	}
	if isNegative {
		f *= -1
	}
	return int64(f), exp
}

// returns float value == i / 2^exp
func intExpToFloat64(i int64, exp int) float64 {
	if exp == 0 {
		return float64(i)
	}

	return float64(i) / math.Pow(2, float64(exp))
}
