// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"math"
)

func float64IsInt(f, machineEpsilon float64) bool {
	_, frac := math.Modf(math.Abs(f))
	if frac < machineEpsilon || frac > 1.0-machineEpsilon {
		return true
	}
	return false
}

// convert float64 to int64 where f == i / 2^exp
func float64ToIntExp(f float64) (i int64, exp int) {
	if f == 0 {
		return 0, 0
	}

	isNegative := math.Signbit(f)
	f = math.Abs(f)

	machineEpsilon := math.Nextafter(1, 2) - 1
	exp = 0
	// really large float, bring down to within MaxInt64
	for f > float64(math.MaxInt64) {
		f /= 2
		exp--
	}

	for !float64IsInt(f, machineEpsilon) {
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
	} else {
		return float64(i) / math.Pow(2, float64(exp))
	}
}
