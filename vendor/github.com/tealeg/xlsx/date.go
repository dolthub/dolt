package xlsx

import (
	"math"
	"time"
)

const MJD_0 float64 = 2400000.5
const MJD_JD2000 float64 = 51544.5

func shiftJulianToNoon(julianDays, julianFraction float64) (float64, float64) {
	switch {
	case -0.5 < julianFraction && julianFraction < 0.5:
		julianFraction += 0.5
	case julianFraction >= 0.5:
		julianDays += 1
		julianFraction -= 0.5
	case julianFraction <= -0.5:
		julianDays -= 1
		julianFraction += 1.5
	}
	return julianDays, julianFraction
}

// Return the integer values for hour, minutes, seconds and
// nanoseconds that comprised a given fraction of a day.
func fractionOfADay(fraction float64) (hours, minutes, seconds, nanoseconds int) {
	f := 5184000000000000 * fraction
	nanoseconds = int(math.Mod(f, 1000000000))
	f = f / 1000000000
	seconds = int(math.Mod(f, 3600))
	f = f / 3600
	minutes = int(math.Mod(f, 60))
	f = f / 60
	hours = int(f)
	return hours, minutes, seconds, nanoseconds
}

func julianDateToGregorianTime(part1, part2 float64) time.Time {
	part1I, part1F := math.Modf(part1)
	part2I, part2F := math.Modf(part2)
	julianDays := part1I + part2I
	julianFraction := part1F + part2F
	julianDays, julianFraction = shiftJulianToNoon(julianDays, julianFraction)
	day, month, year := doTheFliegelAndVanFlandernAlgorithm(int(julianDays))
	hours, minutes, seconds, nanoseconds := fractionOfADay(julianFraction)
	return time.Date(year, time.Month(month), day, hours, minutes, seconds, nanoseconds, time.UTC)
}

// By this point generations of programmers have repeated the
// algorithm sent to the editor of "Communications of the ACM" in 1968
// (published in CACM, volume 11, number 10, October 1968, p.657).
// None of those programmers seems to have found it necessary to
// explain the constants or variable names set out by Henry F. Fliegel
// and Thomas C. Van Flandern.  Maybe one day I'll buy that jounal and
// expand an explanation here - that day is not today.
func doTheFliegelAndVanFlandernAlgorithm(jd int) (day, month, year int) {
	l := jd + 68569
	n := (4 * l) / 146097
	l = l - (146097*n+3)/4
	i := (4000 * (l + 1)) / 1461001
	l = l - (1461*i)/4 + 31
	j := (80 * l) / 2447
	d := l - (2447*j)/80
	l = j / 11
	m := j + 2 - (12 * l)
	y := 100*(n-49) + i + l
	return d, m, y
}

// Convert an excelTime representation (stored as a floating point number) to a time.Time.
func TimeFromExcelTime(excelTime float64, date1904 bool) time.Time {
	var date time.Time
	var intPart int64 = int64(excelTime)
	// Excel uses Julian dates prior to March 1st 1900, and
	// Gregorian thereafter.
	if intPart <= 61 {
		const OFFSET1900 = 15018.0
		const OFFSET1904 = 16480.0
		var date time.Time
		if date1904 {
			date = julianDateToGregorianTime(MJD_0, excelTime+OFFSET1904)
		} else {
			date = julianDateToGregorianTime(MJD_0, excelTime+OFFSET1900)
		}
		return date
	}
	var floatPart float64 = excelTime - float64(intPart)
	var dayNanoSeconds float64 = 24 * 60 * 60 * 1000 * 1000 * 1000
	if date1904 {
		date = time.Date(1904, 1, 1, 0, 0, 0, 0, time.UTC)
	} else {
		date = time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
	}
	durationDays := time.Duration(intPart) * time.Hour * 24
	durationPart := time.Duration(dayNanoSeconds * floatPart)
	return date.Add(durationDays).Add(durationPart)
}
