package xlsx

import (
	. "gopkg.in/check.v1"
	"time"
)

type DateSuite struct{}

var _ = Suite(&DateSuite{})

func (d *DateSuite) TestFractionOfADay(c *C) {
	var h, m, s, n int
	h, m, s, n = fractionOfADay(0)
	c.Assert(h, Equals, 0)
	c.Assert(m, Equals, 0)
	c.Assert(s, Equals, 0)
	c.Assert(n, Equals, 0)
	h, m, s, n = fractionOfADay(1.0 / 24.0)
	c.Assert(h, Equals, 1)
	c.Assert(m, Equals, 0)
	c.Assert(s, Equals, 0)
	c.Assert(n, Equals, 0)
}

func (d *DateSuite) TestJulianDateToGregorianTime(c *C) {
	c.Assert(julianDateToGregorianTime(2400000.5, 51544.0),
		Equals, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	c.Assert(julianDateToGregorianTime(2400000.5, 51544.5),
		Equals, time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC))
	c.Assert(julianDateToGregorianTime(2400000.5, 51544.245),
		Equals, time.Date(2000, 1, 1, 6, 40, 0, 13578, time.UTC))
	c.Assert(julianDateToGregorianTime(2400000.5, 51544.1),
		Equals, time.Date(2000, 1, 1, 3, 22, 59, 999992456, time.UTC))
	c.Assert(julianDateToGregorianTime(2400000.5, 51544.75),
		Equals, time.Date(2000, 1, 1, 18, 0, 0, 0, time.UTC))
}

func (d *DateSuite) TestTimeFromExcelTime(c *C) {
	date := TimeFromExcelTime(0, false)
	c.Assert(date, Equals, time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC))
	date = TimeFromExcelTime(60, false)
	c.Assert(date, Equals, time.Date(1900, 2, 28, 0, 0, 0, 0, time.UTC))
	date = TimeFromExcelTime(61, false)
	c.Assert(date, Equals, time.Date(1900, 3, 1, 0, 0, 0, 0, time.UTC))
	date = TimeFromExcelTime(41275.0, false)
	c.Assert(date, Equals, time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC))
}

func (d *DateSuite) TestTimeFromExcelTimeWithFractionalPart(c *C) {
	date := TimeFromExcelTime(0.114583333333333, false)
	c.Assert(date.Round(time.Second), Equals, time.Date(1899, 12, 30, 2, 45, 0, 0, time.UTC))

	date = TimeFromExcelTime(60.1145833333333, false)
	c.Assert(date.Round(time.Second), Equals, time.Date(1900, 2, 28, 2, 45, 0, 0, time.UTC))

	date = TimeFromExcelTime(61.3986111111111, false)
	c.Assert(date.Round(time.Second), Equals, time.Date(1900, 3, 1, 9, 34, 0, 0, time.UTC))

	date = TimeFromExcelTime(37947.75, false)
	c.Assert(date.Round(time.Second), Equals, time.Date(2003, 11, 22, 18, 0, 0, 0, time.UTC))

	date = TimeFromExcelTime(41275.1145833333, false)
	c.Assert(date.Round(time.Second), Equals, time.Date(2013, 1, 1, 2, 45, 0, 0, time.UTC))
}

func (d *DateSuite) TestTimeFromExcelTimeWith1904Offest(c *C) {
	date1904Offset := TimeFromExcelTime(39813.0, true)
	c.Assert(date1904Offset, Equals, time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC))

}
