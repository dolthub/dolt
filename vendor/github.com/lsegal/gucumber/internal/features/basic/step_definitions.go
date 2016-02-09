package basic

import (
	. "github.com/lsegal/gucumber"
	"github.com/stretchr/testify/assert"
)

func init() {
	executions := 100
	result := 1

	Before("@basic", func() {
		executions = 0
	})

	Given(`^I have an initial step$`, func() {
		assert.Equal(T, 1, 1)
	})

	And(`^I have a second step$`, func() {
		assert.Equal(T, 2, 2)
	})

	When(`^I run the "(.+?)" command$`, func(s1 string) {
		assert.Equal(T, "gucumber", s1)
	})

	Then(`^this scenario should execute (\d+) time and pass$`, func(i1 int) {
		executions++
		assert.Equal(T, i1, executions)
	})

	Given(`^I perform (\d+) \+ (\d+)$`, func(i1 int, i2 int) {
		result = i1 + i2
	})

	Then(`^I should get (\d+)$`, func(i1 int) {
		assert.Equal(T, result, i1)
	})
}
