package icu

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestICUSmokeTest(t *testing.T) {
	var str UCharStr
	str.SetString("abc")
	var err UErrorCode
	re := Uregex_open(&str, 0, &err)
	assert.NotNil(t, re)
	assert.Equal(t, UErrorCode(0), err)
}
