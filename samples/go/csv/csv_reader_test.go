// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"bytes"
	"strings"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestCR(t *testing.T) {
	testFile := []byte("a,b,c\r1,2,3\r")
	delimiter, err := StringToRune(",")

	r := NewCSVReader(bytes.NewReader(testFile), delimiter)
	lines, err := r.ReadAll()

	assert.NoError(t, err, "An error occurred while reading the data: %v", err)
	if len(lines) != 2 {
		t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
	}
}

func TestLF(t *testing.T) {
	testFile := []byte("a,b,c\n1,2,3\n")
	delimiter, err := StringToRune(",")

	r := NewCSVReader(bytes.NewReader(testFile), delimiter)
	lines, err := r.ReadAll()

	assert.NoError(t, err, "An error occurred while reading the data: %v", err)
	if len(lines) != 2 {
		t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
	}
}

func TestCRLF(t *testing.T) {
	testFile := []byte("a,b,c\r\n1,2,3\r\n")
	delimiter, err := StringToRune(",")

	r := NewCSVReader(bytes.NewReader(testFile), delimiter)
	lines, err := r.ReadAll()

	assert.NoError(t, err, "An error occurred while reading the data: %v", err)
	if len(lines) != 2 {
		t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
	}
}

func TestCRInQuote(t *testing.T) {
	testFile := []byte("a,\"foo,\rbar\",c\r1,\"2\r\n2\",3\r")
	delimiter, err := StringToRune(",")

	r := NewCSVReader(bytes.NewReader(testFile), delimiter)
	lines, err := r.ReadAll()

	assert.NoError(t, err, "An error occurred while reading the data: %v", err)
	if len(lines) != 2 {
		t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
	}
	if strings.Contains(lines[1][1], "\n\n") {
		t.Error("The CRLF was converted to a LFLF")
	}
}

func TestCRLFEndOfBufferLength(t *testing.T) {
	testFile := make([]byte, 4096*2, 4096*2)
	testFile[4095] = 13 // \r byte
	testFile[4096] = 10 // \n byte
	delimiter, err := StringToRune(",")

	r := NewCSVReader(bytes.NewReader(testFile), delimiter)
	lines, err := r.ReadAll()

	assert.NoError(t, err, "An error occurred while reading the data: %v", err)
	if len(lines) != 2 {
		t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
	}
}
