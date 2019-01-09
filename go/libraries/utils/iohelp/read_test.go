package iohelp

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/test"
)

func TestErrPreservingReader(t *testing.T) {
	tr := test.NewTestReader(32, 16)
	epr := NewErrPreservingReader(tr)

	read1, noErr1 := ReadNBytes(epr, 8)
	read2, noErr2 := ReadNBytes(epr, 8)
	read3, firstErr := ReadNBytes(epr, 8)
	read4, secondErr := ReadNBytes(epr, 8)

	for i := 0; i < 8; i++ {
		if read1[i] != byte(i) || read2[i] != byte(i)+8 {
			t.Error("Unexpected values read.")
		}
	}

	if read3 != nil || read4 != nil {
		t.Error("Unexpected read values should be nil.")
	}

	if noErr1 != nil || noErr2 != nil {
		t.Error("Unexpected error.")
	}

	if firstErr == nil || secondErr == nil || epr.Err == nil {
		t.Error("Expected error not received.")
	} else {
		first := firstErr.(*test.TestError).ErrId
		second := secondErr.(*test.TestError).ErrId
		preservedErrID := epr.Err.(*test.TestError).ErrId

		if preservedErrID != first || preservedErrID != second {
			t.Error("Error not preserved properly.")
		}
	}
}

var rlTests = []struct {
	inputStr      string
	expectedLines []string
}{
	{"line 1\nline 2\r\nline 3\n", []string{"line 1", "line 2", "line 3", ""}},
	{"line 1\nline 2\r\nline 3", []string{"line 1", "line 2", "line 3"}},
	{"\r\nline 1\nline 2\r\nline 3\r\r\r\n\n", []string{"", "line 1", "line 2", "line 3", "", ""}},
}

func TestReadReadLineFunctions(t *testing.T) {
	for _, test := range rlTests {
		bufferedTest := getTestReadLineClosure(test.inputStr)
		unbufferedTest := getTestReadLineNoBufClosure(test.inputStr)

		testReadLineFunctions(t, "buffered", test.expectedLines, bufferedTest)
		testReadLineFunctions(t, "unbuffered", test.expectedLines, unbufferedTest)
	}
}

func getTestReadLineClosure(inputStr string) func() (string, bool, error) {
	r := bytes.NewReader([]byte(inputStr))
	br := bufio.NewReader(r)

	return func() (string, bool, error) {
		return ReadLine(br)
	}
}

func getTestReadLineNoBufClosure(inputStr string) func() (string, bool, error) {
	r := bytes.NewReader([]byte(inputStr))

	return func() (string, bool, error) {
		return ReadLineNoBuf(r)
	}
}

func testReadLineFunctions(t *testing.T, testType string, expected []string, rlFunc func() (string, bool, error)) {
	var isDone bool
	var line string
	var err error

	lines := make([]string, 0, len(expected))
	for !isDone {
		line, isDone, err = rlFunc()

		if err == nil {
			lines = append(lines, line)
		}
	}

	if !reflect.DeepEqual(lines, expected) {
		t.Error("Received unexpected results.")
	}
}
