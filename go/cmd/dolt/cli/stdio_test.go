// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFdIsTerminalHonorsRedirectedCliErr(t *testing.T) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// A redirected CliErr (e.g. --stderr <file>) is a plain *os.File and must be
	// detected as non-terminal so no backspaces are written into the file, even
	// if the real stderr fallback happens to be a terminal.
	assert.False(t, fdIsTerminal(f, os.Stdout))

	// A non-*os.File writer (e.g. the colorable wrapper on Windows) falls back
	// to the provided stderr.
	assert.False(t, fdIsTerminal(&bytes.Buffer{}, f))
}

func TestDeleteAndPrintSkipsBackspacesWhenNotATTY(t *testing.T) {
	oldErr, oldTerm := CliErr, outputIsTerminal
	defer func() { CliErr, outputIsTerminal = oldErr, oldTerm }()

	const prevFrame = " Uploading..."
	n := len(prevFrame) + 1

	t.Run("not a terminal", func(t *testing.T) {
		var captured bytes.Buffer
		CliErr = &captured
		outputIsTerminal = false

		DeleteAndPrint(n, "")

		assert.Empty(t, captured.String(), "no bytes should be written to a non-TTY")
	})

	t.Run("terminal", func(t *testing.T) {
		var captured bytes.Buffer
		CliErr = &captured
		outputIsTerminal = true

		DeleteAndPrint(n, "")

		want := strings.Repeat("\b", n) + strings.Repeat(" ", n) + strings.Repeat("\b", n)
		assert.Equal(t, want, captured.String())
	})
}

func TestSpinnerDoesNotCorruptCapturedErrorLog(t *testing.T) {
	oldErr, oldTerm := CliErr, outputIsTerminal
	defer func() { CliErr, outputIsTerminal = oldErr, oldTerm }()

	var captured bytes.Buffer
	CliErr = &captured
	outputIsTerminal = false

	const frame = " Uploading..."
	DeleteAndPrint(0, "/"+frame)
	PrintErr("boom: connection reset")
	DeleteAndPrint(len("boom: connection reset"), "")

	out := captured.String()
	assert.NotContains(t, out, "\b", "no backspaces should reach a captured log")
	assert.Contains(t, out, "boom: connection reset", "the error text must survive in the log")
}

func TestEphemeralPrinter(t *testing.T) {
	t.Run("DisplayPutsCursorAtLineStart", func(t *testing.T) {
		old := CliOut
		defer func() {
			CliOut = old
		}()

		var b bytes.Buffer
		CliOut = &b
		p := NewEphemeralPrinter()
		p.Printf("Hi!")
		p.Display()

		assert.Equal(t, b.String(), "Hi!\n")

		p.Display()
		assert.Equal(t, "Hi!\n"+clearLinesTxt(1), b.String())

		p.Printf("Newline!\n")
		p.Printf("And another one!")
		p.Printf("\nSomething else\n")
		p.Display()

		assert.Equal(t,
			"Hi!\n"+
				clearLinesTxt(1)+
				"Newline!\n"+
				"And another one!\n"+
				"Something else\n", b.String())

		p.Display()
		assert.Equal(t,
			"Hi!\n"+
				clearLinesTxt(1)+
				"Newline!\n"+
				"And another one!\n"+
				"Something else\n"+
				clearLinesTxt(3), b.String())
	})
}

// clearLinesTxt moves cursor up n lines and clears the screen from the cursor position to the end.
// Inspired by https://github.com/vbauerster/mpb/blob/v8.0.2/cwriter/writer.go#L11-L15.
func clearLinesTxt(n int) string {
	// These are ANSI escape codes, see
	//  - https://en.wikipedia.org/wiki/ANSI_escape_code#C0_control_codes1
	//  - https://en.wikipedia.org/wiki/ANSI_escape_code#Control_Sequence_Introducer_commands
	//
	// \x1b: ESC (Escape)
	// [%dA: CUU (Cursor Up). [5A means moves the cursor up 5 lines
	// [J  : ED (Erase in Display)
	return fmt.Sprintf("\x1b[%dA\x1b[J", n)
}
