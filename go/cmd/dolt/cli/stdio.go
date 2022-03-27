// Copyright 2019 Dolthub, Inc.
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/gosuri/uilive"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

var colorOutput = color.Output
var colorFd = os.Stdout.Fd()
var CliOut = colorOutput
var CliErr = color.Error

var outputClosed uint64

var InStream io.ReadCloser = os.Stdin
var OutStream io.WriteCloser = os.Stdout

var ExecuteWithStdioRestored func(userFunc func())

func CloseOutput() {
	if atomic.CompareAndSwapUint64(&outputClosed, 0, 1) {
		fmt.Fprintln(CliOut)
	}
}

func SetIOStreams(inStream io.ReadCloser, outStream io.WriteCloser) {
	InStream = inStream
	OutStream = outStream
}

func InitIO() (restoreIO func()) {
	stdOut, stdErr := os.Stdout, os.Stderr

	outFile := filepath.Join(os.TempDir(), uuid.New().String())
	f, err := os.Create(outFile)

	if err == nil {
		os.Stdout = f
		os.Stderr = f
		SetIOStreams(os.Stdin, iohelp.NopWrCloser(CliOut))
	}

	restoreIO = func() {
		if f != nil {
			f.Close()
		}

		os.Stdout = stdOut
		os.Stderr = stdErr
		SetIOStreams(os.Stdin, os.Stdout)
	}

	ExecuteWithStdioRestored = func(userFunc func()) {
		initialNoColor := color.NoColor
		color.NoColor = true
		os.Stdout = stdOut
		os.Stderr = stdErr
		SetIOStreams(os.Stdin, os.Stdout)

		userFunc()

		os.Stdout = f
		os.Stderr = f
		color.NoColor = initialNoColor
		SetIOStreams(os.Stdin, iohelp.NopWrCloser(CliOut))
	}

	return restoreIO
}

func Println(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintln(CliOut, a...)
}

func Print(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprint(CliOut, a...)
}

func Printf(format string, a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintf(CliOut, format, a...)
}

func PrintErrln(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintln(CliErr, a...)
}

func PrintErr(a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprint(CliErr, a...)
}

func PrintErrf(format string, a ...interface{}) {
	if outputIsClosed() {
		return
	}

	fmt.Fprintf(CliErr, format, a...)
}

// DeleteAndPrint prints a new message and deletes the old one given the
// previous messages length. It returns the length of the printed message that
// should be passed as prevMsgLen on the next call of DeleteAndPrint.
//
// DeleteAndPrint does not work for multiline messages.
func DeleteAndPrint(prevMsgLen int, msg string) int {
	if outputIsClosed() {
		return 0
	}

	msgLen := len(msg)
	backspacesAndMsg := make([]byte, prevMsgLen+msgLen, 2*prevMsgLen+msgLen)
	for i := 0; i < prevMsgLen; i++ {
		backspacesAndMsg[i] = '\b'
	}

	for i, c := range []byte(msg) {
		backspacesAndMsg[i+prevMsgLen] = c
	}

	diff := prevMsgLen - msgLen

	if diff > 0 {
		for i := 0; i < diff; i++ {
			backspacesAndMsg = append(backspacesAndMsg, ' ')
		}

		for i := 0; i < diff; i++ {
			backspacesAndMsg = append(backspacesAndMsg, '\b')
		}
	}

	Print(string(backspacesAndMsg))
	return msgLen
}

// EphemeralPrinter is tool than you can use to print temporary line(s) to the
// console. Each time Display is called, the output is reset, and you can begin
// writing anew.
type EphemeralPrinter struct {
	outW         io.Writer
	w            *uilive.Writer
	wrote        bool
	wroteNewline bool

	started bool
}

// NewEphemeralPrinter creates a new EphemeralPrinter.
func NewEphemeralPrinter() *EphemeralPrinter {
	w := uilive.New()
	// How uilive needs to clear terminal output, depends on what StdOut is
	// being used. It checks the running terminal by casting the provided writer
	// to an internal interface that just defines a `Fd() uintptr` function. It
	// uses the file descriptor returned by fd to check if the writer writes to
	// a terminal.
	//
	// If we use |colorOutput|, the type cast will fail and uilive will fail to
	// detect the output terminal even though colorOutput always points to
	// StdOut. We provide the file descriptor by wrapping |colorOutput| in a
	// struct.
	if CliOut == colorOutput {
		w.Out = fdProvider{CliOut, colorFd}
	} else {
		w.Out = CliOut
	}
	e := &EphemeralPrinter{outW: w, w: w}
	return e
}

// Printf formats and prints a string to the printer. If no newline character is
// provided, one will be added to ensure that the output can be cleared in the
// future. You can print multiple lines.
func (e *EphemeralPrinter) Printf(format string, a ...interface{}) {
	if outputIsClosed() {
		return
	}

	str := fmt.Sprintf(format, a...)
	lines := strings.Split(str, "\n")
	for i, line := range lines {
		if !e.started {
			e.started = true
			e.w.Start()
		}
		if i != 0 {
			_, _ = e.outW.Write([]byte("\n"))
			e.wroteNewline = true
		}

		e.wrote = true
		_, _ = e.outW.Write([]byte(line))
		e.outW = e.w.Newline()
	}
}

// Display clears the previous output and displays the new text.
func (e *EphemeralPrinter) Display() {
	if outputIsClosed() {
		return
	}
	if !e.wrote {
		// If nothing was written, write the null character in order to clear
		// the display.
		_, _ = e.w.Write([]byte("\x00"))
	} else if !e.wroteNewline {
		// If no newline was written, add it. This will ensure that the output
		// is cleared properly.
		_, _ = e.w.Write([]byte("\n"))
	}
	if e.started {
		e.started = false
		e.w.Stop()
	}

	e.outW = e.w
	e.wrote = false
	e.wroteNewline = false
}

func outputIsClosed() bool {
	isClosed := atomic.LoadUint64(&outputClosed)
	return isClosed == 1
}

type fdProvider struct {
	io.Writer
	fd uintptr
}

var _ uilive.FdWriter = fdProvider{}

func (p fdProvider) Fd() uintptr {
	return p.fd
}
