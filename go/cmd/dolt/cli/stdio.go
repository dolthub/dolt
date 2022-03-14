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

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/gosuri/uilive"

	"github.com/fatih/color"
	"github.com/google/uuid"
)

var outputClosed uint64

func CloseOutput() {
	if atomic.CompareAndSwapUint64(&outputClosed, 0, 1) {
		fmt.Fprintln(CliOut)
	}
}

func outputIsClosed() bool {
	isClosed := atomic.LoadUint64(&outputClosed)
	return isClosed == 1
}

var CliOut = color.Output
var CliErr = color.Error

var ExecuteWithStdioRestored func(userFunc func())

var InStream io.ReadCloser = os.Stdin
var OutStream io.WriteCloser = os.Stdout

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

// EphemeralPrinter is tool than you can use to print temporary line(s) to the
// console. Each time Display is called, the output is reset, and you can begin
// writing anew. If you need to display multiple temporary lines, call Newline,
// before writing each line.
type EphemeralPrinter struct {
	outW io.Writer
	w    *uilive.Writer
}

// StartEphemeralPrinter creates a new EphemeralPrinter and starts it. You
// should defer Stop after calling this.
func StartEphemeralPrinter() *EphemeralPrinter {
	w := uilive.New()
	w.Out = CliOut
	e := &EphemeralPrinter{w, w}
	e.start()
	return e
}

// Printf formats and prints a string to the printer. Printf will panic if
// |format| contains the newline character. If you need to display multiple
// lines, use Newline.
func (e *EphemeralPrinter) Printf(format string, a ...interface{}) {
	if outputIsClosed() {
		return
	}

	if strings.ContainsRune(format, '\n') {
		panic("EphemeralPrinter Printf was passed a newline, this will break line clearing functionality!")
	}

	_, _ = fmt.Fprintf(e.outW, format, a...)
}

// Display clears the previous output and displays the new text.
func (e *EphemeralPrinter) Display() {
	if outputIsClosed() {
		return
	}
	_, _ = e.w.Write([]byte("\n"))
	_ = e.w.Flush()
	e.outW = e.w
}

// Newline allows EphemeralPrinter to Display multiple lines. You can print the
// first line with Printf, call Newline, and then print a second line with
// Printf again.
func (e *EphemeralPrinter) Newline() {
	if outputIsClosed() {
		return
	}
	_, _ = e.w.Write([]byte("\n"))
	e.outW = e.w.Newline()
}

func (e *EphemeralPrinter) start() {
	e.w.Start()
}

// Stop stops the ephemeral printer. It must be called after using StartEphemeralPrinter
func (e *EphemeralPrinter) Stop() {
	// Writing null character here to clear the output
	_, _ = e.w.Write([]byte("\x00"))
	e.w.Stop()
}

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
