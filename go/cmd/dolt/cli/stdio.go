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
	"sync/atomic"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"

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
