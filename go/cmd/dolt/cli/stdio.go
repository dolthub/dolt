// Copyright 2019 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/mvdata"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/google/uuid"
)

var CliOut = color.Output
var CliErr = color.Error

var ExecuteWithStdioRestored func(userFunc func())

func InitIO() (restoreIO func()) {
	stdOut, stdErr := os.Stdout, os.Stderr

	outFile := filepath.Join(os.TempDir(), uuid.New().String())
	f, err := os.Create(outFile)

	if err == nil {
		os.Stdout = f
		os.Stderr = f
		mvdata.SetIOStreams(os.Stdout, os.Stdin)
	}

	restoreIO = func() {
		if f != nil {
			f.Close()
		}

		os.Stdout = stdOut
		os.Stderr = stdErr
		mvdata.SetIOStreams(os.Stdout, os.Stdin)
	}

	ExecuteWithStdioRestored = func(userFunc func()) {
		initialNoColor := color.NoColor
		color.NoColor = true
		os.Stdout = stdOut
		os.Stderr = stdErr
		mvdata.SetIOStreams(os.Stdout, os.Stdin)

		userFunc()

		os.Stdout = f
		os.Stderr = f
		color.NoColor = initialNoColor
		mvdata.SetIOStreams(os.Stdout, os.Stdin)
	}

	return restoreIO
}

func Println(a ...interface{}) {
	fmt.Fprintln(CliOut, a...)
}

func Print(a ...interface{}) {
	fmt.Fprint(CliOut, a...)
}

func Printf(format string, a ...interface{}) {
	fmt.Fprintf(CliOut, format, a...)
}

func PrintErrln(a ...interface{}) {
	fmt.Fprintln(CliErr, a...)
}

func PrintErr(a ...interface{}) {
	fmt.Fprint(CliErr, a...)
}

func PrintErrf(format string, a ...interface{}) {
	fmt.Fprintf(CliErr, format, a...)
}

func DeleteAndPrint(prevMsgLen int, msg string) int {
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
