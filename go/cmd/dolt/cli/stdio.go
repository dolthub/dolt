package cli

import (
	"fmt"
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
	}

	restoreIO = func() {
		if f != nil {
			f.Close()
		}

		os.Stdout = stdOut
		os.Stderr = stdErr
	}

	ExecuteWithStdioRestored = func(userFunc func()) {
		initialNoColor := color.NoColor
		color.NoColor = true
		os.Stdout = stdOut
		os.Stderr = stdErr

		userFunc()

		os.Stdout = f
		os.Stderr = f
		color.NoColor = initialNoColor
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
