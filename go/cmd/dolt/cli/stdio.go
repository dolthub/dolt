package cli

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/google/uuid"
	"os"
	"path/filepath"
)

var CliOut = color.Output
var CliErr = color.Error

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
