package stump

import (
	"fmt"
	"io"
	"os"
	"strings"
)

var Verbose bool

var ErrorPrefix = "ERROR: "

var LogOut io.Writer = os.Stdout
var ErrOut io.Writer = os.Stdout

func Error(args ...interface{}) {
	log(ErrOut, ErrorPrefix, args)
}

func Fatal(args ...interface{}) {
	Error(args...)
	os.Exit(1)
}

func Log(args ...interface{}) {
	log(LogOut, "", args)
}

func VLog(args ...interface{}) {
	if Verbose {
		log(LogOut, "", args)
	}
}

func log(out io.Writer, prefix string, args []interface{}) {
	writelog := func(format string, args ...interface{}) {
		n := strings.Count(format, "%")
		if n < len(args) {
			format += strings.Repeat(" %s", len(args)-n)
		}
		if !strings.HasSuffix(format, "\n") {
			format += "\n"
		}
		fmt.Fprintf(out, format, args...)
	}

	if len(args) == 0 {
		writelog(prefix)
		return
	}

	switch s := args[0].(type) {
	case string:
		writelog(prefix+s, args[1:]...)
	case fmt.Stringer:
		writelog(prefix+s.String(), args[1:]...)
	default:
		format := strings.Repeat("%s ", len(args))
		writelog(prefix+format, args...)
	}
}
