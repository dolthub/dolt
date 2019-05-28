package sql

import (
	"errors"
	"fmt"
	"github.com/xwb1989/sqlparser"
)

const UnknownTableErrFmt = "Unknown table: '%v'"
const AmbiguousTableErrFmt = "Ambiguous table: '%v'"
const UnknownColumnErrFmt = "Unknown column: '%v'"
const AmbiguousColumnErrFmt = "Ambiguous column: '%v'"

// Turns a node to a string
func nodeToString(node sqlparser.SQLNode) string {
	buffer := sqlparser.NewTrackedBuffer(nil)
	node.Format(buffer)
	return buffer.String()
}

// Returns an error with the format string and arguments given.
func errFmt(fmtMsg string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(fmtMsg, args...))
}