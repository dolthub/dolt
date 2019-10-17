package parser

import "bufio"

// LineScanner wraps a standard bufio.Scanner and adds line numbers for debugging / reporting.
type LineScanner struct {
	*bufio.Scanner
	LineNum int
}

func (ls *LineScanner) Scan() bool {
	ls.LineNum++
	return ls.Scanner.Scan()
}

