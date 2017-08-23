package unit

import "fmt"

type Information int64

const (
	_  Information = iota // ignore first value by assigning to blank identifier
	KB             = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
)

func (i Information) String() string {

	tmp := int64(i)

	// default
	var d = tmp
	symbol := "B"

	switch {
	case i > EB:
		d = tmp / EB
		symbol = "EB"
	case i > PB:
		d = tmp / PB
		symbol = "PB"
	case i > TB:
		d = tmp / TB
		symbol = "TB"
	case i > GB:
		d = tmp / GB
		symbol = "GB"
	case i > MB:
		d = tmp / MB
		symbol = "MB"
	case i > KB:
		d = tmp / KB
		symbol = "KB"
	}
	return fmt.Sprintf("%d %s", d, symbol)
}
