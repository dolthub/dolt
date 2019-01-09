package argparser

import "errors"

type UnknownArgumentParam struct {
	name string
}

func (unkn UnknownArgumentParam) Error() string {
	return "error: unknown option `" + unkn.name + "'"
}

var ErrHelp = errors.New("Help")
