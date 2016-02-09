package gucumber

import (
	"flag"
	"fmt"
)

type filters []string

func (f *filters) String() string {
	return fmt.Sprint(*f)
}

func (f *filters) Set(value string) error {
	*f = append(*f, value)
	return nil
}

var filterFlag filters

func init() {
	flag.Var(&filterFlag, "tags", "comma-separated list of tags to filter scenarios by")
}

func RunMain() {
	flag.Parse()

	var dir string
	if flag.NArg() == 0 {
		dir = "internal/features"
	} else {
		dir = flag.Arg(0)
	}

	filt := []string{}
	for _, f := range filterFlag {
		filt = append(filt, string(f))
	}
	if err := BuildAndRunDir(dir, filt); err != nil {
		panic(err)
	}
}
