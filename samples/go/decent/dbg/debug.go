// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package dbg

import (
	"fmt"
	"github.com/attic-labs/noms/go/d"
	"log"
	"os"
	"strconv"
)

var (
	Filepath = "/tmp/noms-dbg.log"
	lg       = NewLogger(Filepath)
)

func NewLogger(fp string) *log.Logger {
	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	d.PanicIfError(err)
	pid := strconv.FormatInt(int64(os.Getpid()), 10)
	return log.New(f, pid+": ", 0644)
}

func GetLogger() *log.Logger {
	return lg
}

func SetLogger(newLg *log.Logger) {
	lg = newLg
}

func Debug(s string, args ...interface{}) {
	s1 := fmt.Sprintf(s, args...)
	lg.Println(s1)
}

func BoxF(s string, args ...interface{}) func() {
	s1 := fmt.Sprintf(s, args...)
	Debug("starting %s", s1)
	f := func() {
		Debug("finished %s", s1)
	}
	return f
}
