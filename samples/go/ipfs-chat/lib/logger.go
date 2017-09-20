// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"fmt"
	"log"
	"os"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
)

func NewLogger(username string) *log.Logger {
	f, err := os.OpenFile(dbg.Filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	d.PanicIfError(err)
	prefix := fmt.Sprintf("%d-%s: ", os.Getpid(), username)
	return log.New(f, prefix, 0644)
}
