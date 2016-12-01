// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"log"
	"os"
)

const dropCaches = "/proc/sys/vm/drop_caches"

func main() {
	f, err := os.OpenFile(dropCaches, os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln(err)
	}

	if _, err := f.WriteString("1"); err != nil {
		log.Fatalln(err)
	}
}
