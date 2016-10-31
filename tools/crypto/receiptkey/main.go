// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/attic-labs/noms/go/util/receipts"
)

func main() {
	var key receipts.Key
	rand.Read(key[:])
	fmt.Println(base64.URLEncoding.EncodeToString(key[:]))
}
