// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"fmt"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	flag "github.com/juju/gnuflag"
)

func RegisterDatabaseFlags(flags *flag.FlagSet) {
	chunks.RegisterLevelDBFlags(flags)
}

func CreateDatabaseSpecString(protocol, path string) string {
	return fmt.Sprintf("%s:%s", protocol, path)
}

func CreateValueSpecString(protocol, path, value string) string {
	return fmt.Sprintf("%s:%s%s%s", protocol, path, Separator, value)
}

func CreateHashSpecString(protocol, path string, h hash.Hash) string {
	return fmt.Sprintf("%s:%s%s#%s", protocol, path, Separator, h.String())
}
