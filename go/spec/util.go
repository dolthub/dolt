// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

func CreateDatabaseSpecString(protocol, db string) string {
	return Spec{Protocol: protocol, DatabaseName: db}.String()
}

func CreateValueSpecString(protocol, db, path string) string {
	p, err := NewAbsolutePath(path)
	d.Chk.NoError(err)
	return Spec{Protocol: protocol, DatabaseName: db, Path: p}.String()
}

func CreateHashSpecString(protocol, db string, h hash.Hash) string {
	return Spec{Protocol: protocol, DatabaseName: db, Path: AbsolutePath{Hash: h}}.String()
}
