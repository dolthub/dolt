// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func CreateDatabaseSpecString(format *types.Format, protocol, db string) string {
	return Spec{Protocol: protocol, DatabaseName: db}.String(format)
}

func CreateValueSpecString(format *types.Format, protocol, db, path string) string {
	p, err := NewAbsolutePath(format, path)
	d.Chk.NoError(err)
	return Spec{Protocol: protocol, DatabaseName: db, Path: p}.String(format)
}

func CreateHashSpecString(format *types.Format, protocol, db string, h hash.Hash) string {
	return Spec{Protocol: protocol, DatabaseName: db, Path: AbsolutePath{Hash: h}}.String(format)
}
