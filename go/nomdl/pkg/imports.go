// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package pkg

import (
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

func resolveImports(aliases map[string]string, includePath string) (imports map[string][]*types.Type) {
	canonicalize := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(includePath, path)
	}

	for alias, target := range aliases {
		canonical := canonicalize(target)
		inFile, err := os.Open(canonical)
		d.Chk.NoError(err)
		defer inFile.Close()
		ts := ParseNomDL(alias, inFile, filepath.Dir(canonical))
		imports[alias] = ts
	}
	return
}
