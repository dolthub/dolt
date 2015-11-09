package pkg

import (
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

// getDeps reads the types.Package objects referred to by depRefs out of cs and returns a map of ref: PackageDef.
func getDeps(deps []ref.Ref, cs chunks.ChunkStore) map[ref.Ref]types.Package {
	depsMap := map[ref.Ref]types.Package{}
	for _, depRef := range deps {
		v := types.ReadValue(depRef, cs)
		d.Chk.NotNil(v, "Importing package by ref %s failed.", depRef.String())
		depsMap[depRef] = v.(types.Package)
	}
	return depsMap
}

func resolveImports(aliases map[string]string, includePath string, cs chunks.ChunkStore) map[string]ref.Ref {
	canonicalize := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(includePath, path)
	}
	imports := map[string]ref.Ref{}

	for alias, target := range aliases {
		var r ref.Ref
		if d.Try(func() { r = ref.Parse(target) }) != nil {
			canonical := canonicalize(target)
			inFile, err := os.Open(canonical)
			d.Chk.NoError(err)
			defer inFile.Close()
			parsedDep := ParseNomDL(alias, inFile, filepath.Dir(canonical), cs)
			imports[alias] = types.WriteValue(parsedDep.Package, cs)
		} else {
			imports[alias] = r
		}
	}
	return imports
}

func importsToDeps(imports map[string]ref.Ref) []ref.Ref {
	depsSet := make(map[ref.Ref]bool, len(imports))
	deps := make([]ref.Ref, 0, len(imports))
	for _, target := range imports {
		if !depsSet[target] {
			deps = append(deps, target)
		}
		depsSet[target] = true
	}
	return deps
}
