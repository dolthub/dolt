package main

import (
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/tools/fileutil"
	"github.com/attic-labs/noms/tools/runner"
)

func main() {
	// ln -sf ../../js/.babelrc .babelrc hack, because zip files screw up symlinks.
	path, err := filepath.Abs(".babelrc")
	d.Chk.NoError(err)
	fileutil.ForceSymlink("../../js/.babelrc", path)

	path, err = filepath.Abs("link.sh")
	d.Chk.NoError(err)
	runner.ForceRun(path)

	runner.ForceRun("npm", "install")
	if _, present := os.LookupEnv("NOMS_SERVER"); !present {
		os.Setenv("NOMS_SERVER", "http://localhost:8000")
	}
	runner.ForceRun("npm", "run", "build")
}
