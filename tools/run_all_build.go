// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/tools/runner"
	flag "github.com/juju/gnuflag"
)

const (
	buildScript = "build.py"
	stageScript = "stage.py"

	nomsCheckoutPath  = "src/github.com/attic-labs/noms"
	atticCheckoutPath = "src/github.com/attic-labs/attic"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n  %s path/to/staging/dir\n", os.Args[0], os.Args[0])
	}
	flag.Parse(true)
	if flag.Arg(0) == "" {
		flag.Usage()
		os.Exit(1)
	}
	err := d.Try(func() {
		stagingDir, err := filepath.Abs(flag.Arg(0))
		if err != nil {
			d.Panic("Path to staging directory (first arg) must be valid, not %s", flag.Arg(0))
		}
		d.PanicIfError(os.MkdirAll(stagingDir, 0755))

		goPath := os.Getenv("GOPATH")
		if goPath == "" {
			d.Panic("GOPATH must be set!")
		}
		pythonPath := filepath.Join(goPath, nomsCheckoutPath, "tools")
		env := runner.Env{
			"PYTHONPATH": pythonPath,
		}

		if !runner.Serial(os.Stdout, os.Stderr, env, ".", buildScript) {
			os.Exit(1)
		}

		if !runner.Serial(os.Stdout, os.Stderr, env, ".", stageScript, stagingDir) {
			os.Exit(1)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
}
