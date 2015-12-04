package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/tools/runner"
)

const (
	buildScript = "build.go"
	pkgScript   = "pkg.go"

	nomsServer        = "http://ds.noms.io"
	nomsCheckoutPath  = "src/github.com/attic-labs/noms"
	atticCheckoutPath = "src/github.com/attic-labs/attic"
)

func main() {
	goPath := os.Getenv("GOPATH")
	d.Exp.NotEmpty(goPath, "GOPATH must be set!")
	workspace := os.Getenv("WORKSPACE")
	if workspace == "" {
		fmt.Printf("WORKSPACE not set in environment; using GOPATH (%s).\n", goPath)
		workspace = goPath
	}
	env := runner.Env{
		"GOPATH":              goPath,
		"PATH":                os.Getenv("PATH"),
		"NOMS_SERVER":         nomsServer,
		"NOMS_CHECKOUT_PATH":  filepath.Join(workspace, nomsCheckoutPath),
		"ATTIC_CHECKOUT_PATH": filepath.Join(workspace, atticCheckoutPath),
	}

	if !runner.Serial(os.Stdout, os.Stderr, env, ".", buildScript) {
		os.Exit(1)
	}
}
