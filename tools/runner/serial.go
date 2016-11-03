// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package runner

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/attic-labs/noms/go/d"
)

// Env is a map of env vars, mapping key string to value string.
type Env map[string]string

func (e Env) toStrings() (out []string) {
	out = os.Environ()
	// Sadly, it seems like we need to force-set GOROOT in the environment to handle some funky runtime environments (e.g. on our Travis setup)
	if e == nil {
		e = Env{}
	}

	if _, overridden := e["GOROOT"]; !overridden {
		e["GOROOT"] = runtime.GOROOT()
	}
	for n, v := range e {
		out = append(out, fmt.Sprintf("%s=%s", n, v))
	}
	return
}

// ForceRun runs 'exe [args...]' in current working directory, and d.Chk()s on failure. Inherits the environment of the current process.
func ForceRun(exe string, args ...string) {
	err := runEnvDir(os.Stdout, os.Stderr, Env{}, "", exe, args...)
	d.Chk.NoError(err)
}

// ForceRunInDir runs 'exe [args...]' in the given directory, and d.Chk()s on failure. Inherits the environment of the current process.
func ForceRunInDir(dir string, env Env, exe string, args ...string) {
	info, err := os.Stat(dir)
	if err != nil {
		d.Panic("Can't stat %s", dir)
	}
	if !info.IsDir() {
		d.Panic("%s must be a path to a directory.", dir)
	}
	d.Chk.NoError(runEnvDir(os.Stdout, os.Stderr, env, dir, exe, args...))
}

// RunInDir runs 'exe [args...]' in the given directory, returning any failure. The child's stdout and stderr are mapped to out and err respectively. Inherits the environment of the current process.
func RunInDir(out, err io.Writer, dir, exe string, args ...string) error {
	return runEnvDir(out, err, Env{}, dir, exe, args...)
}

// runEnvDir 'exe [args...]' in dir with the environment env overlaid on that of the current process. If dir == "", use the current working directory.
func runEnvDir(out, err io.Writer, env Env, dir, exe string, args ...string) error {
	cmd := exec.Command(exe, args...)
	cmd.Dir = dir
	cmd.Env = env.toStrings()
	cmd.Stdout = out
	cmd.Stderr = err
	return cmd.Run()
}

// Serial serially runs all instances of filename found under dir, mapping stdout and stderr to each subprocess in the obvious way. env is overlaid on the environment of the current process. If args are provided, they're passed en masse to each subprocess.
func Serial(stdout, stderr io.Writer, env Env, dir, filename string, args ...string) bool {
	success := true
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			// Some programs like npm create temporary log files which confuse filepath.Walk.
			return nil
		}
		if err != nil {
			d.Panic("Failed directory traversal at %s", path)
		}
		if !info.IsDir() && filepath.Base(path) == filename {
			scriptAndArgs := append([]string{filepath.Base(path)}, args...)
			runErr := runEnvDir(stdout, stderr, env, filepath.Dir(path), "python", scriptAndArgs...)
			if runErr != nil {
				success = false
				fmt.Fprintf(stderr, "Running %s failed with %v\n", path, runErr)
			}
		}
		return nil
	})
	d.PanicIfError(err)
	return success
}
