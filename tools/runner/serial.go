package runner

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/attic-labs/noms/d"
)

// Env is a map of env vars, mapping key string to value string.
type Env map[string]string

func (e Env) toStrings() (out []string) {
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

// runEnvDir 'exe [args...]' in dir with the environment env. If dir == "", use the current working directory. If env contains no mappings, then use the environment of the current process.
func runEnvDir(out, err io.Writer, env Env, dir, exe string, args ...string) error {
	cmd := exec.Command(exe, args...)
	cmd.Dir = dir
	cmd.Env = env.toStrings()
	cmd.Stdout = out
	cmd.Stderr = err
	return cmd.Run()
}

// Serial serially runs all instances of filename found under dir, mapping stdout and stderr to each subprocess in the obvious way. env is an optional execution environment for subprocesses.
func Serial(stdout, stderr io.Writer, env Env, dir, filename string) bool {
	success := true
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		d.Exp.NoError(err, "Failed directory traversal at %s", path)
		if !info.IsDir() && filepath.Base(path) == filename {
			runErr := runEnvDir(stdout, stderr, env, filepath.Dir(path), "go", "run", filepath.Base(path))
			if runErr != nil {
				success = false
				fmt.Fprintf(stderr, "Running %s failed with %v", path, runErr)
			}
		}
		return nil
	})
	d.Exp.NoError(err)
	return success
}
