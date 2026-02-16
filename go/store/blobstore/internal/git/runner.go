// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package git provides helpers for invoking git plumbing commands against a bare
// repository or .git directory without a working tree checkout.
package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/gitauth"
)

const maxCapturedOutputBytes = 64 * 1024

// Runner executes git commands with GIT_DIR set (and optionally GIT_INDEX_FILE).
// It is intended for git plumbing usage and should not require a working tree.
type Runner struct {
	gitPath string
	gitDir  string
	// extraEnv is appended to os.Environ() for every command.
	extraEnv []string
}

// NewRunner creates a Runner using the git binary on PATH.
func NewRunner(gitDir string) (*Runner, error) {
	p, err := exec.LookPath("git")
	if err != nil {
		return nil, fmt.Errorf("git not found on PATH: %w", err)
	}
	return NewRunnerWithGitPath(gitDir, p), nil
}

// NewRunnerWithGitPath creates a Runner using an explicit git binary path.
func NewRunnerWithGitPath(gitDir, gitPath string) *Runner {
	return &Runner{
		gitPath: gitPath,
		gitDir:  gitDir,
	}
}

// WithExtraEnv returns a copy of r that appends env entries (e.g. "K=V") to all commands.
func (r *Runner) WithExtraEnv(env ...string) *Runner {
	cp := *r
	cp.extraEnv = append(append([]string(nil), r.extraEnv...), env...)
	return &cp
}

// RunOptions control a single git invocation.
type RunOptions struct {
	// Dir is the working directory for the git process. Optional.
	Dir string
	// IndexFile sets GIT_INDEX_FILE for the git process. Optional.
	IndexFile string
	// Stdin provides stdin to the git process. Optional.
	Stdin io.Reader
	// Stdout and Stderr override output destinations. If both are nil, output is captured and returned.
	Stdout io.Writer
	Stderr io.Writer
	// Env is appended to the process environment.
	Env []string
}

// CmdError represents a failed git invocation with captured output.
type CmdError struct {
	Args     []string
	Dir      string
	ExitCode int
	Output   []byte
	Cause    error
}

func (e *CmdError) Error() string {
	var b strings.Builder
	b.WriteString("git command failed")
	if e.ExitCode != 0 {
		b.WriteString(fmt.Sprintf(" (exit %d)", e.ExitCode))
	}
	if len(e.Args) > 0 {
		b.WriteString("\ncommand: git ")
		b.WriteString(strings.Join(e.Args, " "))
	}
	if e.Dir != "" {
		b.WriteString("\ndir: ")
		b.WriteString(e.Dir)
	}
	b.WriteString("\noutput:\n")
	b.WriteString(formatOutput(e.Output))
	if e.Cause != nil {
		b.WriteString("\nerror: ")
		b.WriteString(e.Cause.Error())
	}
	return b.String()
}

func (e *CmdError) Unwrap() error { return e.Cause }

// Run executes "git <args...>" with GIT_DIR set and returns captured combined output
// when Stdout/Stderr are not supplied.
func (r *Runner) Run(ctx context.Context, opts RunOptions, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.gitPath, args...) //nolint:gosec // args are controlled by caller; used for internal plumbing.
	if opts.Dir != "" {
		cmd.Dir = opts.Dir
	}
	cmd.Env = r.env(opts)

	if opts.Stdin != nil {
		cmd.Stdin = opts.Stdin
	}

	// Capture combined output unless caller provided destinations.
	var buf bytes.Buffer
	if opts.Stdout == nil && opts.Stderr == nil {
		cmd.Stdout = &buf
		cmd.Stderr = &buf
	} else {
		if opts.Stdout != nil {
			cmd.Stdout = opts.Stdout
		}
		if opts.Stderr != nil {
			cmd.Stderr = opts.Stderr
		} else if opts.Stdout != nil {
			// Reasonable default: if only Stdout is set, send stderr there too.
			cmd.Stderr = opts.Stdout
		}
	}

	err := cmd.Run()
	out := buf.Bytes()
	if err == nil {
		return out, nil
	}

	exitCode := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		exitCode = ee.ExitCode()
	}
	cerr := &CmdError{
		Args:     append([]string(nil), args...),
		Dir:      cmd.Dir,
		ExitCode: exitCode,
		Output:   out,
		Cause:    err,
	}
	return out, gitauth.Normalize(cerr, out)
}

// Start starts "git <args...>" and returns a ReadCloser for stdout.
//
// Resource management:
//   - Call Close() on the returned ReadCloser to ensure the underlying git process
//     is waited (cmd.Wait()) and resources are released.
//   - The returned *exec.Cmd is provided for advanced uses (e.g. signals), but most
//     callers should not call Wait() directly.
func (r *Runner) Start(ctx context.Context, opts RunOptions, args ...string) (io.ReadCloser, *exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, r.gitPath, args...) //nolint:gosec // args are controlled by caller; used for internal plumbing.
	if opts.Dir != "" {
		cmd.Dir = opts.Dir
	}
	cmd.Env = r.env(opts)
	if opts.Stdin != nil {
		cmd.Stdin = opts.Stdin
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	// Capture stderr into a buffer so failures have actionable output.
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, nil, err
	}

	// Wrap stdout so that Close also waits to avoid zombies if callers bail early.
	rc := &cmdReadCloser{
		r:      stdout,
		cmd:    cmd,
		stderr: &stderr,
		args:   append([]string(nil), args...),
		dir:    cmd.Dir,
	}
	return rc, cmd, nil
}

type cmdReadCloser struct {
	r      io.ReadCloser
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	args   []string
	dir    string
}

func (c *cmdReadCloser) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *cmdReadCloser) Close() error {
	_ = c.r.Close()
	err := c.cmd.Wait()
	if err == nil {
		return nil
	}
	exitCode := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		exitCode = ee.ExitCode()
	}
	cerr := &CmdError{
		Args:     c.args,
		Dir:      c.dir,
		ExitCode: exitCode,
		Output:   c.stderr.Bytes(),
		Cause:    err,
	}
	return gitauth.Normalize(cerr, cerr.Output)
}

func (r *Runner) env(opts RunOptions) []string {
	env := append([]string(nil), os.Environ()...)
	env = append(env, "GIT_DIR="+r.gitDir)
	if opts.IndexFile != "" {
		env = append(env, "GIT_INDEX_FILE="+opts.IndexFile)
	}
	env = append(env, r.extraEnv...)
	env = append(env, opts.Env...)
	return env
}

func formatOutput(out []byte) string {
	if len(out) == 0 {
		return "(no output)"
	}
	if len(out) <= maxCapturedOutputBytes {
		return strings.TrimRight(string(out), "\n")
	}
	trimmed := out[len(out)-maxCapturedOutputBytes:]
	return fmt.Sprintf("... (truncated; showing last %d bytes)\n%s", maxCapturedOutputBytes, strings.TrimRight(string(trimmed), "\n"))
}
