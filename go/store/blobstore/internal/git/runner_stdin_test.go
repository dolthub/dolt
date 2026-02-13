//go:build unix

package git

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
)

// TestRunnerHelperProcess is executed as a subprocess by tests in this file.
//
// It uses non-blocking reads on stdin to distinguish:
// - /dev/null: readable immediately, read returns (0, nil) => prints "EOF"
// - pipe/tty with no available bytes: read returns EAGAIN => prints "WAIT"
func TestRunnerHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	// Find args after "--".
	var cmd string
	for i := 0; i < len(os.Args)-1; i++ {
		if os.Args[i] == "--" {
			cmd = os.Args[i+1]
			break
		}
	}
	if cmd != "stdin-check" {
		fmt.Fprint(os.Stdout, "ERR:missing-stdin-check")
		os.Exit(2)
	}

	fd := int(os.Stdin.Fd())
	_ = syscall.SetNonblock(fd, true)

	buf := make([]byte, 1)
	n, err := os.Stdin.Read(buf)
	switch {
	case n == 0 && (err == nil || errors.Is(err, io.EOF)):
		fmt.Fprint(os.Stdout, "EOF")
		os.Exit(0)
	case n > 0:
		fmt.Fprint(os.Stdout, "DATA")
		os.Exit(0)
	case err != nil && (errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)):
		fmt.Fprint(os.Stdout, "WAIT")
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stdout, "ERR:%v", err)
		os.Exit(1)
	}
}

func TestRunnerRun_DefaultStdinInheritsOsStdin(t *testing.T) {
	ctx := context.Background()

	origStdin := os.Stdin
	defer func() { os.Stdin = origStdin }()

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipeR.Close()
	defer pipeW.Close() // keep writer open so pipeR is not EOF

	// If Runner.Run inherits os.Stdin, the helper will report WAIT (no bytes available).
	os.Stdin = pipeR

	gitDir := t.TempDir()
	r := NewRunnerWithGitPath(gitDir, os.Args[0]).WithExtraEnv("GO_WANT_HELPER_PROCESS=1")

	out, err := r.Run(ctx, RunOptions{}, "-test.run=TestRunnerHelperProcess", "--", "stdin-check")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(out))
	if got != "WAIT" {
		t.Fatalf("unexpected stdin probe result: got %q, want %q", got, "WAIT")
	}
}

func TestRunnerStart_DefaultStdinInheritsOsStdin(t *testing.T) {
	ctx := context.Background()

	origStdin := os.Stdin
	defer func() { os.Stdin = origStdin }()

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipeR.Close()
	defer pipeW.Close() // keep writer open so pipeR is not EOF

	os.Stdin = pipeR

	gitDir := t.TempDir()
	r := NewRunnerWithGitPath(gitDir, os.Args[0]).WithExtraEnv("GO_WANT_HELPER_PROCESS=1")

	rc, _, err := r.Start(ctx, RunOptions{}, "-test.run=TestRunnerHelperProcess", "--", "stdin-check")
	if err != nil {
		t.Fatal(err)
	}
	out, rerr := io.ReadAll(rc)
	cerr := rc.Close()
	if rerr != nil {
		t.Fatal(rerr)
	}
	if cerr != nil {
		t.Fatal(cerr)
	}

	got := strings.TrimSpace(string(out))
	if got != "WAIT" {
		t.Fatalf("unexpected stdin probe result: got %q, want %q", got, "WAIT")
	}
}
