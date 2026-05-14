package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPreScanDirFlag tests the lightweight pre-scan that resolves -C/--directory
// before the full arg parse runs. --chdir is handled separately in runMain.

func TestPreScanDirFlag(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantDir string
	}{
		{
			name:    "no dir flag",
			args:    []string{"status"},
			wantDir: "",
		},
		{
			name:    "-C flag",
			args:    []string{"-C", "/some/path", "status"},
			wantDir: "/some/path",
		},
		{
			name:    "--directory flag",
			args:    []string{"--directory", "/some/path", "status"},
			wantDir: "/some/path",
		},
		{
			name:    "--chdir not scanned here (handled in runMain)",
			args:    []string{"--chdir", "/some/path", "status"},
			wantDir: "",
		},
		{
			name:    "-C with relative path",
			args:    []string{"-C", "subdir", "status"},
			wantDir: "subdir",
		},
		{
			name:    "-C flag missing value",
			args:    []string{"-C"},
			wantDir: "",
		},
		{
			name:    "--directory flag missing value",
			args:    []string{"--directory"},
			wantDir: "",
		},
		{
			name:    "-C flag among other flags",
			args:    []string{"--user", "root", "-C", "/my/db", "sql"},
			wantDir: "/my/db",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotDir := preScanDirFlag(tc.args)
			if gotDir != tc.wantDir {
				t.Errorf("preScanDirFlag(%v) = %q, want %q", tc.args, gotDir, tc.wantDir)
			}
		})
	}
}

// buildDolt compiles the dolt binary and returns the path. Results are cached per test binary run.
var (
	builtDoltBin  string
	builtDoltErr  error
	builtDoltOnce = new(struct{ done bool })
)

func buildDolt(t *testing.T) string {
	t.Helper()
	if os.Getenv("DOLT_TEST_BUILD") != "1" {
		t.Skip("set DOLT_TEST_BUILD=1 to run dolt binary integration tests")
	}
	if !builtDoltOnce.done {
		builtDoltOnce.done = true
		bin := filepath.Join(t.TempDir(), "dolt")
		cmd := exec.Command("go", "build", "-o", bin, ".")
		cmd.Dir = filepath.Join(mustFindModRoot(t), "cmd/dolt")
		out, err := cmd.CombinedOutput()
		if err != nil {
			builtDoltErr = err
			t.Logf("build output: %s", out)
		}
		builtDoltBin = bin
	}
	if builtDoltErr != nil {
		t.Fatalf("failed to build dolt: %v", builtDoltErr)
	}
	return builtDoltBin
}

func mustFindModRoot(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(exe)
	// Walk up to find go.mod
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find go.mod")
		}
		dir = parent
	}
}

// TestDirectoryFlagIntegration tests the -C/--directory/--chdir flags end-to-end
// by running the compiled dolt binary. Requires DOLT_TEST_BUILD=1.
func TestDirectoryFlagIntegration(t *testing.T) {
	dolt := buildDolt(t)

	// Create a temp directory with a dolt repo initialised inside it.
	repoDir := t.TempDir()
	initCmd := exec.Command(dolt, "init")
	initCmd.Dir = repoDir
	if out, err := initCmd.CombinedOutput(); err != nil {
		t.Fatalf("dolt init failed: %v\n%s", err, out)
	}

	otherDir := t.TempDir()

	t.Run("C_flag_runs_from_target_dir", func(t *testing.T) {
		cmd := exec.Command(dolt, "-C", repoDir, "status")
		cmd.Dir = otherDir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("dolt -C status failed: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "On branch") && !strings.Contains(string(out), "nothing to commit") {
			t.Errorf("unexpected status output: %s", out)
		}
	})

	t.Run("directory_long_flag_runs_from_target_dir", func(t *testing.T) {
		cmd := exec.Command(dolt, "--directory", repoDir, "status")
		cmd.Dir = otherDir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("dolt --directory status failed: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "On branch") && !strings.Contains(string(out), "nothing to commit") {
			t.Errorf("unexpected status output: %s", out)
		}
	})

	t.Run("chdir_deprecated_flag_warns_and_works", func(t *testing.T) {
		cmd := exec.Command(dolt, "--chdir", repoDir, "status")
		cmd.Dir = otherDir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("dolt --chdir status failed: %v\n%s", err, out)
		}
		outStr := string(out)
		if !strings.Contains(outStr, "deprecated") {
			t.Errorf("expected deprecation warning for --chdir, got: %s", outStr)
		}
		if !strings.Contains(outStr, "On branch") && !strings.Contains(outStr, "nothing to commit") {
			t.Errorf("unexpected status output: %s", outStr)
		}
	})

	t.Run("C_flag_nonexistent_dir_errors_cleanly", func(t *testing.T) {
		cmd := exec.Command(dolt, "-C", "/nonexistent/dolt-C-test-path", "status")
		cmd.Dir = otherDir
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected failure for -C /nonexistent, got: %s", out)
		}
		if !strings.Contains(string(out), "cannot change to directory") {
			t.Errorf("expected error message, got: %s", out)
		}
	})

	t.Run("no_C_flag_uses_current_dir", func(t *testing.T) {
		// Running without -C from repoDir should also work.
		cmd := exec.Command(dolt, "status")
		cmd.Dir = repoDir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("dolt status failed: %v\n%s", err, out)
		}
	})

	t.Run("C_flag_does_not_mutate_cwd", func(t *testing.T) {
		// The first invocation uses -C to reach repoDir. The second does NOT use -C
		// and runs from otherDir (no dolt repo). It must fail — proving -C did not
		// permanently mutate the process cwd.
		cmd1 := exec.Command(dolt, "-C", repoDir, "status")
		cmd1.Dir = otherDir
		if out, err := cmd1.CombinedOutput(); err != nil {
			t.Fatalf("first dolt -C status failed: %v\n%s", err, out)
		}

		cmd2 := exec.Command(dolt, "status")
		cmd2.Dir = otherDir
		var stderr2 bytes.Buffer
		cmd2.Stderr = &stderr2
		_, err := cmd2.CombinedOutput()
		if err == nil {
			t.Fatal("second dolt status (no -C) should have failed in a non-repo dir")
		}
	})
}
