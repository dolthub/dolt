package gitcreds

import (
	"os"
	"strings"
	"testing"
)

func resetGlobal() {
	globalMu.Lock()
	defer globalMu.Unlock()
	if global != nil {
		os.RemoveAll(global.dir)
		global = nil
	}
	closed = false
}

func TestEnv_LazyInit(t *testing.T) {
	resetGlobal()
	defer resetGlobal()

	env := Env()
	if env == nil {
		t.Fatal("expected non-nil env")
	}

	// Should contain credential.helper config.
	found := false
	for _, e := range env {
		if strings.HasPrefix(e, "GIT_CONFIG_KEY_0=credential.helper") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected credential.helper in env, got: %v", env)
	}

	// Temp dir should exist.
	globalMu.Lock()
	dir := global.dir
	globalMu.Unlock()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("temp dir should exist: %v", err)
	}
}

func TestEnv_ReturnsSameSlice(t *testing.T) {
	resetGlobal()
	defer resetGlobal()

	env1 := Env()
	env2 := Env()
	if len(env1) != len(env2) {
		t.Fatal("expected same env on repeated calls")
	}
	for i := range env1 {
		if env1[i] != env2[i] {
			t.Fatalf("env mismatch at %d: %q vs %q", i, env1[i], env2[i])
		}
	}
}

func TestClose_RemovesTempDir(t *testing.T) {
	resetGlobal()
	defer resetGlobal()

	Env()
	globalMu.Lock()
	dir := global.dir
	globalMu.Unlock()

	Close()

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("expected temp dir to be removed, got err: %v", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	resetGlobal()
	defer resetGlobal()

	Env()
	Close()
	Close() // should not panic
}

func TestEnv_ReturnsNilAfterClose(t *testing.T) {
	resetGlobal()
	defer resetGlobal()

	Env()
	Close()
	if env := Env(); env != nil {
		t.Fatalf("expected nil after Close, got: %v", env)
	}
}

func TestEnv_SkipsSSHCommandWhenAlreadySet(t *testing.T) {
	resetGlobal()
	defer resetGlobal()

	orig := os.Getenv("GIT_SSH_COMMAND")
	os.Setenv("GIT_SSH_COMMAND", "ssh -o SomeOption=yes")
	defer func() {
		if orig == "" {
			os.Unsetenv("GIT_SSH_COMMAND")
		} else {
			os.Setenv("GIT_SSH_COMMAND", orig)
		}
	}()

	env := Env()
	for _, e := range env {
		if strings.HasPrefix(e, "GIT_SSH_COMMAND=") {
			t.Fatalf("should not override GIT_SSH_COMMAND, but found: %s", e)
		}
	}
}

func TestEnv_IncludesSSHCommandWhenNotSet(t *testing.T) {
	resetGlobal()
	defer resetGlobal()

	orig := os.Getenv("GIT_SSH_COMMAND")
	os.Unsetenv("GIT_SSH_COMMAND")
	defer func() {
		if orig != "" {
			os.Setenv("GIT_SSH_COMMAND", orig)
		}
	}()

	env := Env()
	found := false
	for _, e := range env {
		if strings.HasPrefix(e, "GIT_SSH_COMMAND=") {
			found = true
			if !strings.Contains(e, "ControlMaster=auto") {
				t.Fatalf("expected ControlMaster in GIT_SSH_COMMAND, got: %s", e)
			}
		}
	}
	if !found {
		t.Fatal("expected GIT_SSH_COMMAND in env")
	}
}
