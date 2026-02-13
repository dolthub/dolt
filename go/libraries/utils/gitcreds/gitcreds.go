package gitcreds

import (
	"os"
	"sync"
)

// Session manages a temporary directory containing unix sockets for SSH
// connection multiplexing and git credential caching. All child git processes
// that inherit the env vars returned by Env() will share cached credentials
// for the lifetime of the session, avoiding repeated authentication prompts
// within a single dolt command.
type Session struct {
	dir string
	env []string
}

var (
	global   *Session
	globalMu sync.Mutex
	closed   bool
)

// Env returns environment variables that configure child git processes for
// credential caching. On first call it lazily creates a temp directory with
// mode 0700 for the unix sockets. Thread-safe.
//
// Returns nil if the session has been closed or if creation fails (best-effort).
func Env() []string {
	globalMu.Lock()
	defer globalMu.Unlock()
	if closed {
		return nil
	}
	if global != nil {
		return global.env
	}
	s, err := newSession()
	if err != nil {
		return nil
	}
	global = s
	return global.env
}

// Close tears down the global session, removing the temp directory and its
// sockets. Safe to call multiple times. After Close, Env() returns nil.
func Close() {
	globalMu.Lock()
	defer globalMu.Unlock()
	closed = true
	if global != nil {
		os.RemoveAll(global.dir)
		global = nil
	}
}

func newSession() (*Session, error) {
	dir, err := os.MkdirTemp("", "dolt-git-creds-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	var env []string

	// SSH connection multiplexing: reuse authenticated SSH connections across
	// multiple git subprocesses. Only set if the user hasn't already configured
	// GIT_SSH_COMMAND (to avoid conflicts with custom SSH setups).
	if os.Getenv("GIT_SSH_COMMAND") == "" {
		controlPath := dir + "/ssh-%h-%p"
		env = append(env,
			"GIT_SSH_COMMAND=ssh -o ControlMaster=auto -o ControlPath="+controlPath+" -o ControlPersist=60",
		)
	}

	// Git credential cache: in-memory credential daemon communicating over a
	// unix socket. First HTTP(S) auth prompt caches the credential; subsequent
	// git invocations retrieve it from the daemon without prompting.
	credSock := dir + "/cred-sock"
	env = append(env,
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=credential.helper",
		"GIT_CONFIG_VALUE_0=cache --timeout=300 --socket="+credSock,
	)

	return &Session{dir: dir, env: env}, nil
}
