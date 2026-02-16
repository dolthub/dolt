package gitauth

import "os"

// DisableInteractivePrompts enforces a non-interactive git authentication policy
// for the current process by overriding environment variables.
//
// This prevents git / ssh from prompting for credentials (stdin / askpass), so
// operations fail fast when credentials are unavailable.
func DisableInteractivePrompts() {
	// For HTTPS remotes, disable username/password prompting.
	_ = os.Setenv("GIT_TERMINAL_PROMPT", "0")
	// Disable interactive Git Credential Manager flows (where installed).
	_ = os.Setenv("GCM_INTERACTIVE", "Never")
	// For SSH remotes, prevent passphrase/password prompting.
	_ = os.Setenv("GIT_SSH_COMMAND", "ssh -o BatchMode=yes")
}
