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
