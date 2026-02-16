package gitauth

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalize_WrapsCommonAuthPromptFailures(t *testing.T) {
	tests := []struct {
		name   string
		output string
		errMsg string
	}{
		{"terminal prompts disabled", "fatal: could not read Username for 'https://example.com': terminal prompts disabled", "git failed"},
		{"could not read Username", "fatal: could not read Username for 'https://example.com': No such device or address", "git failed"},
		{"could not read Password", "fatal: could not read Password for 'https://example.com': No such device or address", "git failed"},
		{"Authentication failed", "remote: Invalid username or password.\nfatal: Authentication failed for 'https://example.com/'", "git failed"},
		{"Enter passphrase for key", "Enter passphrase for key '/tmp/fake_key': ", "git failed"},
		{"Permission denied (publickey)", "Permission denied (publickey).", "git failed"},
		{"could not read from remote repository", "fatal: could not read from remote repository.", "git failed"},
		// If output is empty, Normalize should still match patterns in err.Error().
		{"match from err text", "", "fatal: could not read Username for 'https://example.com': terminal prompts disabled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := errors.New(tt.errMsg)
			got := Normalize(base, []byte(tt.output))

			var niae *NonInteractiveAuthError
			if !errors.As(got, &niae) {
				t.Fatalf("expected NonInteractiveAuthError, got %T: %v", got, got)
			}
			if niae.Cause == nil {
				t.Fatalf("expected Cause to be set")
			}
			msg := got.Error()
			if !strings.Contains(msg, "interactive prompting is disabled") {
				t.Fatalf("expected normalized message, got: %q", msg)
			}
			if !strings.Contains(msg, "Hints:") {
				t.Fatalf("expected hints, got: %q", msg)
			}
			if !strings.Contains(msg, "HTTPS:") || !strings.Contains(msg, "SSH:") {
				t.Fatalf("expected HTTPS and SSH hints, got: %q", msg)
			}
			// Ensure output is preserved in the error message when provided.
			if strings.TrimSpace(tt.output) != "" && !strings.Contains(msg, "Git output:") {
				t.Fatalf("expected git output section, got: %q", msg)
			}
		})
	}
}

func TestNormalize_NoMatch_ReturnsOriginalError(t *testing.T) {
	base := errors.New("some other failure")
	got := Normalize(base, []byte("unrelated output"))
	if got != base {
		t.Fatalf("expected original error, got %T: %v", got, got)
	}
}

func TestNormalize_Idempotent(t *testing.T) {
	base := errors.New("fatal: could not read Username for 'https://example.com': terminal prompts disabled")
	got1 := Normalize(base, nil)
	got2 := Normalize(got1, []byte("different output"))
	if got1 != got2 {
		t.Fatalf("expected Normalize to be idempotent when already normalized")
	}
}
