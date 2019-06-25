// Package utils contains utility functions for use throughout git-dolt.
package utils

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

// EnsureSuffix adds a suffix to a string if not already present.
func EnsureSuffix(s string, suffix string) string {
	if !strings.HasSuffix(s, suffix) {
		return s + suffix
	}
	return s
}

// LastSegment gets the last segment of a slash-separated string.
func LastSegment(s string) string {
	tokens := strings.Split(s, "/")
	return tokens[len(tokens)-1]
}

var hashRegex = regexp.MustCompile(`[0-9a-v]{32}`)

// CurrentRevision gets the commit hash of the currently checked-out revision of
// the dolt repo at the given dirname.
func CurrentRevision(dirname string) (string, error) {
	cmd := exec.Command("dolt", "log", "-n", "1")
	cmd.Dir = dirname
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("error running dolt log to find current revision: %v", err)
	}
	return hashRegex.FindString(string(out)), nil
}
