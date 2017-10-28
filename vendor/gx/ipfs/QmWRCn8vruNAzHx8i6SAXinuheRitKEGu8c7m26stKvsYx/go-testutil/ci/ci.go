// Package ci implements some helper functions to use during
// tests. Many times certain facilities are not available, or tests
// must run differently.
package ci

import (
	"os"

	travis "gx/ipfs/QmWRCn8vruNAzHx8i6SAXinuheRitKEGu8c7m26stKvsYx/go-testutil/ci/travis"
)

// EnvVar is a type to use travis-only env var names with
// the type system.
type EnvVar string

// Environment variables that TravisCI uses.
const (
	VarCI      EnvVar = "CI"
	VarNoFuse  EnvVar = "TEST_NO_FUSE"
	VarVerbose EnvVar = "TEST_VERBOSE"
)

// IsRunning attempts to determine whether this process is
// running on CI. This is done by checking any of:
//
//  CI=true
//  travis.IsRunning()
//
func IsRunning() bool {
	return os.Getenv(string(VarCI)) == "true" || travis.IsRunning()
}

// Env returns the value of a CI env variable.
func Env(v EnvVar) string {
	return os.Getenv(string(v))
}

// Returns whether FUSE is explicitly disabled wiht TEST_NO_FUSE.
func NoFuse() bool {
	return os.Getenv(string(VarNoFuse)) == "1"
}

// Returns whether TEST_VERBOSE is enabled.
func Verbose() bool {
	return os.Getenv(string(VarVerbose)) == "1"
}
