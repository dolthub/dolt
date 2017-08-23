// Package travis implements some helper functions to use during
// tests. Many times certain facilities are not available, or tests
// must run differently.
package travis

import "os"

// EnvVar is a type to use travis-only env var names with
// the type system.
type EnvVar string

// Environment variables that TravisCI uses.
const (
	VarCI            EnvVar = "CI"
	VarTravis        EnvVar = "TRAVIS"
	VarBranch        EnvVar = "TRAVIS_BRANCH"
	VarBuildDir      EnvVar = "TRAVIS_BUILD_DIR"
	VarBuildId       EnvVar = "TRAVIS_BUILD_ID"
	VarBuildNumber   EnvVar = "TRAVIS_BUILD_NUMBER"
	VarCommit        EnvVar = "TRAVIS_COMMIT"
	VarCommitRange   EnvVar = "TRAVIS_COMMIT_RANGE"
	VarJobId         EnvVar = "TRAVIS_JOB_ID"
	VarJobNumber     EnvVar = "TRAVIS_JOB_NUMBER"
	VarPullRequest   EnvVar = "TRAVIS_PULL_REQUEST"
	VarSecureEnvVars EnvVar = "TRAVIS_SECURE_ENV_VARS"
	VarRepoSlug      EnvVar = "TRAVIS_REPO_SLUG"
	VarOsName        EnvVar = "TRAVIS_OS_NAME"
	VarTag           EnvVar = "TRAVIS_TAG"
	VarGoVersion     EnvVar = "TRAVIS_GO_VERSION"
)

// IsRunning attempts to determine whether this process is
// running on Travis-CI. This is done by checking ALL of the
// following env vars are set:
//
//  CI=true
//  TRAVIS=true
//
// NOTE: cannot just check CI.
func IsRunning() bool {
	return Env(VarCI) == "true" && Env(VarTravis) == "true"
}

// Env returns the value of a travis env variable.
func Env(v EnvVar) string {
	return os.Getenv(string(v))
}

// JobId returns the travis JOB_ID of this build.
func JobId() string {
	return Env(VarJobId)
}

// JobNumber returns the travis JOB_NUMBER of this build.
func JobNumber() string {
	return Env(VarJobNumber)
}
