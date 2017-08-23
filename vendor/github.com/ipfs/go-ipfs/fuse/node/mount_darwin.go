// +build !nofuse

package node

import (
	"bytes"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"syscall"

	core "github.com/ipfs/go-ipfs/core"

	"gx/ipfs/QmU1N5xVAUXgo3XRTt6GhJ2SuJEbxj2zRgMS7FpjSR2U83/semver"
)

func init() {
	// this is a hack, but until we need to do it another way, this works.
	platformFuseChecks = darwinFuseCheckVersion
}

// dontCheckOSXFUSEConfigKey is a key used to let the user tell us to
// skip fuse checks.
var dontCheckOSXFUSEConfigKey = "DontCheckOSXFUSE"

// fuseVersionPkg is the go pkg url for fuse-version
var fuseVersionPkg = "github.com/jbenet/go-fuse-version/fuse-version"

// errStrFuseRequired is returned when we're sure the user does not have fuse.
var errStrFuseRequired = `OSXFUSE not found.

OSXFUSE is required to mount, please install it.
NOTE: Version 2.7.2 or higher required; prior versions are known to kernel panic!
It is recommended you install it from the OSXFUSE website:

	http://osxfuse.github.io/

For more help, see:

	https://github.com/ipfs/go-ipfs/issues/177
`

// errStrNoFuseHeaders is included in the output of `go get <fuseVersionPkg>` if there
// are no fuse headers. this means they dont have OSXFUSE installed.
var errStrNoFuseHeaders = "no such file or directory: '/usr/local/lib/libosxfuse.dylib'"

var errStrUpgradeFuse = `OSXFUSE version %s not supported.

OSXFUSE versions <2.7.2 are known to cause kernel panics!
Please upgrade to the latest OSXFUSE version.
It is recommended you install it from the OSXFUSE website:

	http://osxfuse.github.io/

For more help, see:

	https://github.com/ipfs/go-ipfs/issues/177
`

var errStrNeedFuseVersion = `unable to check fuse version.

Dear User,

Before mounting, we must check your version of OSXFUSE. We are protecting
you from a nasty kernel panic we found in OSXFUSE versions <2.7.2.[1]. To
make matters worse, it's harder than it should be to check whether you have
the right version installed...[2]. We've automated the process with the
help of a little tool. We tried to install it, but something went wrong[3].
Please install it yourself by running:

	go get %s

You can also stop ipfs from running these checks and use whatever OSXFUSE
version you have by running:

	ipfs config %s true

[1]: https://github.com/ipfs/go-ipfs/issues/177
[2]: https://github.com/ipfs/go-ipfs/pull/533
[3]: %s
`

var errStrFailedToRunFuseVersion = `unable to check fuse version.

Dear User,

Before mounting, we must check your version of OSXFUSE. We are protecting
you from a nasty kernel panic we found in OSXFUSE versions <2.7.2.[1]. To
make matters worse, it's harder than it should be to check whether you have
the right version installed...[2]. We've automated the process with the
help of a little tool. We tried to run it, but something went wrong[3].
Please, try to run it yourself with:

	go get %s
	fuse-version

You should see something like this:

	> fuse-version
	fuse-version -only agent
	OSXFUSE.AgentVersion: 2.7.3

Just make sure the number is 2.7.2 or higher. You can then stop ipfs from
trying to run these checks with:

	ipfs config %s true

[1]: https://github.com/ipfs/go-ipfs/issues/177
[2]: https://github.com/ipfs/go-ipfs/pull/533
[3]: %s
`

var errStrFixConfig = `config key invalid: %s %v
You may be able to get this error to go away by setting it again:

	ipfs config %s true

Either way, please tell us at: http://github.com/ipfs/go-ipfs/issues
`

func darwinFuseCheckVersion(node *core.IpfsNode) error {
	// on OSX, check FUSE version.
	if runtime.GOOS != "darwin" {
		return nil
	}

	ov, errGFV := tryGFV()
	if errGFV != nil {
		// if we failed AND the user has told us to ignore the check we
		// continue. this is in case fuse-version breaks or the user cannot
		// install it, but is sure their fuse version will work.
		if skip, err := userAskedToSkipFuseCheck(node); err != nil {
			return err
		} else if skip {
			return nil // user told us not to check version... ok....
		} else {
			return errGFV
		}
	}

	log.Debug("mount: osxfuse version:", ov)

	min := semver.MustParse("2.7.2")
	curr, err := semver.Make(ov)
	if err != nil {
		return err
	}

	if curr.LT(min) {
		return fmt.Errorf(errStrUpgradeFuse, ov)
	}
	return nil
}

func tryGFV() (string, error) {
	// first try sysctl. it may work!
	ov, err := trySysctl()
	if err == nil {
		return ov, nil
	}
	log.Debug(err)

	return tryGFVFromFuseVersion()
}

func trySysctl() (string, error) {
	v, err := syscall.Sysctl("osxfuse.version.number")
	if err != nil {
		log.Debug("mount: sysctl osxfuse.version.number:", "failed")
		return "", err
	}
	log.Debug("mount: sysctl osxfuse.version.number:", v)
	return v, nil
}

func tryGFVFromFuseVersion() (string, error) {
	if err := ensureFuseVersionIsInstalled(); err != nil {
		return "", err
	}

	cmd := exec.Command("fuse-version", "-q", "-only", "agent", "-s", "OSXFUSE")
	out := new(bytes.Buffer)
	cmd.Stdout = out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf(errStrFailedToRunFuseVersion, fuseVersionPkg, dontCheckOSXFUSEConfigKey, err)
	}

	return out.String(), nil
}

func ensureFuseVersionIsInstalled() error {
	// see if fuse-version is there
	if _, err := exec.LookPath("fuse-version"); err == nil {
		return nil // got it!
	}

	// try installing it...
	log.Debug("fuse-version: no fuse-version. attempting to install.")
	cmd := exec.Command("go", "get", "github.com/jbenet/go-fuse-version/fuse-version")
	cmdout := new(bytes.Buffer)
	cmd.Stdout = cmdout
	cmd.Stderr = cmdout
	if err := cmd.Run(); err != nil {
		// Ok, install fuse-version failed. is it they dont have fuse?
		cmdoutstr := cmdout.String()
		if strings.Contains(cmdoutstr, errStrNoFuseHeaders) {
			// yes! it is! they dont have fuse!
			return fmt.Errorf(errStrFuseRequired)
		}

		log.Debug("fuse-version: failed to install.")
		s := err.Error() + "\n" + cmdoutstr
		return fmt.Errorf(errStrNeedFuseVersion, fuseVersionPkg, dontCheckOSXFUSEConfigKey, s)
	}

	// ok, try again...
	if _, err := exec.LookPath("fuse-version"); err != nil {
		log.Debug("fuse-version: failed to install?")
		return fmt.Errorf(errStrNeedFuseVersion, fuseVersionPkg, dontCheckOSXFUSEConfigKey, err)
	}

	log.Debug("fuse-version: install success")
	return nil
}

func userAskedToSkipFuseCheck(node *core.IpfsNode) (skip bool, err error) {
	val, err := node.Repo.GetConfigKey(dontCheckOSXFUSEConfigKey)
	if err != nil {
		return false, nil // failed to get config value. dont skip check.
	}

	switch val := val.(type) {
	case string:
		return val == "true", nil
	case bool:
		return val, nil
	default:
		// got config value, but it's invalid... dont skip check, ask the user to fix it...
		return false, fmt.Errorf(errStrFixConfig, dontCheckOSXFUSEConfigKey, val,
			dontCheckOSXFUSEConfigKey)
	}
}
