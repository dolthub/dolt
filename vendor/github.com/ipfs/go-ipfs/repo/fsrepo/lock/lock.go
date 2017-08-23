package lock

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"syscall"

	"gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	lock "gx/ipfs/QmWi28zbQG6B1xfaaWx5cYoLn3kBFU6pQ6GWQNRV5P6dNe/lock"
)

// LockFile is the filename of the repo lock, relative to config dir
// TODO rename repo lock and hide name
const LockFile = "repo.lock"

// log is the fsrepo logger
var log = logging.Logger("lock")

func errPerm(path string) error {
	return fmt.Errorf("failed to take lock at %s: permission denied", path)
}

func Lock(confdir string) (io.Closer, error) {
	return lock.Lock(path.Join(confdir, LockFile))
}

func Locked(confdir string) (bool, error) {
	log.Debugf("Checking lock")
	if !util.FileExists(path.Join(confdir, LockFile)) {
		log.Debugf("File doesn't exist: %s", path.Join(confdir, LockFile))
		return false, nil
	}
	if lk, err := Lock(confdir); err != nil {
		// EAGAIN == someone else has the lock
		if err == syscall.EAGAIN {
			log.Debugf("Someone else has the lock: %s", path.Join(confdir, LockFile))
			return true, nil
		}
		if strings.Contains(err.Error(), "resource temporarily unavailable") {
			log.Debugf("Can't lock file: %s.\n reason: %s", path.Join(confdir, LockFile), err.Error())
			return true, nil
		}

		// lock fails on permissions error
		if os.IsPermission(err) {
			log.Debugf("Lock fails on permissions error")
			return false, errPerm(confdir)
		}
		if isLockCreatePermFail(err) {
			log.Debugf("Lock fails on permissions error")
			return false, errPerm(confdir)
		}

		// otherwise, we cant guarantee anything, error out
		return false, err
	} else {
		log.Debugf("No one has a lock")
		lk.Close()
		return false, nil
	}
}

func isLockCreatePermFail(err error) bool {
	s := err.Error()
	return strings.Contains(s, "Lock Create of") && strings.Contains(s, "permission denied")
}
