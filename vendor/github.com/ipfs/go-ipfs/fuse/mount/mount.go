// package mount provides a simple abstraction around a mount point
package mount

import (
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"time"

	goprocess "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
	logging "gx/ipfs/QmZChCsSt8DctjceaL56Eibc29CVQq4dGKRXC5JRZ6Ppae/go-log"
)

var log = logging.Logger("mount")

var MountTimeout = time.Second * 5

// Mount represents a filesystem mount
type Mount interface {
	// MountPoint is the path at which this mount is mounted
	MountPoint() string

	// Unmounts the mount
	Unmount() error

	// Checks if the mount is still active.
	IsActive() bool

	// Process returns the mount's Process to be able to link it
	// to other processes. Unmount upon closing.
	Process() goprocess.Process
}

// ForceUnmount attempts to forcibly unmount a given mount.
// It does so by calling diskutil or fusermount directly.
func ForceUnmount(m Mount) error {
	point := m.MountPoint()
	log.Warningf("Force-Unmounting %s...", point)

	cmd, err := UnmountCmd(point)
	if err != nil {
		return err
	}

	errc := make(chan error, 1)
	go func() {
		defer close(errc)

		// try vanilla unmount first.
		if err := exec.Command("umount", point).Run(); err == nil {
			return
		}

		// retry to unmount with the fallback cmd
		errc <- cmd.Run()
	}()

	select {
	case <-time.After(7 * time.Second):
		return fmt.Errorf("umount timeout")
	case err := <-errc:
		return err
	}
}

// UnmountCmd creates an exec.Cmd that is GOOS-specific
// for unmount a FUSE mount
func UnmountCmd(point string) (*exec.Cmd, error) {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("diskutil", "umount", "force", point), nil
	case "linux":
		return exec.Command("fusermount", "-u", point), nil
	default:
		return nil, fmt.Errorf("unmount: unimplemented")
	}
}

// ForceUnmountManyTimes attempts to forcibly unmount a given mount,
// many times. It does so by calling diskutil or fusermount directly.
// Attempts a given number of times.
func ForceUnmountManyTimes(m Mount, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = ForceUnmount(m)
		if err == nil {
			return err
		}

		<-time.After(time.Millisecond * 500)
	}
	return fmt.Errorf("unmount %s failed after 10 seconds of trying", m.MountPoint())
}

type closer struct {
	M Mount
}

func (c *closer) Close() error {
	log.Warning(" (c *closer) Close(),", c.M.MountPoint())
	return c.M.Unmount()
}

func Closer(m Mount) io.Closer {
	return &closer{m}
}
