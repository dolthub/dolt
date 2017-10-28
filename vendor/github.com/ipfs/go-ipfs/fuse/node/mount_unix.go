// +build linux darwin freebsd netbsd openbsd
// +build !nofuse

package node

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	core "github.com/ipfs/go-ipfs/core"
	ipns "github.com/ipfs/go-ipfs/fuse/ipns"
	mount "github.com/ipfs/go-ipfs/fuse/mount"
	rofs "github.com/ipfs/go-ipfs/fuse/readonly"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var log = logging.Logger("node")

// fuseNoDirectory used to check the returning fuse error
const fuseNoDirectory = "fusermount: failed to access mountpoint"

// fuseExitStatus1 used to check the returning fuse error
const fuseExitStatus1 = "fusermount: exit status 1"

// platformFuseChecks can get overridden by arch-specific files
// to run fuse checks (like checking the OSXFUSE version)
var platformFuseChecks = func(*core.IpfsNode) error {
	return nil
}

func Mount(node *core.IpfsNode, fsdir, nsdir string) error {
	// check if we already have live mounts.
	// if the user said "Mount", then there must be something wrong.
	// so, close them and try again.
	if node.Mounts.Ipfs != nil && node.Mounts.Ipfs.IsActive() {
		node.Mounts.Ipfs.Unmount()
	}
	if node.Mounts.Ipns != nil && node.Mounts.Ipns.IsActive() {
		node.Mounts.Ipns.Unmount()
	}

	if err := platformFuseChecks(node); err != nil {
		return err
	}

	return doMount(node, fsdir, nsdir)
}

func doMount(node *core.IpfsNode, fsdir, nsdir string) error {
	fmtFuseErr := func(err error, mountpoint string) error {
		s := err.Error()
		if strings.Contains(s, fuseNoDirectory) {
			s = strings.Replace(s, `fusermount: "fusermount:`, "", -1)
			s = strings.Replace(s, `\n", exit status 1`, "", -1)
			return errors.New(s)
		}
		if s == fuseExitStatus1 {
			s = fmt.Sprintf("fuse failed to access mountpoint %s", mountpoint)
			return errors.New(s)
		}
		return err
	}

	// this sync stuff is so that both can be mounted simultaneously.
	var fsmount, nsmount mount.Mount
	var err1, err2 error

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		fsmount, err1 = rofs.Mount(node, fsdir)
	}()

	if node.OnlineMode() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nsmount, err2 = ipns.Mount(node, nsdir, fsdir)
		}()
	}

	wg.Wait()

	if err1 != nil {
		log.Errorf("error mounting: %s", err1)
	}

	if err2 != nil {
		log.Errorf("error mounting: %s", err2)
	}

	if err1 != nil || err2 != nil {
		if fsmount != nil {
			fsmount.Unmount()
		}
		if nsmount != nil {
			nsmount.Unmount()
		}

		if err1 != nil {
			return fmtFuseErr(err1, fsdir)
		}
		return fmtFuseErr(err2, nsdir)
	}

	// setup node state, so that it can be cancelled
	node.Mounts.Ipfs = fsmount
	node.Mounts.Ipns = nsmount
	return nil
}
