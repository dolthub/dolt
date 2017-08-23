package fs_test

import (
	"testing"

	"gx/ipfs/QmTq8ag5pgTCqtGDtmpm1F5TPE2i1H8bcU6295WFKTc5ie/sys/unix"
	"gx/ipfs/QmaFNtBAXX4nVMQWbUqNysXyhevUj1k4B1y5uS45LC7Vw9/fuse/fs/fstestutil"
)

type exchangeData struct {
	fstestutil.File
	// this struct cannot be zero size or multiple instances may look identical
	_ int
}

func TestExchangeDataNotSupported(t *testing.T) {
	t.Parallel()
	mnt, err := fstestutil.MountedT(t, fstestutil.SimpleFS{&fstestutil.ChildMap{
		"one": &exchangeData{},
		"two": &exchangeData{},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mnt.Close()

	if err := unix.Exchangedata(mnt.Dir+"/one", mnt.Dir+"/two", 0); err != unix.ENOTSUP {
		t.Fatalf("expected ENOTSUP from exchangedata: %v", err)
	}
}
