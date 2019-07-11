package nbs

import "github.com/liquidata-inc/ld/dolt/go/store/d"

func mustUint32(val uint32, err error) uint32 {
	d.PanicIfError(err)
	return val
}

func mustUint64(val uint64, err error) uint64 {
	d.PanicIfError(err)
	return val
}

func mustAddr(h addr, err error) addr {
	d.PanicIfError(err)
	return h
}
