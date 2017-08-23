package commands

import (
	"os"
	"path"
	"runtime"

	cmds "github.com/ipfs/go-ipfs/commands"
	config "github.com/ipfs/go-ipfs/repo/config"

	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	sysi "gx/ipfs/QmZRjKbHa6DenStpQJFiaPcEwkZqrx7TH6xTf342LDU3qM/go-sysinfo"
)

var sysDiagCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Print system diagnostic information.",
		ShortDescription: `
Prints out information about your computer to aid in easier debugging.
`,
	},
	Run: func(req cmds.Request, res cmds.Response) {
		info := make(map[string]interface{})
		err := runtimeInfo(info)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		err = envVarInfo(info)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		err = diskSpaceInfo(info)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		err = memInfo(info)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		node, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		err = netInfo(node.OnlineMode(), info)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		info["ipfs_version"] = config.CurrentVersionNumber
		info["ipfs_commit"] = config.CurrentCommit
		res.SetOutput(info)
	},
}

func runtimeInfo(out map[string]interface{}) error {
	rt := make(map[string]interface{})
	rt["os"] = runtime.GOOS
	rt["arch"] = runtime.GOARCH
	rt["compiler"] = runtime.Compiler
	rt["version"] = runtime.Version()
	rt["numcpu"] = runtime.NumCPU()
	rt["gomaxprocs"] = runtime.GOMAXPROCS(0)
	rt["numgoroutines"] = runtime.NumGoroutine()

	out["runtime"] = rt
	return nil
}

func envVarInfo(out map[string]interface{}) error {
	ev := make(map[string]interface{})
	ev["GOPATH"] = os.Getenv("GOPATH")
	ev["IPFS_PATH"] = os.Getenv("IPFS_PATH")

	out["environment"] = ev
	return nil
}

func ipfsPath() string {
	p := os.Getenv("IPFS_PATH")
	if p == "" {
		p = path.Join(os.Getenv("HOME"), ".ipfs")
	}
	return p
}

func diskSpaceInfo(out map[string]interface{}) error {
	di := make(map[string]interface{})
	dinfo, err := sysi.DiskUsage(ipfsPath())
	if err != nil {
		return err
	}

	di["fstype"] = dinfo.FsType
	di["total_space"] = dinfo.Total
	di["free_space"] = dinfo.Free

	out["diskinfo"] = di
	return nil
}

func memInfo(out map[string]interface{}) error {
	m := make(map[string]interface{})

	meminf, err := sysi.MemoryInfo()
	if err != nil {
		return err
	}

	m["swap"] = meminf.Swap
	m["virt"] = meminf.Used
	out["memory"] = m
	return nil
}

func netInfo(online bool, out map[string]interface{}) error {
	n := make(map[string]interface{})
	addrs, err := manet.InterfaceMultiaddrs()
	if err != nil {
		return err
	}

	var straddrs []string
	for _, a := range addrs {
		straddrs = append(straddrs, a.String())
	}

	n["interface_addresses"] = straddrs
	n["online"] = online
	out["net"] = n
	return nil
}
