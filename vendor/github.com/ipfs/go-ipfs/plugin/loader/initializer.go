package loader

import (
	"github.com/ipfs/go-ipfs/core/coredag"
	"github.com/ipfs/go-ipfs/plugin"

	format "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

func initialize(plugins []plugin.Plugin) error {
	for _, p := range plugins {
		err := p.Init()
		if err != nil {
			return err
		}
	}

	return nil
}

func run(plugins []plugin.Plugin) error {
	for _, pl := range plugins {
		err := runIPLDPlugin(pl)
		if err != nil {
			return err
		}
	}
	return nil
}

func runIPLDPlugin(pl plugin.Plugin) error {
	ipldpl, ok := pl.(plugin.PluginIPLD)
	if !ok {
		return nil
	}

	err := ipldpl.RegisterBlockDecoders(format.DefaultBlockDecoder)
	if err != nil {
		return err
	}

	return ipldpl.RegisterInputEncParsers(coredag.DefaultInputEncParsers)
}
