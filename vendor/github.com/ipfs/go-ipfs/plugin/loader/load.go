package loader

import (
	"fmt"
	"os"

	"github.com/ipfs/go-ipfs/plugin"

	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var log = logging.Logger("plugin/loader")

var loadPluginsFunc = func(string) ([]plugin.Plugin, error) {
	return nil, nil
}

// LoadPlugins loads and initializes plugins.
func LoadPlugins(pluginDir string) ([]plugin.Plugin, error) {
	plMap := make(map[string]plugin.Plugin)
	for _, v := range preloadPlugins {
		plMap[v.Name()] = v
	}

	newPls, err := loadDynamicPlugins(pluginDir)
	if err != nil {
		return nil, err
	}

	for _, pl := range newPls {
		if ppl, ok := plMap[pl.Name()]; ok {
			// plugin is already preloaded
			return nil, fmt.Errorf(
				"plugin: %s, is duplicated in version: %s, "+
					"while trying to load dynamically: %s",
				ppl.Name(), ppl.Version(), pl.Version())
		}
		plMap[pl.Name()] = pl
	}

	pls := make([]plugin.Plugin, 0, len(plMap))
	for _, v := range plMap {
		pls = append(pls, v)
	}

	err = initialize(pls)
	if err != nil {
		return nil, err
	}

	err = run(pls)
	return nil, err
}

func loadDynamicPlugins(pluginDir string) ([]plugin.Plugin, error) {
	_, err := os.Stat(pluginDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return loadPluginsFunc(pluginDir)
}
