package plugin

// Plugin is base interface for all kinds of go-ipfs plugins
// It will be included in interfaces of different Plugins
type Plugin interface {
	// Name should return uniqe name of the plugin
	Name() string
	// Version returns current version of the plugin
	Version() string
	// Init is called once when the Plugin is being loaded
	Init() error
}
