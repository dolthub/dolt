package config

// Gateway contains options for the HTTP gateway server.
type Gateway struct {
	HTTPHeaders  map[string][]string // HTTP headers to return with the gateway
	RootRedirect string
	Writable     bool
	PathPrefixes []string
}
