package corehttp

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	commands "github.com/ipfs/go-ipfs/commands"
	cmdsHttp "github.com/ipfs/go-ipfs/commands/http"
	core "github.com/ipfs/go-ipfs/core"
	corecommands "github.com/ipfs/go-ipfs/core/commands"
	config "github.com/ipfs/go-ipfs/repo/config"
)

const originEnvKey = "API_ORIGIN"
const originEnvKeyDeprecate = `You are using the ` + originEnvKey + `ENV Variable.
This functionality is deprecated, and will be removed in future versions.
Instead, try either adding headers to the config, or passing them via
cli arguments:

	ipfs config API.HTTPHeaders 'Access-Control-Allow-Origin' '*'
	ipfs daemon

or

	ipfs daemon --api-http-header 'Access-Control-Allow-Origin: *'
`

var defaultLocalhostOrigins = []string{
	"http://127.0.0.1:<port>",
	"https://127.0.0.1:<port>",
	"http://localhost:<port>",
	"https://localhost:<port>",
}

func addCORSFromEnv(c *cmdsHttp.ServerConfig) {
	origin := os.Getenv(originEnvKey)
	if origin != "" {
		log.Warning(originEnvKeyDeprecate)
		if len(c.AllowedOrigins()) == 0 {
			c.SetAllowedOrigins([]string{origin}...)
		}
		c.AppendAllowedOrigins(origin)
	}
}

func addHeadersFromConfig(c *cmdsHttp.ServerConfig, nc *config.Config) {
	log.Info("Using API.HTTPHeaders:", nc.API.HTTPHeaders)

	if acao := nc.API.HTTPHeaders[cmdsHttp.ACAOrigin]; acao != nil {
		c.SetAllowedOrigins(acao...)
	}
	if acam := nc.API.HTTPHeaders[cmdsHttp.ACAMethods]; acam != nil {
		c.SetAllowedMethods(acam...)
	}
	if acac := nc.API.HTTPHeaders[cmdsHttp.ACACredentials]; acac != nil {
		for _, v := range acac {
			c.SetAllowCredentials(strings.ToLower(v) == "true")
		}
	}

	c.Headers = nc.API.HTTPHeaders
}

func addCORSDefaults(c *cmdsHttp.ServerConfig) {
	// by default use localhost origins
	if len(c.AllowedOrigins()) == 0 {
		c.SetAllowedOrigins(defaultLocalhostOrigins...)
	}

	// by default, use GET, PUT, POST
	if len(c.AllowedMethods()) == 0 {
		c.SetAllowedMethods("GET", "POST", "PUT")
	}
}

func patchCORSVars(c *cmdsHttp.ServerConfig, addr net.Addr) {

	// we have to grab the port from an addr, which may be an ip6 addr.
	// TODO: this should take multiaddrs and derive port from there.
	port := ""
	if tcpaddr, ok := addr.(*net.TCPAddr); ok {
		port = strconv.Itoa(tcpaddr.Port)
	} else if udpaddr, ok := addr.(*net.UDPAddr); ok {
		port = strconv.Itoa(udpaddr.Port)
	}

	// we're listening on tcp/udp with ports. ("udp!?" you say? yeah... it happens...)
	origins := c.AllowedOrigins()
	for i, o := range origins {
		// TODO: allow replacing <host>. tricky, ip4 and ip6 and hostnames...
		if port != "" {
			o = strings.Replace(o, "<port>", port, -1)
		}
		origins[i] = o
	}
	c.SetAllowedOrigins(origins...)
}

func commandsOption(cctx commands.Context, command *commands.Command) ServeOption {
	return func(n *core.IpfsNode, l net.Listener, mux *http.ServeMux) (*http.ServeMux, error) {

		cfg := cmdsHttp.NewServerConfig()
		cfg.SetAllowedMethods("GET", "POST", "PUT")
		rcfg, err := n.Repo.Config()
		if err != nil {
			return nil, err
		}

		addHeadersFromConfig(cfg, rcfg)
		addCORSFromEnv(cfg)
		addCORSDefaults(cfg)
		patchCORSVars(cfg, l.Addr())

		cmdHandler := cmdsHttp.NewHandler(cctx, command, cfg)
		mux.Handle(cmdsHttp.ApiPath+"/", cmdHandler)
		return mux, nil
	}
}

func CommandsOption(cctx commands.Context) ServeOption {
	return commandsOption(cctx, corecommands.Root)
}

func CommandsROOption(cctx commands.Context) ServeOption {
	return commandsOption(cctx, corecommands.RootRO)
}
