package config

// ConfigProfiles is a map holding configuration transformers
var ConfigProfiles = map[string]func(*Config) error{
	"server": func(c *Config) error {

		// defaultServerFilters has a list of non-routable IPv4 prefixes
		// according to http://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml
		defaultServerFilters := []string{
			"/ip4/10.0.0.0/ipcidr/8",
			"/ip4/100.64.0.0/ipcidr/10",
			"/ip4/169.254.0.0/ipcidr/16",
			"/ip4/172.16.0.0/ipcidr/12",
			"/ip4/192.0.0.0/ipcidr/24",
			"/ip4/192.0.0.0/ipcidr/29",
			"/ip4/192.0.0.8/ipcidr/32",
			"/ip4/192.0.0.170/ipcidr/32",
			"/ip4/192.0.0.171/ipcidr/32",
			"/ip4/192.0.2.0/ipcidr/24",
			"/ip4/192.168.0.0/ipcidr/16",
			"/ip4/198.18.0.0/ipcidr/15",
			"/ip4/198.51.100.0/ipcidr/24",
			"/ip4/203.0.113.0/ipcidr/24",
			"/ip4/240.0.0.0/ipcidr/4",
		}

		c.Swarm.AddrFilters = append(c.Swarm.AddrFilters, defaultServerFilters...)
		c.Discovery.MDNS.Enabled = false
		return nil
	},
	"test": func(c *Config) error {
		c.Addresses.API = "/ip4/127.0.0.1/tcp/0"
		c.Addresses.Gateway = "/ip4/127.0.0.1/tcp/0"

		c.Swarm.DisableNatPortMap = true
		c.Addresses.Swarm = []string{
			"/ip4/127.0.0.1/tcp/0",
		}

		c.Bootstrap = []string{}
		c.Discovery.MDNS.Enabled = false
		return nil
	},
	"badgerds": func(c *Config) error {
		c.Datastore.Spec = map[string]interface{}{
			"type":   "measure",
			"prefix": "badger.datastore",
			"child": map[string]interface{}{
				"type":       "badgerds",
				"path":       "badgerds",
				"syncWrites": true,
			},
		}
		return nil
	},
}
