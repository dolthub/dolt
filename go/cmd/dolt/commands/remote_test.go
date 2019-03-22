package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"testing"
)

func TestGetAbsRemoteUrl(t *testing.T) {
	tests := []struct {
		str         string
		cfg         *config.MapConfig
		expectedUrl string
		expectErr   bool
	}{
		{
			"",
			config.NewMapConfig(map[string]string{}),
			"dolthub.com:443",
			false,
		},
		{
			"ts/emp",
			config.NewMapConfig(map[string]string{}),
			"dolthub.com:443/ts/emp",
			false,
		},
		{"ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey: "host.dom",
			}),
			"host.dom:443/ts/emp",
			false,
		},
		{
			"ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostPortKey: "8080",
			}),
			"dolthub.com:8080/ts/emp",
			false,
		},
		{"ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey:     "host.dom",
				env.RemotesApiHostPortKey: "8080",
			}),
			"host.dom:8080/ts/emp",
			false,
		},
		{
			"test.org/ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey:     "host.dom",
				env.RemotesApiHostPortKey: "8080",
			}),
			"test.org/ts/emp",
			false,
		},
		{
			"localhost/ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey:     "host.dom",
				env.RemotesApiHostPortKey: "8080",
			}),
			"localhost/ts/emp",
			false,
		},
	}

	for _, test := range tests {
		actualUrl, err := getAbsRemoteUrl(test.cfg, test.str)

		if (err != nil) != test.expectErr {
			t.Error("input:", test.str, "config:", test.cfg, "got error:", err != nil, "expected error:", test.expectErr, "result:", actualUrl, "err:", err)
		} else if actualUrl != test.expectedUrl {
			t.Error(actualUrl, "!=", test.expectedUrl)
		}
	}
}
