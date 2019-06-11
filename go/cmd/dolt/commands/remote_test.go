package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetAbsRemoteUrl(t *testing.T) {
	tests := []struct {
		str            string
		cfg            *config.MapConfig
		expectedUrl    string
		expectedScheme string
		expectErr      bool
	}{
		{
			"",
			config.NewMapConfig(map[string]string{}),
			"https://dolthub.com",
			"https",
			false,
		},
		{
			"ts/emp",
			config.NewMapConfig(map[string]string{}),
			"https://dolthub.com/ts/emp",
			"https",
			false,
		},
		{"ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey: "host.dom",
			}),
			"https://host.dom/ts/emp",
			"https",
			false,
		},
		{
			"http://dolthub.com/ts/emp",
			config.NewMapConfig(map[string]string{}),
			"http://dolthub.com/ts/emp",
			"http",
			false,
		},
		{
			"https://test.org:443/ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey: "host.dom",
			}),
			"https://test.org:443/ts/emp",
			"https",
			false,
		},
		{
			"localhost/ts/emp",
			config.NewMapConfig(map[string]string{
				env.RemotesApiHostKey: "host.dom",
			}),
			"https://localhost/ts/emp",
			"https",
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.str, func(t *testing.T) {
			actualScheme, actualUrl, err := getAbsRemoteUrl(nil, test.cfg, test.str)

			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.expectedUrl, actualUrl)
			assert.Equal(t, test.expectedScheme, actualScheme)
		})
	}
}
