package sqlserver

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEncodeDecodeVersion(t *testing.T) {
	versions := []string{
		"0.0.0",
		"1.2.3",
		"128.128.32768",
		"255.255.65535",
	}

	for _, version := range versions {
		t.Run(version, func(t *testing.T) {
			encoded, err := encodeVersion(version)
			require.NoError(t, err)

			decoded := decodeVersion(encoded)
			require.Equal(t, version, decoded)
		})
	}
}

func TestBadVersionEncodeFailure(t *testing.T) {
	versions := []string{
		"256.0.0",
		"0.256.0",
		"0.0.65536",
		"a.0.0",
		"0.40.256c",
		"-1.0.0",
		"2.0",
		"3.5.",
		"..",
	}

	for _, version := range versions {
		t.Run(version, func(t *testing.T) {
			_, err := encodeVersion(version)
			require.Error(t, err)
		})
	}
}
