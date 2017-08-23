package sm_yamux

import (
	"testing"

	test "gx/ipfs/QmeZBgYBHvxMukGK5ojg28BCNLB9SeXqT7XXg6o7r2GbJy/go-stream-muxer/test"
)

func TestYamuxTransport(t *testing.T) {
	test.SubtestAll(t, DefaultTransport)
}
