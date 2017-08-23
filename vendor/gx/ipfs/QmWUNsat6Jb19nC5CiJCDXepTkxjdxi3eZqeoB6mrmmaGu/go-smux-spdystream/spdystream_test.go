package peerstream_spdystream

import (
	"testing"

	test "gx/ipfs/QmeZBgYBHvxMukGK5ojg28BCNLB9SeXqT7XXg6o7r2GbJy/go-stream-muxer/test"
)

func TestSpdyStreamTransport(t *testing.T) {
	test.SubtestAll(t, Transport)
}
