package peerstream_spdystream

import (
	"fmt"
	"os"
	"testing"

	test "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer/test"
)

func TestSpdyStreamTransport(t *testing.T) {
	// test.SubtestAll(t, Transport)

	tests := []test.TransportTest{
		test.SubtestSimpleWrite,
		test.SubtestStress1Conn1Stream1Msg,
		test.SubtestStress1Conn1Stream100Msg,
		test.SubtestStress1Conn100Stream100Msg,
		test.SubtestStress50Conn10Stream50Msg,
		test.SubtestStress1Conn1000Stream10Msg,
		test.SubtestStress1Conn100Stream100Msg10MB,
		// broken:
		// test.SubtestStreamOpenStress,
		// broken:
		// test.SubtestStreamReset,
	}

	for _, f := range tests {
		if testing.Verbose() {
			fmt.Fprintf(os.Stderr, "==== RUN %s\n", test.GetFunctionName(f))
		}
		f(t, Transport)
	}
}
