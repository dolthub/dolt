package integrationtest

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	context "context"
	"github.com/ipfs/go-ipfs/core"
	coreunix "github.com/ipfs/go-ipfs/core/coreunix"
	mock "github.com/ipfs/go-ipfs/core/mock"
	"github.com/ipfs/go-ipfs/thirdparty/unit"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	testutil "gx/ipfs/QmWRCn8vruNAzHx8i6SAXinuheRitKEGu8c7m26stKvsYx/go-testutil"
	mocknet "gx/ipfs/QmefgzMbKZYsmHFkLqxgaTBG9ypeEjrdWRD5WXH4j1cWDL/go-libp2p/p2p/net/mock"
)

func BenchmarkCat1MB(b *testing.B) { benchmarkVarCat(b, unit.MB*1) }
func BenchmarkCat2MB(b *testing.B) { benchmarkVarCat(b, unit.MB*2) }
func BenchmarkCat4MB(b *testing.B) { benchmarkVarCat(b, unit.MB*4) }

func benchmarkVarCat(b *testing.B, size int64) {
	data := RandomBytes(size)
	b.SetBytes(size)
	for n := 0; n < b.N; n++ {
		err := benchCat(b, data, instant)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchCat(b *testing.B, data []byte, conf testutil.LatencyConfig) error {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create network
	mn := mocknet.New(ctx)
	mn.SetLinkDefaults(mocknet.LinkOptions{
		Latency: conf.NetworkLatency,
		// TODO add to conf. This is tricky because we want 0 values to be functional.
		Bandwidth: math.MaxInt32,
	})

	adder, err := core.NewNode(ctx, &core.BuildCfg{
		Online: true,
		Host:   mock.MockHostOption(mn),
	})
	if err != nil {
		return err
	}
	defer adder.Close()

	catter, err := core.NewNode(ctx, &core.BuildCfg{
		Online: true,
		Host:   mock.MockHostOption(mn),
	})
	if err != nil {
		return err
	}
	defer catter.Close()

	err = mn.LinkAll()
	if err != nil {
		return err
	}

	bs1 := []pstore.PeerInfo{adder.Peerstore.PeerInfo(adder.Identity)}
	bs2 := []pstore.PeerInfo{catter.Peerstore.PeerInfo(catter.Identity)}

	if err := catter.Bootstrap(core.BootstrapConfigWithPeers(bs1)); err != nil {
		return err
	}
	if err := adder.Bootstrap(core.BootstrapConfigWithPeers(bs2)); err != nil {
		return err
	}

	added, err := coreunix.Add(adder, bytes.NewReader(data))
	if err != nil {
		return err
	}

	b.StartTimer()
	readerCatted, err := coreunix.Cat(ctx, catter, added)
	if err != nil {
		return err
	}

	// verify
	bufout := new(bytes.Buffer)
	io.Copy(bufout, readerCatted)
	if 0 != bytes.Compare(bufout.Bytes(), data) {
		return errors.New("catted data does not match added data")
	}
	return nil
}
