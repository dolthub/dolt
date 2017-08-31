package integrationtest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	context "context"

	core "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/corerouting"
	"github.com/ipfs/go-ipfs/core/coreunix"
	mock "github.com/ipfs/go-ipfs/core/mock"
	ds2 "github.com/ipfs/go-ipfs/thirdparty/datastore2"
	"github.com/ipfs/go-ipfs/thirdparty/unit"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	testutil "gx/ipfs/QmZJD56ZWLViJAVkvLc7xbbDerHzUMLr2X4fLRYfbxZWDN/go-testutil"
	mocknet "gx/ipfs/QmZyngpQxUGyx1T2bzEcst6YzERkvVwDzBMbsSQF4f1smE/go-libp2p/p2p/net/mock"
)

func TestSupernodeBootstrappedAddCat(t *testing.T) {
	// create 8 supernode-routing bootstrap nodes
	// create 2 supernode-routing clients both bootstrapped to the bootstrap nodes
	// let the bootstrap nodes share a single datastore
	// add a large file on one node then cat the file from the other
	conf := testutil.LatencyConfig{
		NetworkLatency:    0,
		RoutingLatency:    0,
		BlockstoreLatency: 0,
	}
	if err := RunSupernodeBootstrappedAddCat(RandomBytes(100*unit.MB), conf); err != nil {
		t.Fatal(err)
	}
}

func RunSupernodeBootstrappedAddCat(data []byte, conf testutil.LatencyConfig) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers, clients, err := InitializeSupernodeNetwork(ctx, 8, 2, conf)
	if err != nil {
		return err
	}
	for _, n := range append(servers, clients...) {
		defer n.Close()
	}

	adder := clients[0]
	catter := clients[1]

	log.Info("adder is", adder.Identity)
	log.Info("catter is", catter.Identity)

	keyAdded, err := coreunix.Add(adder, bytes.NewReader(data))
	if err != nil {
		return err
	}

	readerCatted, err := coreunix.Cat(ctx, catter, keyAdded)
	if err != nil {
		return err
	}

	// verify
	bufout := new(bytes.Buffer)
	io.Copy(bufout, readerCatted)
	if 0 != bytes.Compare(bufout.Bytes(), data) {
		return errors.New("catted data does not match added data")
	}
	cancel()
	return nil
}

func InitializeSupernodeNetwork(
	ctx context.Context,
	numServers, numClients int,
	conf testutil.LatencyConfig) ([]*core.IpfsNode, []*core.IpfsNode, error) {

	// create network
	mn := mocknet.New(ctx)

	mn.SetLinkDefaults(mocknet.LinkOptions{
		Latency:   conf.NetworkLatency,
		Bandwidth: math.MaxInt32,
	})

	routingDatastore := ds2.ThreadSafeCloserMapDatastore()
	var servers []*core.IpfsNode
	for i := 0; i < numServers; i++ {
		bootstrap, err := core.NewNode(ctx, &core.BuildCfg{
			Online:  true,
			Host:    mock.MockHostOption(mn),
			Routing: corerouting.SupernodeServer(routingDatastore),
		})
		if err != nil {
			return nil, nil, err
		}
		servers = append(servers, bootstrap)
	}

	var bootstrapInfos []pstore.PeerInfo
	for _, n := range servers {
		info := n.Peerstore.PeerInfo(n.PeerHost.ID())
		bootstrapInfos = append(bootstrapInfos, info)
	}

	var clients []*core.IpfsNode
	for i := 0; i < numClients; i++ {
		n, err := core.NewNode(ctx, &core.BuildCfg{
			Online:  true,
			Host:    mock.MockHostOption(mn),
			Routing: corerouting.SupernodeClient(bootstrapInfos...),
		})
		if err != nil {
			return nil, nil, err
		}
		clients = append(clients, n)
	}
	mn.LinkAll()

	bcfg := core.BootstrapConfigWithPeers(bootstrapInfos)
	for _, n := range clients {
		if err := n.Bootstrap(bcfg); err != nil {
			return nil, nil, err
		}
	}
	return servers, clients, nil
}

func TestSupernodePutRecordGetRecord(t *testing.T) {
	// create 8 supernode-routing bootstrap nodes
	// create 2 supernode-routing clients both bootstrapped to the bootstrap nodes
	// let the bootstrap nodes share a single datastore
	// add a large file on one node then cat the file from the other
	conf := testutil.LatencyConfig{
		NetworkLatency:    0,
		RoutingLatency:    0,
		BlockstoreLatency: 0,
	}
	if err := RunSupernodePutRecordGetRecord(conf); err != nil {
		t.Fatal(err)
	}
}

func RunSupernodePutRecordGetRecord(conf testutil.LatencyConfig) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers, clients, err := InitializeSupernodeNetwork(ctx, 2, 2, conf)
	if err != nil {
		return err
	}
	for _, n := range append(servers, clients...) {
		defer n.Close()
	}

	putter := clients[0]
	getter := clients[1]

	k := "key"
	note := []byte("a note from putter")

	if err := putter.Routing.PutValue(ctx, k, note); err != nil {
		return fmt.Errorf("failed to put value: %s", err)
	}

	received, err := getter.Routing.GetValue(ctx, k)
	if err != nil {
		return fmt.Errorf("failed to get value: %s", err)
	}

	if 0 != bytes.Compare(note, received) {
		return errors.New("record doesn't match")
	}
	cancel()
	return nil
}
