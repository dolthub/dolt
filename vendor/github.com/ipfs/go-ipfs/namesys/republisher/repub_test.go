package republisher_test

import (
	"errors"
	"testing"
	"time"

	context "context"
	goprocess "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"

	"github.com/ipfs/go-ipfs/core"
	mock "github.com/ipfs/go-ipfs/core/mock"
	namesys "github.com/ipfs/go-ipfs/namesys"
	. "github.com/ipfs/go-ipfs/namesys/republisher"
	path "github.com/ipfs/go-ipfs/path"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	mocknet "gx/ipfs/QmefgzMbKZYsmHFkLqxgaTBG9ypeEjrdWRD5WXH4j1cWDL/go-libp2p/p2p/net/mock"
)

func TestRepublish(t *testing.T) {
	// set cache life to zero for testing low-period repubs

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create network
	mn := mocknet.New(ctx)

	var nodes []*core.IpfsNode
	for i := 0; i < 10; i++ {
		nd, err := core.NewNode(ctx, &core.BuildCfg{
			Online: true,
			Host:   mock.MockHostOption(mn),
		})
		if err != nil {
			t.Fatal(err)
		}

		nd.Namesys = namesys.NewNameSystem(nd.Routing, nd.Repo.Datastore(), 0)

		nodes = append(nodes, nd)
	}

	mn.LinkAll()

	bsinf := core.BootstrapConfigWithPeers(
		[]pstore.PeerInfo{
			nodes[0].Peerstore.PeerInfo(nodes[0].Identity),
		},
	)

	for _, n := range nodes[1:] {
		if err := n.Bootstrap(bsinf); err != nil {
			t.Fatal(err)
		}
	}

	// have one node publish a record that is valid for 1 second
	publisher := nodes[3]
	p := path.FromString("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn") // does not need to be valid
	rp := namesys.NewRoutingPublisher(publisher.Routing, publisher.Repo.Datastore())
	err := rp.PublishWithEOL(ctx, publisher.PrivateKey, p, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	name := "/ipns/" + publisher.Identity.Pretty()
	if err := verifyResolution(nodes, name, p); err != nil {
		t.Fatal(err)
	}

	// Now wait a second, the records will be invalid and we should fail to resolve
	time.Sleep(time.Second)
	if err := verifyResolutionFails(nodes, name); err != nil {
		t.Fatal(err)
	}

	// The republishers that are contained within the nodes have their timeout set
	// to 12 hours. Instead of trying to tweak those, we're just going to pretend
	// they dont exist and make our own.
	repub := NewRepublisher(publisher.Routing, publisher.Repo.Datastore(), publisher.PrivateKey, publisher.Repo.Keystore())
	repub.Interval = time.Second
	repub.RecordLifetime = time.Second * 5

	proc := goprocess.Go(repub.Run)
	defer proc.Close()

	// now wait a couple seconds for it to fire
	time.Sleep(time.Second * 2)

	// we should be able to resolve them now
	if err := verifyResolution(nodes, name, p); err != nil {
		t.Fatal(err)
	}
}

func verifyResolution(nodes []*core.IpfsNode, key string, exp path.Path) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		val, err := n.Namesys.Resolve(ctx, key)
		if err != nil {
			return err
		}

		if val != exp {
			return errors.New("resolved wrong record")
		}
	}
	return nil
}

func verifyResolutionFails(nodes []*core.IpfsNode, key string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		_, err := n.Namesys.Resolve(ctx, key)
		if err == nil {
			return errors.New("expected resolution to fail")
		}
	}
	return nil
}
