package mocknet

import (
	"context"
	"fmt"
	"sort"
	"sync"

	host "gx/ipfs/Qmc1XhrFEiSeBNn3mpfg6gEuYCt5im2gYmNVmncsvmpeAk/go-libp2p-host"
	bhost "gx/ipfs/QmefgzMbKZYsmHFkLqxgaTBG9ypeEjrdWRD5WXH4j1cWDL/go-libp2p/p2p/host/basic"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	p2putil "gx/ipfs/QmQGX417WoxKxDJeHqouMEmmH4G1RCENNSzkZYHrXy3Xb3/go-libp2p-netutil"
	"gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
	goprocessctx "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess/context"
	testutil "gx/ipfs/QmWRCn8vruNAzHx8i6SAXinuheRitKEGu8c7m26stKvsYx/go-testutil"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ic "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

// mocknet implements mocknet.Mocknet
type mocknet struct {
	nets  map[peer.ID]*peernet
	hosts map[peer.ID]*bhost.BasicHost

	// links make it possible to connect two peers.
	// think of links as the physical medium.
	// usually only one, but there could be multiple
	// **links are shared between peers**
	links map[peer.ID]map[peer.ID]map[*link]struct{}

	linkDefaults LinkOptions

	proc goprocess.Process // for Context closing
	ctx  context.Context
	sync.Mutex
}

func New(ctx context.Context) Mocknet {
	return &mocknet{
		nets:  map[peer.ID]*peernet{},
		hosts: map[peer.ID]*bhost.BasicHost{},
		links: map[peer.ID]map[peer.ID]map[*link]struct{}{},
		proc:  goprocessctx.WithContext(ctx),
		ctx:   ctx,
	}
}

func (mn *mocknet) GenPeer() (host.Host, error) {
	sk, err := p2putil.RandTestBogusPrivateKey()
	if err != nil {
		return nil, err
	}

	a := testutil.RandLocalTCPAddress()

	h, err := mn.AddPeer(sk, a)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (mn *mocknet) AddPeer(k ic.PrivKey, a ma.Multiaddr) (host.Host, error) {
	p, err := peer.IDFromPublicKey(k.GetPublic())
	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstore()
	ps.AddAddr(p, a, pstore.PermanentAddrTTL)
	ps.AddPrivKey(p, k)
	ps.AddPubKey(p, k.GetPublic())

	return mn.AddPeerWithPeerstore(p, ps)
}

func (mn *mocknet) AddPeerWithPeerstore(p peer.ID, ps pstore.Peerstore) (host.Host, error) {
	n, err := newPeernet(mn.ctx, mn, p, ps)
	if err != nil {
		return nil, err
	}

	opts := &bhost.HostOpts{
		NegotiationTimeout: -1,
	}

	h, err := bhost.NewHost(mn.ctx, n, opts)
	if err != nil {
		return nil, err
	}

	mn.proc.AddChild(n.proc)

	mn.Lock()
	mn.nets[n.peer] = n
	mn.hosts[n.peer] = h
	mn.Unlock()
	return h, nil
}

func (mn *mocknet) Peers() []peer.ID {
	mn.Lock()
	defer mn.Unlock()

	cp := make([]peer.ID, 0, len(mn.nets))
	for _, n := range mn.nets {
		cp = append(cp, n.peer)
	}
	sort.Sort(peer.IDSlice(cp))
	return cp
}

func (mn *mocknet) Host(pid peer.ID) host.Host {
	mn.Lock()
	host := mn.hosts[pid]
	mn.Unlock()
	return host
}

func (mn *mocknet) Net(pid peer.ID) inet.Network {
	mn.Lock()
	n := mn.nets[pid]
	mn.Unlock()
	return n
}

func (mn *mocknet) Hosts() []host.Host {
	mn.Lock()
	defer mn.Unlock()

	cp := make([]host.Host, 0, len(mn.hosts))
	for _, h := range mn.hosts {
		cp = append(cp, h)
	}

	sort.Sort(hostSlice(cp))
	return cp
}

func (mn *mocknet) Nets() []inet.Network {
	mn.Lock()
	defer mn.Unlock()

	cp := make([]inet.Network, 0, len(mn.nets))
	for _, n := range mn.nets {
		cp = append(cp, n)
	}
	sort.Sort(netSlice(cp))
	return cp
}

// Links returns a copy of the internal link state map.
// (wow, much map. so data structure. how compose. ahhh pointer)
func (mn *mocknet) Links() LinkMap {
	mn.Lock()
	defer mn.Unlock()

	links := map[string]map[string]map[Link]struct{}{}
	for p1, lm := range mn.links {
		sp1 := string(p1)
		links[sp1] = map[string]map[Link]struct{}{}
		for p2, ls := range lm {
			sp2 := string(p2)
			links[sp1][sp2] = map[Link]struct{}{}
			for l := range ls {
				links[sp1][sp2][l] = struct{}{}
			}
		}
	}
	return links
}

func (mn *mocknet) LinkAll() error {
	nets := mn.Nets()
	for _, n1 := range nets {
		for _, n2 := range nets {
			if _, err := mn.LinkNets(n1, n2); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mn *mocknet) LinkPeers(p1, p2 peer.ID) (Link, error) {
	mn.Lock()
	n1 := mn.nets[p1]
	n2 := mn.nets[p2]
	mn.Unlock()

	if n1 == nil {
		return nil, fmt.Errorf("network for p1 not in mocknet")
	}

	if n2 == nil {
		return nil, fmt.Errorf("network for p2 not in mocknet")
	}

	return mn.LinkNets(n1, n2)
}

func (mn *mocknet) validate(n inet.Network) (*peernet, error) {
	// WARNING: assumes locks acquired

	nr, ok := n.(*peernet)
	if !ok {
		return nil, fmt.Errorf("Network not supported (use mock package nets only)")
	}

	if _, found := mn.nets[nr.peer]; !found {
		return nil, fmt.Errorf("Network not on mocknet. is it from another mocknet?")
	}

	return nr, nil
}

func (mn *mocknet) LinkNets(n1, n2 inet.Network) (Link, error) {
	mn.Lock()
	n1r, err1 := mn.validate(n1)
	n2r, err2 := mn.validate(n2)
	ld := mn.linkDefaults
	mn.Unlock()

	if err1 != nil {
		return nil, err1
	}
	if err2 != nil {
		return nil, err2
	}

	l := newLink(mn, ld)
	l.nets = append(l.nets, n1r, n2r)
	mn.addLink(l)
	return l, nil
}

func (mn *mocknet) Unlink(l2 Link) error {

	l, ok := l2.(*link)
	if !ok {
		return fmt.Errorf("only links from mocknet are supported")
	}

	mn.removeLink(l)
	return nil
}

func (mn *mocknet) UnlinkPeers(p1, p2 peer.ID) error {
	ls := mn.LinksBetweenPeers(p1, p2)
	if ls == nil {
		return fmt.Errorf("no link between p1 and p2")
	}

	for _, l := range ls {
		if err := mn.Unlink(l); err != nil {
			return err
		}
	}
	return nil
}

func (mn *mocknet) UnlinkNets(n1, n2 inet.Network) error {
	return mn.UnlinkPeers(n1.LocalPeer(), n2.LocalPeer())
}

// get from the links map. and lazily contruct.
func (mn *mocknet) linksMapGet(p1, p2 peer.ID) map[*link]struct{} {

	l1, found := mn.links[p1]
	if !found {
		mn.links[p1] = map[peer.ID]map[*link]struct{}{}
		l1 = mn.links[p1] // so we make sure it's there.
	}

	l2, found := l1[p2]
	if !found {
		m := map[*link]struct{}{}
		l1[p2] = m
		l2 = l1[p2]
	}

	return l2
}

func (mn *mocknet) addLink(l *link) {
	mn.Lock()
	defer mn.Unlock()

	n1, n2 := l.nets[0], l.nets[1]
	mn.linksMapGet(n1.peer, n2.peer)[l] = struct{}{}
	mn.linksMapGet(n2.peer, n1.peer)[l] = struct{}{}
}

func (mn *mocknet) removeLink(l *link) {
	mn.Lock()
	defer mn.Unlock()

	n1, n2 := l.nets[0], l.nets[1]
	delete(mn.linksMapGet(n1.peer, n2.peer), l)
	delete(mn.linksMapGet(n2.peer, n1.peer), l)
}

func (mn *mocknet) ConnectAllButSelf() error {
	nets := mn.Nets()
	for _, n1 := range nets {
		for _, n2 := range nets {
			if n1 == n2 {
				continue
			}

			if _, err := mn.ConnectNets(n1, n2); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mn *mocknet) ConnectPeers(a, b peer.ID) (inet.Conn, error) {
	return mn.Net(a).DialPeer(mn.ctx, b)
}

func (mn *mocknet) ConnectNets(a, b inet.Network) (inet.Conn, error) {
	return a.DialPeer(mn.ctx, b.LocalPeer())
}

func (mn *mocknet) DisconnectPeers(p1, p2 peer.ID) error {
	return mn.Net(p1).ClosePeer(p2)
}

func (mn *mocknet) DisconnectNets(n1, n2 inet.Network) error {
	return n1.ClosePeer(n2.LocalPeer())
}

func (mn *mocknet) LinksBetweenPeers(p1, p2 peer.ID) []Link {
	mn.Lock()
	defer mn.Unlock()

	ls2 := mn.linksMapGet(p1, p2)
	cp := make([]Link, 0, len(ls2))
	for l := range ls2 {
		cp = append(cp, l)
	}
	return cp
}

func (mn *mocknet) LinksBetweenNets(n1, n2 inet.Network) []Link {
	return mn.LinksBetweenPeers(n1.LocalPeer(), n2.LocalPeer())
}

func (mn *mocknet) SetLinkDefaults(o LinkOptions) {
	mn.Lock()
	mn.linkDefaults = o
	mn.Unlock()
}

func (mn *mocknet) LinkDefaults() LinkOptions {
	mn.Lock()
	defer mn.Unlock()
	return mn.linkDefaults
}

// netSlice for sorting by peer
type netSlice []inet.Network

func (es netSlice) Len() int           { return len(es) }
func (es netSlice) Swap(i, j int)      { es[i], es[j] = es[j], es[i] }
func (es netSlice) Less(i, j int) bool { return string(es[i].LocalPeer()) < string(es[j].LocalPeer()) }

// hostSlice for sorting by peer
type hostSlice []host.Host

func (es hostSlice) Len() int           { return len(es) }
func (es hostSlice) Swap(i, j int)      { es[i], es[j] = es[j], es[i] }
func (es hostSlice) Less(i, j int) bool { return string(es[i].ID()) < string(es[j].ID()) }
