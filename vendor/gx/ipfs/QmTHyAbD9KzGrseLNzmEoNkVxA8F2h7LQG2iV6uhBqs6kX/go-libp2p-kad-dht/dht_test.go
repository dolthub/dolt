package dht

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	pb "gx/ipfs/QmTHyAbD9KzGrseLNzmEoNkVxA8F2h7LQG2iV6uhBqs6kX/go-libp2p-kad-dht/pb"

	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	ci "gx/ipfs/QmSXpwmggGd8fe7e9abqRuST7SqaZjrz3Cg5avMJbdmKGe/go-testutil/ci"
	travisci "gx/ipfs/QmSXpwmggGd8fe7e9abqRuST7SqaZjrz3Cg5avMJbdmKGe/go-testutil/ci/travis"
	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dssync "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/sync"
	netutil "gx/ipfs/QmViDDJGzv2TKrheoxckReECc72iRgaYsobG2HYUGWuPVF/go-libp2p-netutil"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	bhost "gx/ipfs/QmapADMpK4e5kFGBxC2aHreaDqKP9vmMng5f91MA14Ces9/go-libp2p/p2p/host/basic"
	kb "gx/ipfs/QmbiCMdwmmhif5axuGSHzYbPFGeKjLAuMY6JrGpVteHFsy/go-libp2p-kbucket"
	record "gx/ipfs/QmbxkgUceEcuSZ4ZdBA3x74VUDSSYjHYmmeEqkjxbtZ6Jg/go-libp2p-record"
)

var testCaseValues = map[string][]byte{}
var testCaseCids []*cid.Cid

func init() {
	testCaseValues["hello"] = []byte("world")
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("%d -- key", i)
		v := fmt.Sprintf("%d -- value", i)
		testCaseValues[k] = []byte(v)

		mhv := u.Hash([]byte(v))
		testCaseCids = append(testCaseCids, cid.NewCidV0(mhv))
	}
}

func setupDHT(ctx context.Context, t *testing.T, client bool) *IpfsDHT {
	h := bhost.New(netutil.GenSwarmNetwork(t, ctx))

	dss := dssync.MutexWrap(ds.NewMapDatastore())
	var d *IpfsDHT
	if client {
		d = NewDHTClient(ctx, h, dss)
	} else {
		d = NewDHT(ctx, h, dss)
	}

	d.Validator["v"] = &record.ValidChecker{
		Func: func(string, []byte) error {
			return nil
		},
		Sign: false,
	}
	d.Selector["v"] = func(_ string, bs [][]byte) (int, error) { return 0, nil }
	return d
}

func setupDHTS(ctx context.Context, n int, t *testing.T) ([]ma.Multiaddr, []peer.ID, []*IpfsDHT) {
	addrs := make([]ma.Multiaddr, n)
	dhts := make([]*IpfsDHT, n)
	peers := make([]peer.ID, n)

	sanityAddrsMap := make(map[string]struct{})
	sanityPeersMap := make(map[string]struct{})

	for i := 0; i < n; i++ {
		dhts[i] = setupDHT(ctx, t, false)
		peers[i] = dhts[i].self
		addrs[i] = dhts[i].peerstore.Addrs(dhts[i].self)[0]

		if _, lol := sanityAddrsMap[addrs[i].String()]; lol {
			t.Fatal("While setting up DHTs address got duplicated.")
		} else {
			sanityAddrsMap[addrs[i].String()] = struct{}{}
		}
		if _, lol := sanityPeersMap[peers[i].String()]; lol {
			t.Fatal("While setting up DHTs peerid got duplicated.")
		} else {
			sanityPeersMap[peers[i].String()] = struct{}{}
		}
	}

	return addrs, peers, dhts
}

func connectNoSync(t *testing.T, ctx context.Context, a, b *IpfsDHT) {
	idB := b.self
	addrB := b.peerstore.Addrs(idB)
	if len(addrB) == 0 {
		t.Fatal("peers setup incorrectly: no local address")
	}

	a.peerstore.AddAddrs(idB, addrB, pstore.TempAddrTTL)
	pi := pstore.PeerInfo{ID: idB}
	if err := a.host.Connect(ctx, pi); err != nil {
		t.Fatal(err)
	}
}

func connect(t *testing.T, ctx context.Context, a, b *IpfsDHT) {
	connectNoSync(t, ctx, a, b)

	// loop until connection notification has been received.
	// under high load, this may not happen as immediately as we would like.
	for a.routingTable.Find(b.self) == "" {
		time.Sleep(time.Millisecond * 5)
	}

	for b.routingTable.Find(a.self) == "" {
		time.Sleep(time.Millisecond * 5)
	}
}

func bootstrap(t *testing.T, ctx context.Context, dhts []*IpfsDHT) {

	ctx, cancel := context.WithCancel(ctx)
	log.Debugf("Bootstrapping DHTs...")

	// tried async. sequential fares much better. compare:
	// 100 async https://gist.github.com/jbenet/56d12f0578d5f34810b2
	// 100 sync https://gist.github.com/jbenet/6c59e7c15426e48aaedd
	// probably because results compound

	var cfg BootstrapConfig
	cfg = DefaultBootstrapConfig
	cfg.Queries = 3

	start := rand.Intn(len(dhts)) // randomize to decrease bias.
	for i := range dhts {
		dht := dhts[(start+i)%len(dhts)]
		dht.runBootstrap(ctx, cfg)
	}
	cancel()
}

func TestValueGetSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtA := setupDHT(ctx, t, false)
	dhtB := setupDHT(ctx, t, false)

	defer dhtA.Close()
	defer dhtB.Close()
	defer dhtA.host.Close()
	defer dhtB.host.Close()

	vf := &record.ValidChecker{
		Func: func(string, []byte) error { return nil },
		Sign: false,
	}
	nulsel := func(_ string, bs [][]byte) (int, error) { return 0, nil }

	dhtA.Validator["v"] = vf
	dhtB.Validator["v"] = vf
	dhtA.Selector["v"] = nulsel
	dhtB.Selector["v"] = nulsel

	connect(t, ctx, dhtA, dhtB)

	log.Error("adding value on: ", dhtA.self)
	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := dhtA.PutValue(ctxT, "/v/hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	/*
		ctxT, _ = context.WithTimeout(ctx, time.Second*2)
		val, err := dhtA.GetValue(ctxT, "/v/hello")
		if err != nil {
			t.Fatal(err)
		}

		if string(val) != "world" {
			t.Fatalf("Expected 'world' got '%s'", string(val))
		}
	*/

	log.Error("requesting value on dht: ", dhtB.self)
	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	valb, err := dhtB.GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(valb) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(valb))
	}
}

func TestProvides(t *testing.T) {
	// t.Skip("skipping test to debug another")
	ctx := context.Background()

	_, _, dhts := setupDHTS(ctx, 4, t)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	for _, k := range testCaseCids {
		log.Debugf("announcing provider for %s", k)
		if err := dhts[3].Provide(ctx, k, true); err != nil {
			t.Fatal(err)
		}
	}

	// what is this timeout for? was 60ms before.
	time.Sleep(time.Millisecond * 6)

	n := 0
	for _, c := range testCaseCids {
		n = (n + 1) % 3

		log.Debugf("getting providers for %s from %d", c, n)
		ctxT, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		provchan := dhts[n].FindProvidersAsync(ctxT, c, 1)

		select {
		case prov := <-provchan:
			if prov.ID == "" {
				t.Fatal("Got back nil provider")
			}
			if prov.ID != dhts[3].self {
				t.Fatal("Got back wrong provider")
			}
		case <-ctxT.Done():
			t.Fatal("Did not get a provider back.")
		}
	}
}

func TestLocalProvides(t *testing.T) {
	// t.Skip("skipping test to debug another")
	ctx := context.Background()

	_, _, dhts := setupDHTS(ctx, 4, t)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	for _, k := range testCaseCids {
		log.Debugf("announcing provider for %s", k)
		if err := dhts[3].Provide(ctx, k, false); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Millisecond * 10)

	for _, c := range testCaseCids {
		for i := 0; i < 3; i++ {
			provs := dhts[i].providers.GetProviders(ctx, c)
			if len(provs) > 0 {
				t.Fatal("shouldnt know this")
			}
		}
	}
}

// if minPeers or avgPeers is 0, dont test for it.
func waitForWellFormedTables(t *testing.T, dhts []*IpfsDHT, minPeers, avgPeers int, timeout time.Duration) bool {
	// test "well-formed-ness" (>= minPeers peers in every routing table)

	checkTables := func() bool {
		totalPeers := 0
		for _, dht := range dhts {
			rtlen := dht.routingTable.Size()
			totalPeers += rtlen
			if minPeers > 0 && rtlen < minPeers {
				t.Logf("routing table for %s only has %d peers (should have >%d)", dht.self, rtlen, minPeers)
				return false
			}
		}
		actualAvgPeers := totalPeers / len(dhts)
		t.Logf("avg rt size: %d", actualAvgPeers)
		if avgPeers > 0 && actualAvgPeers < avgPeers {
			t.Logf("avg rt size: %d < %d", actualAvgPeers, avgPeers)
			return false
		}
		return true
	}

	timeoutA := time.After(timeout)
	for {
		select {
		case <-timeoutA:
			log.Debugf("did not reach well-formed routing tables by %s", timeout)
			return false // failed
		case <-time.After(5 * time.Millisecond):
			if checkTables() {
				return true // succeeded
			}
		}
	}
}

func printRoutingTables(dhts []*IpfsDHT) {
	// the routing tables should be full now. let's inspect them.
	fmt.Printf("checking routing table of %d\n", len(dhts))
	for _, dht := range dhts {
		fmt.Printf("checking routing table of %s\n", dht.self)
		dht.routingTable.Print()
		fmt.Println("")
	}
}

func TestBootstrap(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	nDHTs := 30
	_, _, dhts := setupDHTS(ctx, nDHTs, t)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	<-time.After(100 * time.Millisecond)
	// bootstrap a few times until we get good tables.
	stop := make(chan struct{})
	go func() {
		for {
			t.Logf("bootstrapping them so they find each other %d", nDHTs)
			ctxT, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			bootstrap(t, ctxT, dhts)

			select {
			case <-time.After(50 * time.Millisecond):
				continue // being explicit
			case <-stop:
				return
			}
		}
	}()

	waitForWellFormedTables(t, dhts, 7, 10, 20*time.Second)
	close(stop)

	if u.Debug {
		// the routing tables should be full now. let's inspect them.
		printRoutingTables(dhts)
	}
}

func TestPeriodicBootstrap(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if ci.IsRunning() {
		t.Skip("skipping on CI. highly timing dependent")
	}
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	nDHTs := 30
	_, _, dhts := setupDHTS(ctx, nDHTs, t)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	// signal amplifier
	amplify := func(signal chan time.Time, other []chan time.Time) {
		for t := range signal {
			for _, s := range other {
				s <- t
			}
		}
		for _, s := range other {
			close(s)
		}
	}

	signal := make(chan time.Time)
	allSignals := []chan time.Time{}

	var cfg BootstrapConfig
	cfg = DefaultBootstrapConfig
	cfg.Queries = 5

	// kick off periodic bootstrappers with instrumented signals.
	for _, dht := range dhts {
		s := make(chan time.Time)
		allSignals = append(allSignals, s)
		dht.BootstrapOnSignal(cfg, s)
	}
	go amplify(signal, allSignals)

	t.Logf("dhts are not connected. %d", nDHTs)
	for _, dht := range dhts {
		rtlen := dht.routingTable.Size()
		if rtlen > 0 {
			t.Errorf("routing table for %s should have 0 peers. has %d", dht.self, rtlen)
		}
	}

	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	t.Logf("DHTs are now connected to 1-2 others. %d", nDHTs)
	for _, dht := range dhts {
		rtlen := dht.routingTable.Size()
		if rtlen > 2 {
			t.Errorf("routing table for %s should have at most 2 peers. has %d", dht.self, rtlen)
		}
	}

	if u.Debug {
		printRoutingTables(dhts)
	}

	t.Logf("bootstrapping them so they find each other. %d", nDHTs)
	signal <- time.Now()

	// this is async, and we dont know when it's finished with one cycle, so keep checking
	// until the routing tables look better, or some long timeout for the failure case.
	waitForWellFormedTables(t, dhts, 7, 10, 20*time.Second)

	if u.Debug {
		printRoutingTables(dhts)
	}
}

func TestProvidesMany(t *testing.T) {
	t.Skip("this test doesn't work")
	// t.Skip("skipping test to debug another")
	ctx := context.Background()

	nDHTs := 40
	_, _, dhts := setupDHTS(ctx, nDHTs, t)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	<-time.After(100 * time.Millisecond)
	t.Logf("bootstrapping them so they find each other. %d", nDHTs)
	ctxT, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	bootstrap(t, ctxT, dhts)

	if u.Debug {
		// the routing tables should be full now. let's inspect them.
		t.Logf("checking routing table of %d", nDHTs)
		for _, dht := range dhts {
			fmt.Printf("checking routing table of %s\n", dht.self)
			dht.routingTable.Print()
			fmt.Println("")
		}
	}

	providers := make(map[string]peer.ID)

	d := 0
	for _, c := range testCaseCids {
		d = (d + 1) % len(dhts)
		dht := dhts[d]
		providers[c.KeyString()] = dht.self

		t.Logf("announcing provider for %s", c)
		if err := dht.Provide(ctx, c, true); err != nil {
			t.Fatal(err)
		}
	}

	// what is this timeout for? was 60ms before.
	time.Sleep(time.Millisecond * 6)

	errchan := make(chan error)

	ctxT, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	getProvider := func(dht *IpfsDHT, k *cid.Cid) {
		defer wg.Done()

		expected := providers[k.KeyString()]

		provchan := dht.FindProvidersAsync(ctxT, k, 1)
		select {
		case prov := <-provchan:
			actual := prov.ID
			if actual == "" {
				errchan <- fmt.Errorf("Got back nil provider (%s at %s)", k, dht.self)
			} else if actual != expected {
				errchan <- fmt.Errorf("Got back wrong provider (%s != %s) (%s at %s)",
					expected, actual, k, dht.self)
			}
		case <-ctxT.Done():
			errchan <- fmt.Errorf("Did not get a provider back (%s at %s)", k, dht.self)
		}
	}

	for _, c := range testCaseCids {
		// everyone should be able to find it...
		for _, dht := range dhts {
			log.Debugf("getting providers for %s at %s", c, dht.self)
			wg.Add(1)
			go getProvider(dht, c)
		}
	}

	// we need this because of printing errors
	go func() {
		wg.Wait()
		close(errchan)
	}()

	for err := range errchan {
		t.Error(err)
	}
}

func TestProvidesAsync(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	_, _, dhts := setupDHTS(ctx, 4, t)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	err := dhts[3].Provide(ctx, testCaseCids[0], true)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 60)

	ctxT, cancel := context.WithTimeout(ctx, time.Millisecond*300)
	defer cancel()
	provs := dhts[0].FindProvidersAsync(ctxT, testCaseCids[0], 5)
	select {
	case p, ok := <-provs:
		if !ok {
			t.Fatal("Provider channel was closed...")
		}
		if p.ID == "" {
			t.Fatal("Got back nil provider!")
		}
		if p.ID != dhts[3].self {
			t.Fatalf("got a provider, but not the right one. %s", p)
		}
	case <-ctxT.Done():
		t.Fatal("Didnt get back providers")
	}
}

func TestLayeredGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, dhts := setupDHTS(ctx, 4, t)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[2], dhts[3])

	err := dhts[3].PutValue(ctx, "/v/hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 6)

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	val, err := dhts[0].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Error("got wrong value")
	}
}

func TestFindPeer(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	_, peers, dhts := setupDHTS(ctx, 4, t)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			dhts[i].host.Close()
		}
	}()

	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	p, err := dhts[0].FindPeer(ctxT, peers[2])
	if err != nil {
		t.Fatal(err)
	}

	if p.ID == "" {
		t.Fatal("Failed to find peer.")
	}

	if p.ID != peers[2] {
		t.Fatal("Didnt find expected peer.")
	}
}

func TestFindPeersConnectedToPeer(t *testing.T) {
	t.Skip("not quite correct (see note)")

	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	_, peers, dhts := setupDHTS(ctx, 4, t)
	defer func() {
		for i := 0; i < 4; i++ {
			dhts[i].Close()
			dhts[i].host.Close()
		}
	}()

	// topology:
	// 0-1, 1-2, 1-3, 2-3
	connect(t, ctx, dhts[0], dhts[1])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])
	connect(t, ctx, dhts[2], dhts[3])

	// fmt.Println("0 is", peers[0])
	// fmt.Println("1 is", peers[1])
	// fmt.Println("2 is", peers[2])
	// fmt.Println("3 is", peers[3])

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pchan, err := dhts[0].FindPeersConnectedToPeer(ctxT, peers[2])
	if err != nil {
		t.Fatal(err)
	}

	// shouldFind := []peer.ID{peers[1], peers[3]}
	var found []*pstore.PeerInfo
	for nextp := range pchan {
		found = append(found, nextp)
	}

	// fmt.Printf("querying 0 (%s) FindPeersConnectedToPeer 2 (%s)\n", peers[0], peers[2])
	// fmt.Println("should find 1, 3", shouldFind)
	// fmt.Println("found", found)

	// testPeerListsMatch(t, shouldFind, found)

	log.Warning("TestFindPeersConnectedToPeer is not quite correct")
	if len(found) == 0 {
		t.Fatal("didn't find any peers.")
	}
}

func testPeerListsMatch(t *testing.T, p1, p2 []peer.ID) {

	if len(p1) != len(p2) {
		t.Fatal("did not find as many peers as should have", p1, p2)
	}

	ids1 := make([]string, len(p1))
	ids2 := make([]string, len(p2))

	for i, p := range p1 {
		ids1[i] = string(p)
	}

	for i, p := range p2 {
		ids2[i] = string(p)
	}

	sort.Sort(sort.StringSlice(ids1))
	sort.Sort(sort.StringSlice(ids2))

	for i := range ids1 {
		if ids1[i] != ids2[i] {
			t.Fatal("Didnt find expected peer", ids1[i], ids2)
		}
	}
}

func TestConnectCollision(t *testing.T) {
	// t.Skip("skipping test to debug another")
	if testing.Short() {
		t.SkipNow()
	}
	if travisci.IsRunning() {
		t.Skip("Skipping on Travis-CI.")
	}

	runTimes := 10

	for rtime := 0; rtime < runTimes; rtime++ {
		log.Info("Running Time: ", rtime)

		ctx := context.Background()

		dhtA := setupDHT(ctx, t, false)
		dhtB := setupDHT(ctx, t, false)

		addrA := dhtA.peerstore.Addrs(dhtA.self)[0]
		addrB := dhtB.peerstore.Addrs(dhtB.self)[0]

		peerA := dhtA.self
		peerB := dhtB.self

		errs := make(chan error)
		go func() {
			dhtA.peerstore.AddAddr(peerB, addrB, pstore.TempAddrTTL)
			pi := pstore.PeerInfo{ID: peerB}
			err := dhtA.host.Connect(ctx, pi)
			errs <- err
		}()
		go func() {
			dhtB.peerstore.AddAddr(peerA, addrA, pstore.TempAddrTTL)
			pi := pstore.PeerInfo{ID: peerA}
			err := dhtB.host.Connect(ctx, pi)
			errs <- err
		}()

		timeout := time.After(5 * time.Second)
		select {
		case e := <-errs:
			if e != nil {
				t.Fatal(e)
			}
		case <-timeout:
			t.Fatal("Timeout received!")
		}
		select {
		case e := <-errs:
			if e != nil {
				t.Fatal(e)
			}
		case <-timeout:
			t.Fatal("Timeout received!")
		}

		dhtA.Close()
		dhtB.Close()
		dhtA.host.Close()
		dhtB.host.Close()
	}
}

func TestBadProtoMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := setupDHT(ctx, t, false)

	nilrec := new(pb.Message)
	if _, err := d.handlePutValue(ctx, "testpeer", nilrec); err == nil {
		t.Fatal("should have errored on nil record")
	}
}

func TestClientModeConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := setupDHT(ctx, t, false)
	b := setupDHT(ctx, t, true)

	connectNoSync(t, ctx, a, b)

	c := testCaseCids[0]
	p := peer.ID("TestPeer")
	a.providers.AddProvider(ctx, c, p)
	time.Sleep(time.Millisecond * 5) // just in case...

	provs, err := b.FindProviders(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	if len(provs) == 0 {
		t.Fatal("Expected to get a provider back")
	}

	if provs[0].ID != p {
		t.Fatal("expected it to be our test peer")
	}
}

func TestFindPeerQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 101
	_, allpeers, dhts := setupDHTS(ctx, nDHTs, t)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	mrand := rand.New(rand.NewSource(42))
	guy := dhts[0]
	others := dhts[1:]
	for i := 0; i < 20; i++ {
		for j := 0; j < 16; j++ { // 16, high enough to probably not have any partitions
			v := mrand.Intn(80)
			connect(t, ctx, others[i], others[20+v])
		}
	}

	for i := 0; i < 20; i++ {
		connect(t, ctx, guy, others[i])
	}

	val := "foobar"
	rtval := kb.ConvertKey(val)

	rtablePeers := guy.routingTable.NearestPeers(rtval, AlphaValue)
	if len(rtablePeers) != 3 {
		t.Fatalf("expected 3 peers back from routing table, got %d", len(rtablePeers))
	}

	netpeers := guy.host.Network().Peers()
	if len(netpeers) != 20 {
		t.Fatalf("expected 20 peers to be connected, got %d", len(netpeers))
	}

	rtableset := make(map[peer.ID]bool)
	for _, p := range rtablePeers {
		rtableset[p] = true
	}

	out, err := guy.GetClosestPeers(ctx, val)
	if err != nil {
		t.Fatal(err)
	}

	var notfromrtable int
	var count int
	var outpeers []peer.ID
	for p := range out {
		count++
		if !rtableset[p] {
			notfromrtable++
		}
		outpeers = append(outpeers, p)
	}

	if notfromrtable == 0 {
		t.Fatal("got entirely peers from our routing table")
	}

	if count != 20 {
		t.Fatal("should have only gotten 20 peers from getclosestpeers call")
	}

	sort.Sort(peer.IDSlice(allpeers[1:]))
	sort.Sort(peer.IDSlice(outpeers))
	fmt.Println("counts: ", count, notfromrtable)
	actualclosest := kb.SortClosestPeers(allpeers[1:], rtval)
	exp := actualclosest[:20]
	got := kb.SortClosestPeers(outpeers, rtval)

	diffp := countDiffPeers(exp, got)
	if diffp > 0 {
		// could be a partition created during setup
		t.Fatal("didnt get expected closest peers")
	}
}

func countDiffPeers(a, b []peer.ID) int {
	s := make(map[peer.ID]bool)
	for _, p := range a {
		s[p] = true
	}
	var out int
	for _, p := range b {
		if !s[p] {
			out++
		}
	}
	return out
}

func TestFindClosestPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 30
	_, _, dhts := setupDHTS(ctx, nDHTs, t)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
			defer dhts[i].host.Close()
		}
	}()

	t.Logf("connecting %d dhts in a ring", nDHTs)
	for i := 0; i < nDHTs; i++ {
		connect(t, ctx, dhts[i], dhts[(i+1)%len(dhts)])
	}

	peers, err := dhts[1].GetClosestPeers(ctx, "foo")
	if err != nil {
		t.Fatal(err)
	}

	var out []peer.ID
	for p := range peers {
		out = append(out, p)
	}

	if len(out) != KValue {
		t.Fatalf("got wrong number of peers (got %d, expected %d)", len(out), KValue)
	}
}
