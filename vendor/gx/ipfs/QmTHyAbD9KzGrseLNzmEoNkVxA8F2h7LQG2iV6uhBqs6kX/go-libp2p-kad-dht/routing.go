package dht

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	routing "gx/ipfs/QmPjTrrSfE6TzLv6ya6VWhGcCgPrUAdcgrDcQyRDX2VyW1/go-libp2p-routing"
	notif "gx/ipfs/QmPjTrrSfE6TzLv6ya6VWhGcCgPrUAdcgrDcQyRDX2VyW1/go-libp2p-routing/notifications"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	pb "gx/ipfs/QmTHyAbD9KzGrseLNzmEoNkVxA8F2h7LQG2iV6uhBqs6kX/go-libp2p-kad-dht/pb"
	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	pset "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer/peerset"
	inet "gx/ipfs/QmahYsGWry85Y7WUe2SX5G4JkH2zifEQAUtJVLZ24aC9DF/go-libp2p-net"
	kb "gx/ipfs/QmbiCMdwmmhif5axuGSHzYbPFGeKjLAuMY6JrGpVteHFsy/go-libp2p-kbucket"
	record "gx/ipfs/QmbxkgUceEcuSZ4ZdBA3x74VUDSSYjHYmmeEqkjxbtZ6Jg/go-libp2p-record"
)

// asyncQueryBuffer is the size of buffered channels in async queries. This
// buffer allows multiple queries to execute simultaneously, return their
// results and continue querying closer peers. Note that different query
// results will wait for the channel to drain.
var asyncQueryBuffer = 10

// This file implements the Routing interface for the IpfsDHT struct.

// Basic Put/Get

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *IpfsDHT) PutValue(ctx context.Context, key string, value []byte) error {
	log.Debugf("PutValue %s", key)
	sk, err := dht.getOwnPrivateKey()
	if err != nil {
		return err
	}

	sign, err := dht.Validator.IsSigned(key)
	if err != nil {
		return err
	}

	rec, err := record.MakePutRecord(sk, key, value, sign)
	if err != nil {
		log.Debug("creation of record failed!")
		return err
	}

	err = dht.putLocal(key, rec)
	if err != nil {
		return err
	}

	pchan, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range pchan {
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			notif.PublishQueryEvent(ctx, &notif.QueryEvent{
				Type: notif.Value,
				ID:   p,
			})

			err := dht.putValueToPeer(ctx, p, key, rec)
			if err != nil {
				log.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// GetValue searches for the value corresponding to given Key.
func (dht *IpfsDHT) GetValue(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	vals, err := dht.GetValues(ctx, key, 16)
	if err != nil {
		return nil, err
	}

	var recs [][]byte
	for _, v := range vals {
		if v.Val != nil {
			recs = append(recs, v.Val)
		}
	}

	i, err := dht.Selector.BestRecord(key, recs)
	if err != nil {
		return nil, err
	}

	best := recs[i]
	log.Debugf("GetValue %v %v", key, best)
	if best == nil {
		log.Errorf("GetValue yielded correct record with nil value.")
		return nil, routing.ErrNotFound
	}

	fixupRec, err := record.MakePutRecord(dht.peerstore.PrivKey(dht.self), key, best, true)
	if err != nil {
		// probably shouldnt actually 'error' here as we have found a value we like,
		// but this call failing probably isnt something we want to ignore
		return nil, err
	}

	for _, v := range vals {
		// if someone sent us a different 'less-valid' record, lets correct them
		if !bytes.Equal(v.Val, best) {
			go func(v routing.RecvdVal) {
				if v.From == dht.self {
					err := dht.putLocal(key, fixupRec)
					if err != nil {
						log.Error("Error correcting local dht entry:", err)
					}
					return
				}
				ctx, cancel := context.WithTimeout(dht.Context(), time.Second*30)
				defer cancel()
				err := dht.putValueToPeer(ctx, v.From, key, fixupRec)
				if err != nil {
					log.Error("Error correcting DHT entry: ", err)
				}
			}(v)
		}
	}

	return best, nil
}

func (dht *IpfsDHT) GetValues(ctx context.Context, key string, nvals int) ([]routing.RecvdVal, error) {
	var vals []routing.RecvdVal
	var valslock sync.Mutex

	// If we have it local, dont bother doing an RPC!
	lrec, err := dht.getLocal(key)
	if err == nil {
		// TODO: this is tricky, we dont always want to trust our own value
		// what if the authoritative source updated it?
		log.Debug("have it locally")
		vals = append(vals, routing.RecvdVal{
			Val:  lrec.GetValue(),
			From: dht.self,
		})

		if nvals <= 1 {
			return vals, nil
		}
	} else if nvals == 0 {
		return nil, err
	}

	// get closest peers in the routing table
	rtp := dht.routingTable.NearestPeers(kb.ConvertKey(key), AlphaValue)
	log.Debugf("peers in rt: %d %s", len(rtp), rtp)
	if len(rtp) == 0 {
		log.Warning("No peers from routing table!")
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		rec, peers, err := dht.getValueOrPeers(ctx, p, key)
		switch err {
		case routing.ErrNotFound:
			// in this case, they responded with nothing,
			// still send a notification so listeners can know the
			// request has completed 'successfully'
			notif.PublishQueryEvent(parent, &notif.QueryEvent{
				Type: notif.PeerResponse,
				ID:   p,
			})
			return nil, err
		default:
			return nil, err

		case nil, errInvalidRecord:
			// in either of these cases, we want to keep going
		}

		res := &dhtQueryResult{closerPeers: peers}

		if rec.GetValue() != nil || err == errInvalidRecord {
			rv := routing.RecvdVal{
				Val:  rec.GetValue(),
				From: p,
			}
			valslock.Lock()
			vals = append(vals, rv)

			// If weve collected enough records, we're done
			if len(vals) >= nvals {
				res.success = true
			}
			valslock.Unlock()
		}

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return res, nil
	})

	// run it!
	_, err = query.Run(ctx, rtp)
	if len(vals) == 0 {
		if err != nil {
			return nil, err
		}
	}

	return vals, nil

}

// Value provider layer of indirection.
// This is what DSHTs (Coral and MainlineDHT) do to store large values in a DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) Provide(ctx context.Context, key *cid.Cid, brdcst bool) error {
	defer log.EventBegin(ctx, "provide", key, logging.LoggableMap{"broadcast": brdcst}).Done()

	// add self locally
	dht.providers.AddProvider(ctx, key, dht.self)
	if !brdcst {
		return nil
	}

	peers, err := dht.GetClosestPeers(ctx, key.KeyString())
	if err != nil {
		return err
	}

	mes, err := dht.makeProvRecord(key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			log.Debugf("putProvider(%s, %s)", key, p)
			err := dht.sendMessage(ctx, p, mes)
			if err != nil {
				log.Debug(err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}
func (dht *IpfsDHT) makeProvRecord(skey *cid.Cid) (*pb.Message, error) {
	pi := pstore.PeerInfo{
		ID:    dht.self,
		Addrs: dht.host.Addrs(),
	}

	// // only share WAN-friendly addresses ??
	// pi.Addrs = addrutil.WANShareableAddrs(pi.Addrs)
	if len(pi.Addrs) < 1 {
		return nil, fmt.Errorf("no known addresses for self. cannot put provider.")
	}

	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, skey.KeyString(), 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]pstore.PeerInfo{pi})
	return pmes, nil
}

// FindProviders searches until the context expires.
func (dht *IpfsDHT) FindProviders(ctx context.Context, c *cid.Cid) ([]pstore.PeerInfo, error) {
	var providers []pstore.PeerInfo
	for p := range dht.FindProvidersAsync(ctx, c, KValue) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key *cid.Cid, count int) <-chan pstore.PeerInfo {
	log.Event(ctx, "findProviders", key)
	peerOut := make(chan pstore.PeerInfo, count)
	go dht.findProvidersAsyncRoutine(ctx, key, count, peerOut)
	return peerOut
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key *cid.Cid, count int, peerOut chan pstore.PeerInfo) {
	defer log.EventBegin(ctx, "findProvidersAsync", key).Done()
	defer close(peerOut)

	ps := pset.NewLimited(count)
	provs := dht.providers.GetProviders(ctx, key)
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if ps.TryAdd(p) {
			pi := dht.peerstore.PeerInfo(p)
			select {
			case peerOut <- pi:
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, dont bother with remote RPC
		// TODO: is this a DOS vector?
		if ps.Size() >= count {
			return
		}
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(key.KeyString(), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})
		pmes, err := dht.findProvidersSingle(ctx, p, key)
		if err != nil {
			return nil, err
		}

		log.Debugf("%d provider entries", len(pmes.GetProviderPeers()))
		provs := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
		log.Debugf("%d provider entries decoded", len(provs))

		// Add unique providers from request, up to 'count'
		for _, prov := range provs {
			if prov.ID != dht.self {
				dht.peerstore.AddAddrs(prov.ID, prov.Addrs, pstore.TempAddrTTL)
			}
			log.Debugf("got provider: %s", prov)
			if ps.TryAdd(prov.ID) {
				log.Debugf("using provider: %s", prov)
				select {
				case peerOut <- *prov:
				case <-ctx.Done():
					log.Debug("context timed out sending more providers")
					return nil, ctx.Err()
				}
			}
			if ps.Size() >= count {
				log.Debugf("got enough providers (%d/%d)", ps.Size(), count)
				return &dhtQueryResult{success: true}, nil
			}
		}

		// Give closer peers back to the query to be queried
		closer := pmes.GetCloserPeers()
		clpeers := pb.PBPeersToPeerInfos(closer)
		log.Debugf("got closer peers: %d %s", len(clpeers), clpeers)

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: clpeers,
		})
		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	peers := dht.routingTable.NearestPeers(kb.ConvertKey(key.KeyString()), AlphaValue)
	_, err := query.Run(ctx, peers)
	if err != nil {
		log.Debugf("Query error: %s", err)
		// Special handling for issue: https://github.com/ipfs/go-ipfs/issues/3032
		if fmt.Sprint(err) == "<nil>" {
			log.Error("reproduced bug 3032:")
			log.Errorf("Errors type information: %#v", err)
			log.Errorf("go version: %s", runtime.Version())
			log.Error("please report this information to: https://github.com/ipfs/go-ipfs/issues/3032")

			// replace problematic error with something that won't crash the daemon
			err = fmt.Errorf("<nil>")
		}
		notif.PublishQueryEvent(ctx, &notif.QueryEvent{
			Type:  notif.QueryError,
			Extra: err.Error(),
		})
	}
}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (pstore.PeerInfo, error) {
	defer log.EventBegin(ctx, "FindPeer", id).Done()

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	peers := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if len(peers) == 0 {
		return pstore.PeerInfo{}, kb.ErrLookupFailure
	}

	// Sanity...
	for _, p := range peers {
		if p == id {
			log.Debug("found target peer in list of closest peers...")
			return dht.peerstore.PeerInfo(p), nil
		}
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(string(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		closer := pmes.GetCloserPeers()
		clpeerInfos := pb.PBPeersToPeerInfos(closer)

		// see it we got the peer here
		for _, npi := range clpeerInfos {
			if npi.ID == id {
				return &dhtQueryResult{
					peer:    npi,
					success: true,
				}, nil
			}
		}

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			Responses: clpeerInfos,
		})

		return &dhtQueryResult{closerPeers: clpeerInfos}, nil
	})

	// run it!
	result, err := query.Run(ctx, peers)
	if err != nil {
		return pstore.PeerInfo{}, err
	}

	log.Debugf("FindPeer %v %v", id, result.success)
	if result.peer.ID == "" {
		return pstore.PeerInfo{}, routing.ErrNotFound
	}

	return *result.peer, nil
}

// FindPeersConnectedToPeer searches for peers directly connected to a given peer.
func (dht *IpfsDHT) FindPeersConnectedToPeer(ctx context.Context, id peer.ID) (<-chan *pstore.PeerInfo, error) {

	peerchan := make(chan *pstore.PeerInfo, asyncQueryBuffer)
	peersSeen := make(map[peer.ID]struct{})

	peers := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), AlphaValue)
	if len(peers) == 0 {
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	query := dht.newQuery(string(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		var clpeers []*pstore.PeerInfo
		closer := pmes.GetCloserPeers()
		for _, pbp := range closer {
			pi := pb.PBPeerToPeerInfo(pbp)

			// skip peers already seen
			if _, found := peersSeen[pi.ID]; found {
				continue
			}
			peersSeen[pi.ID] = struct{}{}

			// if peer is connected, send it to our client.
			if pb.Connectedness(*pbp.Connection) == inet.Connected {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case peerchan <- pi:
				}
			}

			// if peer is the peer we're looking for, don't bother querying it.
			// TODO maybe query it?
			if pb.Connectedness(*pbp.Connection) != inet.Connected {
				clpeers = append(clpeers, pi)
			}
		}

		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	// run it! run it asynchronously to gen peers as results are found.
	// this does no error checking
	go func() {
		if _, err := query.Run(ctx, peers); err != nil {
			log.Debug(err)
		}

		// close the peerchan channel when done.
		close(peerchan)
	}()

	return peerchan, nil
}
