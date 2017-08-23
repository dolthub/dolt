package dht

import (
	"context"

	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	notif "gx/ipfs/QmPjTrrSfE6TzLv6ya6VWhGcCgPrUAdcgrDcQyRDX2VyW1/go-libp2p-routing/notifications"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	kb "gx/ipfs/QmbiCMdwmmhif5axuGSHzYbPFGeKjLAuMY6JrGpVteHFsy/go-libp2p-kbucket"
)

// Required in order for proper JSON marshaling
func pointerizePeerInfos(pis []pstore.PeerInfo) []*pstore.PeerInfo {
	out := make([]*pstore.PeerInfo, len(pis))
	for i, p := range pis {
		np := p
		out[i] = &np
	}
	return out
}

func toPeerInfos(ps []peer.ID) []*pstore.PeerInfo {
	out := make([]*pstore.PeerInfo, len(ps))
	for i, p := range ps {
		out[i] = &pstore.PeerInfo{ID: p}
	}
	return out
}

func loggableKey(k string) logging.LoggableMap {
	return logging.LoggableMap{
		"key": k,
	}
}

// Kademlia 'node lookup' operation. Returns a channel of the K closest peers
// to the given key
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) (<-chan peer.ID, error) {
	e := log.EventBegin(ctx, "getClosestPeers", loggableKey(key))
	tablepeers := dht.routingTable.NearestPeers(kb.ConvertKey(key), AlphaValue)
	if len(tablepeers) == 0 {
		return nil, kb.ErrLookupFailure
	}

	out := make(chan peer.ID, KValue)

	// since the query doesnt actually pass our context down
	// we have to hack this here. whyrusleeping isnt a huge fan of goprocess
	parent := ctx
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		// For DHT query command
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		closer, err := dht.closerPeersSingle(ctx, key, p)
		if err != nil {
			log.Debugf("error getting closer peers: %s", err)
			return nil, err
		}

		peerinfos := toPeerInfos(closer)

		// For DHT query command
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: peerinfos, // todo: remove need for this pointerize thing
		})

		return &dhtQueryResult{closerPeers: peerinfos}, nil
	})

	go func() {
		defer close(out)
		defer e.Done()
		// run it!
		res, err := query.Run(ctx, tablepeers)
		if err != nil {
			log.Debugf("closestPeers query run error: %s", err)
		}

		if res != nil && res.finalSet != nil {
			sorted := kb.SortClosestPeers(res.finalSet.Peers(), kb.ConvertKey(key))
			if len(sorted) > KValue {
				sorted = sorted[:KValue]
			}

			for _, p := range sorted {
				out <- p
			}
		}
	}()

	return out, nil
}

func (dht *IpfsDHT) closerPeersSingle(ctx context.Context, key string, p peer.ID) ([]peer.ID, error) {
	pmes, err := dht.findPeerSingle(ctx, p, peer.ID(key))
	if err != nil {
		return nil, err
	}

	var out []peer.ID
	for _, pbp := range pmes.GetCloserPeers() {
		pid := peer.ID(pbp.GetId())
		if pid != dht.self { // dont add self
			dht.peerstore.AddAddrs(pid, pbp.Addresses(), pstore.TempAddrTTL)
			out = append(out, pid)
		}
	}
	return out, nil
}
