package dht

import (
	"context"
	"errors"
	"fmt"
	"time"

	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	lgbl "gx/ipfs/QmT4PgCNdv73hnFAqzHqwW44q7M9PWpykSswHDxndquZbc/go-libp2p-loggables"
	pb "gx/ipfs/QmTHyAbD9KzGrseLNzmEoNkVxA8F2h7LQG2iV6uhBqs6kX/go-libp2p-kad-dht/pb"
	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
	base32 "gx/ipfs/QmZvZSVtvxak4dcTkhsQhqd1SQ6rg5UzaSTu62WfWKjj93/base32"
	recpb "gx/ipfs/QmbxkgUceEcuSZ4ZdBA3x74VUDSSYjHYmmeEqkjxbtZ6Jg/go-libp2p-record/pb"
)

// The number of closer peers to send on requests.
var CloserPeerCount = KValue

// dhthandler specifies the signature of functions that handle DHT messages.
type dhtHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (dht *IpfsDHT) handlerForMsgType(t pb.Message_MessageType) dhtHandler {
	switch t {
	case pb.Message_GET_VALUE:
		return dht.handleGetValue
	case pb.Message_PUT_VALUE:
		return dht.handlePutValue
	case pb.Message_FIND_NODE:
		return dht.handleFindPeer
	case pb.Message_ADD_PROVIDER:
		return dht.handleAddProvider
	case pb.Message_GET_PROVIDERS:
		return dht.handleGetProviders
	case pb.Message_PING:
		return dht.handlePing
	default:
		return nil
	}
}

func (dht *IpfsDHT) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	defer log.EventBegin(ctx, "handleGetValue", p).Done()
	log.Debugf("%s handleGetValue for key: %s", dht.self, pmes.GetKey())

	// setup response
	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	// first, is there even a key?
	k := pmes.GetKey()
	if k == "" {
		return nil, errors.New("handleGetValue but no key was provided")
		// TODO: send back an error response? could be bad, but the other node's hanging.
	}

	rec, err := dht.checkLocalDatastore(k)
	if err != nil {
		return nil, err
	}
	resp.Record = rec

	// Find closest peer on given cluster to desired key and reply with that info
	closer := dht.betterPeersToQuery(pmes, p, CloserPeerCount)
	if len(closer) > 0 {
		closerinfos := pstore.PeerInfos(dht.peerstore, closer)
		for _, pi := range closerinfos {
			log.Debugf("handleGetValue returning closer peer: '%s'", pi.ID)
			if len(pi.Addrs) < 1 {
				log.Warningf(`no addresses on peer being sent!
					[local:%s]
					[sending:%s]
					[remote:%s]`, dht.self, pi.ID, p)
			}
		}

		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), closerinfos)
	}

	return resp, nil
}

func (dht *IpfsDHT) checkLocalDatastore(k string) (*recpb.Record, error) {
	log.Debugf("%s handleGetValue looking into ds", dht.self)
	dskey := convertToDsKey(k)
	iVal, err := dht.datastore.Get(dskey)
	log.Debugf("%s handleGetValue looking into ds GOT %v", dht.self, iVal)

	if err == ds.ErrNotFound {
		return nil, nil
	}

	// if we got an unexpected error, bail.
	if err != nil {
		return nil, err
	}

	// if we have the value, send it back
	log.Debugf("%s handleGetValue success!", dht.self)

	byts, ok := iVal.([]byte)
	if !ok {
		return nil, fmt.Errorf("datastore had non byte-slice value for %v", dskey)
	}

	rec := new(recpb.Record)
	err = proto.Unmarshal(byts, rec)
	if err != nil {
		log.Debug("failed to unmarshal DHT record from datastore")
		return nil, err
	}

	// if its our record, dont bother checking the times on it
	if peer.ID(rec.GetAuthor()) == dht.self {
		return rec, nil
	}

	var recordIsBad bool
	recvtime, err := u.ParseRFC3339(rec.GetTimeReceived())
	if err != nil {
		log.Info("either no receive time set on record, or it was invalid: ", err)
		recordIsBad = true
	}

	if time.Now().Sub(recvtime) > MaxRecordAge {
		log.Debug("old record found, tossing.")
		recordIsBad = true
	}

	// NOTE: We do not verify the record here beyond checking these timestamps.
	// we put the burden of checking the records on the requester as checking a record
	// may be computationally expensive

	if recordIsBad {
		err := dht.datastore.Delete(dskey)
		if err != nil {
			log.Error("Failed to delete bad record from datastore: ", err)
		}

		return nil, nil // can treat this as not having the record at all
	}

	return rec, nil
}

// Store a value in this peer local storage
func (dht *IpfsDHT) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	defer log.EventBegin(ctx, "handlePutValue", p).Done()
	dskey := convertToDsKey(pmes.GetKey())

	rec := pmes.GetRecord()
	if rec == nil {
		log.Infof("Got nil record from: %s", p.Pretty())
		return nil, errors.New("nil record")
	}

	if err := dht.verifyRecordLocally(rec); err != nil {
		log.Warningf("Bad dht record in PUT from: %s. %s", peer.ID(pmes.GetRecord().GetAuthor()), err)
		return nil, err
	}

	// record the time we receive every record
	rec.TimeReceived = proto.String(u.FormatRFC3339(time.Now()))

	data, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}

	err = dht.datastore.Put(dskey, data)
	log.Debugf("%s handlePutValue %v", dht.self, dskey)
	return pmes, err
}

func (dht *IpfsDHT) handlePing(_ context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	log.Debugf("%s Responding to ping from %s!\n", dht.self, p)
	return pmes, nil
}

func (dht *IpfsDHT) handleFindPeer(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	defer log.EventBegin(ctx, "handleFindPeer", p).Done()
	resp := pb.NewMessage(pmes.GetType(), "", pmes.GetClusterLevel())
	var closest []peer.ID

	// if looking for self... special case where we send it on CloserPeers.
	if peer.ID(pmes.GetKey()) == dht.self {
		closest = []peer.ID{dht.self}
	} else {
		closest = dht.betterPeersToQuery(pmes, p, CloserPeerCount)
	}

	if closest == nil {
		log.Infof("%s handleFindPeer %s: could not find anything.", dht.self, p)
		return resp, nil
	}

	var withAddresses []pstore.PeerInfo
	closestinfos := pstore.PeerInfos(dht.peerstore, closest)
	for _, pi := range closestinfos {
		if len(pi.Addrs) > 0 {
			withAddresses = append(withAddresses, pi)
			log.Debugf("handleFindPeer: sending back '%s'", pi.ID)
		}
	}

	resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), withAddresses)
	return resp, nil
}

func (dht *IpfsDHT) handleGetProviders(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	lm := make(lgbl.DeferredMap)
	lm["peer"] = func() interface{} { return p.Pretty() }
	defer log.EventBegin(ctx, "handleGetProviders", lm).Done()

	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())
	c, err := cid.Cast([]byte(pmes.GetKey()))
	if err != nil {
		return nil, err
	}

	lm["key"] = func() interface{} { return c.String() }

	// debug logging niceness.
	reqDesc := fmt.Sprintf("%s handleGetProviders(%s, %s): ", dht.self, p, c)
	log.Debugf("%s begin", reqDesc)
	defer log.Debugf("%s end", reqDesc)

	// check if we have this value, to add ourselves as provider.
	has, err := dht.datastore.Has(convertToDsKey(c.KeyString()))
	if err != nil && err != ds.ErrNotFound {
		log.Debugf("unexpected datastore error: %v\n", err)
		has = false
	}

	// setup providers
	providers := dht.providers.GetProviders(ctx, c)
	if has {
		providers = append(providers, dht.self)
		log.Debugf("%s have the value. added self as provider", reqDesc)
	}

	if providers != nil && len(providers) > 0 {
		infos := pstore.PeerInfos(dht.peerstore, providers)
		resp.ProviderPeers = pb.PeerInfosToPBPeers(dht.host.Network(), infos)
		log.Debugf("%s have %d providers: %s", reqDesc, len(providers), infos)
	}

	// Also send closer peers.
	closer := dht.betterPeersToQuery(pmes, p, CloserPeerCount)
	if closer != nil {
		infos := pstore.PeerInfos(dht.peerstore, closer)
		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), infos)
		log.Debugf("%s have %d closer peers: %s", reqDesc, len(closer), infos)
	}

	return resp, nil
}

func (dht *IpfsDHT) handleAddProvider(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	lm := make(lgbl.DeferredMap)
	lm["peer"] = func() interface{} { return p.Pretty() }

	defer log.EventBegin(ctx, "handleAddProvider", lm).Done()
	c, err := cid.Cast([]byte(pmes.GetKey()))
	if err != nil {
		return nil, err
	}

	lm["key"] = func() interface{} { return c.String() }

	log.Debugf("%s adding %s as a provider for '%s'\n", dht.self, p, c)

	// add provider should use the address given in the message
	pinfos := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
	for _, pi := range pinfos {
		if pi.ID != p {
			// we should ignore this provider reccord! not from originator.
			// (we chould sign them and check signature later...)
			log.Debugf("handleAddProvider received provider %s from %s. Ignore.", pi.ID, p)
			continue
		}

		if len(pi.Addrs) < 1 {
			log.Debugf("%s got no valid addresses for provider %s. Ignore.", dht.self, p)
			continue
		}

		log.Infof("received provider %s for %s (addrs: %s)", p, c, pi.Addrs)
		if pi.ID != dht.self { // dont add own addrs.
			// add the received addresses to our peerstore.
			dht.peerstore.AddAddrs(pi.ID, pi.Addrs, pstore.ProviderAddrTTL)
		}
		dht.providers.AddProvider(ctx, c, p)
	}

	return nil, nil
}

func convertToDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}
