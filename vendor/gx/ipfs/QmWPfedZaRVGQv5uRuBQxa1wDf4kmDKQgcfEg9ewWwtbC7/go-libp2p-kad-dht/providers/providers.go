package providers

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	goprocess "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
	goprocessctx "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess/context"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
	lru "gx/ipfs/QmVYxfoJQiZijTgPNHCHgHELvQpbsJNTg6Crmc3dQkj3yy/golang-lru"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	autobatch "gx/ipfs/Qmbi2cDAzNb9ofcdmXqhejBzuQwxCKV8xZu3gnWzJDFPQn/autobatch"
	base32 "gx/ipfs/QmfVj3x4D6Jkq9SEoi5n2NmoUomLwoeiwnYz2KQa15wRw6/base32"
)

var batchBufferSize = 256

var log = logging.Logger("providers")

var lruCacheSize = 256
var ProvideValidity = time.Hour * 24
var defaultCleanupInterval = time.Hour

type ProviderManager struct {
	// all non channel fields are meant to be accessed only within
	// the run method
	providers *lru.Cache
	lpeer     peer.ID
	dstore    ds.Datastore

	newprovs chan *addProv
	getprovs chan *getProv
	period   time.Duration
	proc     goprocess.Process

	cleanupInterval time.Duration
}

type providerSet struct {
	providers []peer.ID
	set       map[peer.ID]time.Time
}

type addProv struct {
	k   *cid.Cid
	val peer.ID
}

type getProv struct {
	k    *cid.Cid
	resp chan []peer.ID
}

func NewProviderManager(ctx context.Context, local peer.ID, dstore ds.Batching) *ProviderManager {
	pm := new(ProviderManager)
	pm.getprovs = make(chan *getProv)
	pm.newprovs = make(chan *addProv)
	pm.dstore = autobatch.NewAutoBatching(dstore, batchBufferSize)
	cache, err := lru.New(lruCacheSize)
	if err != nil {
		panic(err) //only happens if negative value is passed to lru constructor
	}
	pm.providers = cache

	pm.proc = goprocessctx.WithContext(ctx)
	pm.cleanupInterval = defaultCleanupInterval
	pm.proc.Go(func(p goprocess.Process) { pm.run() })

	return pm
}

const providersKeyPrefix = "/providers/"

func mkProvKey(k *cid.Cid) string {
	return providersKeyPrefix + base32.RawStdEncoding.EncodeToString(k.Bytes())
}

func (pm *ProviderManager) Process() goprocess.Process {
	return pm.proc
}

func (pm *ProviderManager) providersForKey(k *cid.Cid) ([]peer.ID, error) {
	pset, err := pm.getProvSet(k)
	if err != nil {
		return nil, err
	}
	return pset.providers, nil
}

func (pm *ProviderManager) getProvSet(k *cid.Cid) (*providerSet, error) {
	cached, ok := pm.providers.Get(k.KeyString())
	if ok {
		return cached.(*providerSet), nil
	}

	pset, err := loadProvSet(pm.dstore, k)
	if err != nil {
		return nil, err
	}

	if len(pset.providers) > 0 {
		pm.providers.Add(k.KeyString(), pset)
	}

	return pset, nil
}

func loadProvSet(dstore ds.Datastore, k *cid.Cid) (*providerSet, error) {
	res, err := dstore.Query(dsq.Query{Prefix: mkProvKey(k)})
	if err != nil {
		return nil, err
	}

	out := newProviderSet()
	for {
		e, ok := res.NextSync()
		if !ok {
			break
		}
		if e.Error != nil {
			log.Error("got an error: ", e.Error)
			continue
		}

		lix := strings.LastIndex(e.Key, "/")

		decstr, err := base32.RawStdEncoding.DecodeString(e.Key[lix+1:])
		if err != nil {
			log.Error("base32 decoding error: ", err)
			continue
		}

		pid := peer.ID(decstr)

		t, err := readTimeValue(e.Value)
		if err != nil {
			log.Warning("parsing providers record from disk: ", err)
			continue
		}

		out.setVal(pid, t)
	}

	return out, nil
}

func readTimeValue(i interface{}) (time.Time, error) {
	data, ok := i.([]byte)
	if !ok {
		return time.Time{}, fmt.Errorf("data was not a []byte")
	}

	nsec, _ := binary.Varint(data)

	return time.Unix(0, nsec), nil
}

func (pm *ProviderManager) addProv(k *cid.Cid, p peer.ID) error {
	iprovs, ok := pm.providers.Get(k.KeyString())
	if !ok {
		stored, err := loadProvSet(pm.dstore, k)
		if err != nil {
			return err
		}
		iprovs = stored
		pm.providers.Add(k.KeyString(), iprovs)
	}
	provs := iprovs.(*providerSet)
	now := time.Now()
	provs.setVal(p, now)

	return writeProviderEntry(pm.dstore, k, p, now)
}

func writeProviderEntry(dstore ds.Datastore, k *cid.Cid, p peer.ID, t time.Time) error {
	dsk := mkProvKey(k) + "/" + base32.RawStdEncoding.EncodeToString([]byte(p))

	buf := make([]byte, 16)
	n := binary.PutVarint(buf, t.UnixNano())

	return dstore.Put(ds.NewKey(dsk), buf[:n])
}

func (pm *ProviderManager) deleteProvSet(k *cid.Cid) error {
	pm.providers.Remove(k.KeyString())

	res, err := pm.dstore.Query(dsq.Query{
		KeysOnly: true,
		Prefix:   mkProvKey(k),
	})
	if err != nil {
		return err
	}

	entries, err := res.Rest()
	if err != nil {
		return err
	}

	for _, e := range entries {
		err := pm.dstore.Delete(ds.NewKey(e.Key))
		if err != nil {
			log.Error("deleting provider set: ", err)
		}
	}
	return nil
}

func (pm *ProviderManager) getProvKeys() (func() (*cid.Cid, bool), error) {
	res, err := pm.dstore.Query(dsq.Query{
		KeysOnly: false,
		Prefix:   providersKeyPrefix,
	})
	if err != nil {
		return nil, err
	}

	iter := func() (*cid.Cid, bool) {
		for e := range res.Next() {
			parts := strings.Split(e.Key, "/")
			if len(parts) != 4 {
				log.Warningf("incorrectly formatted provider entry in datastore: %s", e.Key)
				continue
			}
			decoded, err := base32.RawStdEncoding.DecodeString(parts[2])
			if err != nil {
				log.Warning("error decoding base32 provider key: %s: %s", parts[2], err)
				continue
			}

			c, err := cid.Cast(decoded)
			if err != nil {
				log.Warning("error casting key to cid from datastore key: %s", err)
				continue
			}

			return c, true
		}
		return nil, false
	}

	return iter, nil
}

func (pm *ProviderManager) run() {
	tick := time.NewTicker(pm.cleanupInterval)
	for {
		select {
		case np := <-pm.newprovs:
			err := pm.addProv(np.k, np.val)
			if err != nil {
				log.Error("error adding new providers: ", err)
			}
		case gp := <-pm.getprovs:
			provs, err := pm.providersForKey(gp.k)
			if err != nil && err != ds.ErrNotFound {
				log.Error("error reading providers: ", err)
			}

			gp.resp <- provs
		case <-tick.C:
			keys, err := pm.getProvKeys()
			if err != nil {
				log.Error("Error loading provider keys: ", err)
				continue
			}
			for {
				k, ok := keys()
				if !ok {
					break
				}

				provs, err := pm.getProvSet(k)
				if err != nil {
					log.Error("error loading known provset: ", err)
					continue
				}
				var filtered []peer.ID
				for p, t := range provs.set {
					if time.Now().Sub(t) > ProvideValidity {
						delete(provs.set, p)
					} else {
						filtered = append(filtered, p)
					}
				}

				provs.providers = filtered
				if len(filtered) == 0 {
					err := pm.deleteProvSet(k)
					if err != nil {
						log.Error("error deleting provider set: ", err)
					}
				}
			}
		case <-pm.proc.Closing():
			tick.Stop()
			return
		}
	}
}

func (pm *ProviderManager) AddProvider(ctx context.Context, k *cid.Cid, val peer.ID) {
	prov := &addProv{
		k:   k,
		val: val,
	}
	select {
	case pm.newprovs <- prov:
	case <-ctx.Done():
	}
}

func (pm *ProviderManager) GetProviders(ctx context.Context, k *cid.Cid) []peer.ID {
	gp := &getProv{
		k:    k,
		resp: make(chan []peer.ID, 1), // buffered to prevent sender from blocking
	}
	select {
	case <-ctx.Done():
		return nil
	case pm.getprovs <- gp:
	}
	select {
	case <-ctx.Done():
		return nil
	case peers := <-gp.resp:
		return peers
	}
}

func newProviderSet() *providerSet {
	return &providerSet{
		set: make(map[peer.ID]time.Time),
	}
}

func (ps *providerSet) Add(p peer.ID) {
	ps.setVal(p, time.Now())
}

func (ps *providerSet) setVal(p peer.ID, t time.Time) {
	_, found := ps.set[p]
	if !found {
		ps.providers = append(ps.providers, p)
	}

	ps.set[p] = t
}
