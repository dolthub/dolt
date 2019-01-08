package republisher

import (
	"context"
	"errors"
	"time"

	keystore "github.com/ipfs/go-ipfs/keystore"
	namesys "github.com/ipfs/go-ipfs/namesys"
	path "gx/ipfs/QmT3rzed1ppXefourpmoZ7tyVQfsGPQZ1pHDngLmCvXxd3/go-path"

	ic "gx/ipfs/QmPvyPwuCgJ7pDmrKDxRtsScJgBaM5h4EpRL2qQJsmXf4n/go-libp2p-crypto"
	goprocess "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
	gpctx "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess/context"
	peer "gx/ipfs/QmTRhk7cgjUf2gfQ3p2M9KPECNZEW9XUrmHcFCgog4cPgB/go-libp2p-peer"
	logging "gx/ipfs/QmZChCsSt8DctjceaL56Eibc29CVQq4dGKRXC5JRZ6Ppae/go-log"
	pb "gx/ipfs/QmaRFtZhVAwXBk4Z3zEsvjScH9fjsDZmhXfa1Gm8eMb9cg/go-ipns/pb"
	ds "gx/ipfs/QmaRb5yNXKonhbkpNxNawoydk4N6es6b4fPj19sjEKsh5D/go-datastore"
	proto "gx/ipfs/QmdxUuburamoF6zF9qjeQC4WYcWGbWuRmdLacMEsW8ioD8/gogo-protobuf/proto"
)

var errNoEntry = errors.New("no previous entry")

var log = logging.Logger("ipns-repub")

// DefaultRebroadcastInterval is the default interval at which we rebroadcast IPNS records
var DefaultRebroadcastInterval = time.Hour * 4

// InitialRebroadcastDelay is the delay before first broadcasting IPNS records on start
var InitialRebroadcastDelay = time.Minute * 1

// FailureRetryInterval is the interval at which we retry IPNS records broadcasts (when they fail)
var FailureRetryInterval = time.Minute * 5

// DefaultRecordLifetime is the default lifetime for IPNS records
const DefaultRecordLifetime = time.Hour * 24

type Republisher struct {
	ns   namesys.Publisher
	ds   ds.Datastore
	self ic.PrivKey
	ks   keystore.Keystore

	Interval time.Duration

	// how long records that are republished should be valid for
	RecordLifetime time.Duration
}

// NewRepublisher creates a new Republisher
func NewRepublisher(ns namesys.Publisher, ds ds.Datastore, self ic.PrivKey, ks keystore.Keystore) *Republisher {
	return &Republisher{
		ns:             ns,
		ds:             ds,
		self:           self,
		ks:             ks,
		Interval:       DefaultRebroadcastInterval,
		RecordLifetime: DefaultRecordLifetime,
	}
}

func (rp *Republisher) Run(proc goprocess.Process) {
	timer := time.NewTimer(InitialRebroadcastDelay)
	defer timer.Stop()
	if rp.Interval < InitialRebroadcastDelay {
		timer.Reset(rp.Interval)
	}

	for {
		select {
		case <-timer.C:
			timer.Reset(rp.Interval)
			err := rp.republishEntries(proc)
			if err != nil {
				log.Info("republisher failed to republish: ", err)
				if FailureRetryInterval < rp.Interval {
					timer.Reset(FailureRetryInterval)
				}
			}
		case <-proc.Closing():
			return
		}
	}
}

func (rp *Republisher) republishEntries(p goprocess.Process) error {
	ctx, cancel := context.WithCancel(gpctx.OnClosingContext(p))
	defer cancel()

	// TODO: Use rp.ipns.ListPublished(). We can't currently *do* that
	// because:
	// 1. There's no way to get keys from the keystore by ID.
	// 2. We don't actually have access to the IPNS publisher.
	err := rp.republishEntry(ctx, rp.self)
	if err != nil {
		return err
	}

	if rp.ks != nil {
		keyNames, err := rp.ks.List()
		if err != nil {
			return err
		}
		for _, name := range keyNames {
			priv, err := rp.ks.Get(name)
			if err != nil {
				return err
			}
			err = rp.republishEntry(ctx, priv)
			if err != nil {
				return err
			}

		}
	}

	return nil
}

func (rp *Republisher) republishEntry(ctx context.Context, priv ic.PrivKey) error {
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return err
	}

	log.Debugf("republishing ipns entry for %s", id)

	// Look for it locally only
	p, err := rp.getLastVal(id)
	if err != nil {
		if err == errNoEntry {
			return nil
		}
		return err
	}

	// update record with same sequence number
	eol := time.Now().Add(rp.RecordLifetime)
	return rp.ns.PublishWithEOL(ctx, priv, p, eol)
}

func (rp *Republisher) getLastVal(id peer.ID) (path.Path, error) {
	// Look for it locally only
	val, err := rp.ds.Get(namesys.IpnsDsKey(id))
	switch err {
	case nil:
	case ds.ErrNotFound:
		return "", errNoEntry
	default:
		return "", err
	}

	e := new(pb.IpnsEntry)
	if err := proto.Unmarshal(val, e); err != nil {
		return "", err
	}
	return path.Path(e.Value), nil
}
