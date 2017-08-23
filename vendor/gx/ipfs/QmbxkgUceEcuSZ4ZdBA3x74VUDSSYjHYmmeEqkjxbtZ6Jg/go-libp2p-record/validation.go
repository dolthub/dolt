package record

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
	pb "gx/ipfs/QmbxkgUceEcuSZ4ZdBA3x74VUDSSYjHYmmeEqkjxbtZ6Jg/go-libp2p-record/pb"
)

var log = logging.Logger("routing/record")

// ValidatorFunc is a function that is called to validate a given
// type of DHTRecord.
type ValidatorFunc func(string, []byte) error

// ErrBadRecord is returned any time a dht record is found to be
// incorrectly formatted or signed.
var ErrBadRecord = errors.New("bad dht record")

// ErrInvalidRecordType is returned if a DHTRecord keys prefix
// is not found in the Validator map of the DHT.
var ErrInvalidRecordType = errors.New("invalid record keytype")

// Validator is an object that helps ensure routing records are valid.
// It is a collection of validator functions, each of which implements
// its own notion of validity.
type Validator map[string]*ValidChecker

type ValidChecker struct {
	Func ValidatorFunc
	Sign bool
}

// VerifyRecord checks a record and ensures it is still valid.
// It runs needed validators
func (v Validator) VerifyRecord(r *pb.Record) error {
	// Now, check validity func
	parts := strings.Split(r.GetKey(), "/")
	if len(parts) < 3 {
		log.Infof("Record key does not have validator: %s", r.GetKey())
		return nil
	}

	val, ok := v[parts[1]]
	if !ok {
		log.Infof("Unrecognized key prefix: %s", parts[1])
		return ErrInvalidRecordType
	}

	return val.Func(r.GetKey(), r.GetValue())
}

func (v Validator) IsSigned(k string) (bool, error) {
	// Now, check validity func
	parts := strings.Split(k, "/")
	if len(parts) < 3 {
		log.Infof("Record key does not have validator: %s", k)
		return false, nil
	}

	val, ok := v[parts[1]]
	if !ok {
		log.Infof("Unrecognized key prefix: %s", parts[1])
		return false, ErrInvalidRecordType
	}

	return val.Sign, nil
}

// ValidatePublicKeyRecord implements ValidatorFunc and
// verifies that the passed in record value is the PublicKey
// that matches the passed in key.
func ValidatePublicKeyRecord(k string, val []byte) error {
	if len(k) < 5 {
		return errors.New("invalid public key record key")
	}

	prefix := k[:4]
	if prefix != "/pk/" {
		return errors.New("key was not prefixed with /pk/")
	}

	keyhash := []byte(k[4:])
	if _, err := mh.Cast(keyhash); err != nil {
		return fmt.Errorf("key did not contain valid multihash: %s", err)
	}

	pkh := u.Hash(val)
	if !bytes.Equal(keyhash, pkh) {
		return errors.New("public key does not match storage key")
	}
	return nil
}

var PublicKeyValidator = &ValidChecker{
	Func: ValidatePublicKeyRecord,
	Sign: false,
}

func CheckRecordSig(r *pb.Record, pk ci.PubKey) error {
	blob := RecordBlobForSig(r)
	good, err := pk.Verify(blob, r.Signature)
	if err != nil {
		return nil
	}
	if !good {
		return errors.New("invalid record signature")
	}
	return nil
}
