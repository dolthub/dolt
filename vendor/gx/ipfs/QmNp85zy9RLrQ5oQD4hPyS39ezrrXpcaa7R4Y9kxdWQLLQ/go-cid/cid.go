// Package cid implements the Content-IDentifiers specification
// (https://github.com/ipld/cid) in Go. CIDs are
// self-describing content-addressed identifiers useful for
// distributed information systems. CIDs are used in the IPFS
// (https://ipfs.io) project ecosystem.
//
// CIDs have two major versions. A CIDv0 corresponds to a multihash of type
// DagProtobuf, is deprecated and exists for compatibility reasons. Usually,
// CIDv1 should be used.
//
// A CIDv1 has four parts:
//
//     <cidv1> ::= <multibase-prefix><cid-version><multicodec-packed-content-type><multihash-content-address>
//
// As shown above, the CID implementation relies heavily on Multiformats,
// particularly Multibase
// (https://github.com/multiformats/go-multibase), Multicodec
// (https://github.com/multiformats/multicodec) and Multihash
// implementations (https://github.com/multiformats/go-multihash).
package cid

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	mbase "gx/ipfs/QmafgXF3u3QSWErQoZ2URmQp5PFG384htoE7J338nS2H7T/go-multibase"
)

// UnsupportedVersionString just holds an error message
const UnsupportedVersionString = "<unsupported cid version>"

var (
	// ErrVarintBuffSmall means that a buffer passed to the cid parser was not
	// long enough, or did not contain an invalid cid
	ErrVarintBuffSmall = errors.New("reading varint: buffer too small")

	// ErrVarintTooBig means that the varint in the given cid was above the
	// limit of 2^64
	ErrVarintTooBig = errors.New("reading varint: varint bigger than 64bits" +
		" and not supported")

	// ErrCidTooShort means that the cid passed to decode was not long
	// enough to be a valid Cid
	ErrCidTooShort = errors.New("cid too short")

	// ErrInvalidEncoding means that selected encoding is not supported
	// by this Cid version
	ErrInvalidEncoding = errors.New("invalid base encoding")
)

// These are multicodec-packed content types. The should match
// the codes described in the authoritative document:
// https://github.com/multiformats/multicodec/blob/master/table.csv
const (
	Raw = 0x55

	DagProtobuf = 0x70
	DagCBOR     = 0x71

	GitRaw = 0x78

	EthBlock           = 0x90
	EthBlockList       = 0x91
	EthTxTrie          = 0x92
	EthTx              = 0x93
	EthTxReceiptTrie   = 0x94
	EthTxReceipt       = 0x95
	EthStateTrie       = 0x96
	EthAccountSnapshot = 0x97
	EthStorageTrie     = 0x98
	BitcoinBlock       = 0xb0
	BitcoinTx          = 0xb1
	ZcashBlock         = 0xc0
	ZcashTx            = 0xc1
)

// Codecs maps the name of a codec to its type
var Codecs = map[string]uint64{
	"v0":                   DagProtobuf,
	"raw":                  Raw,
	"protobuf":             DagProtobuf,
	"cbor":                 DagCBOR,
	"git-raw":              GitRaw,
	"eth-block":            EthBlock,
	"eth-block-list":       EthBlockList,
	"eth-tx-trie":          EthTxTrie,
	"eth-tx":               EthTx,
	"eth-tx-receipt-trie":  EthTxReceiptTrie,
	"eth-tx-receipt":       EthTxReceipt,
	"eth-state-trie":       EthStateTrie,
	"eth-account-snapshot": EthAccountSnapshot,
	"eth-storage-trie":     EthStorageTrie,
	"bitcoin-block":        BitcoinBlock,
	"bitcoin-tx":           BitcoinTx,
	"zcash-block":          ZcashBlock,
	"zcash-tx":             ZcashTx,
}

// CodecToStr maps the numeric codec to its name
var CodecToStr = map[uint64]string{
	Raw:                "raw",
	DagProtobuf:        "protobuf",
	DagCBOR:            "cbor",
	GitRaw:             "git-raw",
	EthBlock:           "eth-block",
	EthBlockList:       "eth-block-list",
	EthTxTrie:          "eth-tx-trie",
	EthTx:              "eth-tx",
	EthTxReceiptTrie:   "eth-tx-receipt-trie",
	EthTxReceipt:       "eth-tx-receipt",
	EthStateTrie:       "eth-state-trie",
	EthAccountSnapshot: "eth-account-snapshot",
	EthStorageTrie:     "eth-storage-trie",
	BitcoinBlock:       "bitcoin-block",
	BitcoinTx:          "bitcoin-tx",
	ZcashBlock:         "zcash-block",
	ZcashTx:            "zcash-tx",
}

// NewCidV0 returns a Cid-wrapped multihash.
// They exist to allow IPFS to work with Cids while keeping
// compatibility with the plain-multihash format used used in IPFS.
// NewCidV1 should be used preferentially.
func NewCidV0(mhash mh.Multihash) *Cid {
	return &Cid{
		version: 0,
		codec:   DagProtobuf,
		hash:    mhash,
	}
}

// NewCidV1 returns a new Cid using the given multicodec-packed
// content type.
func NewCidV1(codecType uint64, mhash mh.Multihash) *Cid {
	return &Cid{
		version: 1,
		codec:   codecType,
		hash:    mhash,
	}
}

// Cid represents a self-describing content adressed
// identifier. It is formed by a Version, a Codec (which indicates
// a multicodec-packed content type) and a Multihash.
type Cid struct {
	version uint64
	codec   uint64
	hash    mh.Multihash
}

// Parse is a short-hand function to perform Decode, Cast etc... on
// a generic interface{} type.
func Parse(v interface{}) (*Cid, error) {
	switch v2 := v.(type) {
	case string:
		if strings.Contains(v2, "/ipfs/") {
			return Decode(strings.Split(v2, "/ipfs/")[1])
		}
		return Decode(v2)
	case []byte:
		return Cast(v2)
	case mh.Multihash:
		return NewCidV0(v2), nil
	case *Cid:
		return v2, nil
	default:
		return nil, fmt.Errorf("can't parse %+v as Cid", v2)
	}
}

// Decode parses a Cid-encoded string and returns a Cid object.
// For CidV1, a Cid-encoded string is primarily a multibase string:
//
//     <multibase-type-code><base-encoded-string>
//
// The base-encoded string represents a:
//
// <version><codec-type><multihash>
//
// Decode will also detect and parse CidV0 strings. Strings
// starting with "Qm" are considered CidV0 and treated directly
// as B58-encoded multihashes.
func Decode(v string) (*Cid, error) {
	if len(v) < 2 {
		return nil, ErrCidTooShort
	}

	if len(v) == 46 && v[:2] == "Qm" {
		hash, err := mh.FromB58String(v)
		if err != nil {
			return nil, err
		}

		return NewCidV0(hash), nil
	}

	_, data, err := mbase.Decode(v)
	if err != nil {
		return nil, err
	}

	return Cast(data)
}

func uvError(read int) error {
	switch {
	case read == 0:
		return ErrVarintBuffSmall
	case read < 0:
		return ErrVarintTooBig
	default:
		return nil
	}
}

// Cast takes a Cid data slice, parses it and returns a Cid.
// For CidV1, the data buffer is in the form:
//
//     <version><codec-type><multihash>
//
// CidV0 are also supported. In particular, data buffers starting
// with length 34 bytes, which starts with bytes [18,32...] are considered
// binary multihashes.
//
// Please use decode when parsing a regular Cid string, as Cast does not
// expect multibase-encoded data. Cast accepts the output of Cid.Bytes().
func Cast(data []byte) (*Cid, error) {
	if len(data) == 34 && data[0] == 18 && data[1] == 32 {
		h, err := mh.Cast(data)
		if err != nil {
			return nil, err
		}

		return &Cid{
			codec:   DagProtobuf,
			version: 0,
			hash:    h,
		}, nil
	}

	vers, n := binary.Uvarint(data)
	if err := uvError(n); err != nil {
		return nil, err
	}

	if vers != 0 && vers != 1 {
		return nil, fmt.Errorf("invalid cid version number: %d", vers)
	}

	codec, cn := binary.Uvarint(data[n:])
	if err := uvError(cn); err != nil {
		return nil, err
	}

	rest := data[n+cn:]
	h, err := mh.Cast(rest)
	if err != nil {
		return nil, err
	}

	return &Cid{
		version: vers,
		codec:   codec,
		hash:    h,
	}, nil
}

// Type returns the multicodec-packed content type of a Cid.
func (c *Cid) Type() uint64 {
	return c.codec
}

// String returns the default string representation of a
// Cid. Currently, Base58 is used as the encoding for the
// multibase string.
func (c *Cid) String() string {
	switch c.version {
	case 0:
		return c.hash.B58String()
	case 1:
		mbstr, err := mbase.Encode(mbase.Base58BTC, c.bytesV1())
		if err != nil {
			panic("should not error with hardcoded mbase: " + err.Error())
		}

		return mbstr
	default:
		panic("not possible to reach this point")
	}
}

// String returns the string representation of a Cid
// encoded is selected base
func (c *Cid) StringOfBase(base mbase.Encoding) (string, error) {
	switch c.version {
	case 0:
		if base != mbase.Base58BTC {
			return "", ErrInvalidEncoding
		}
		return c.hash.B58String(), nil
	case 1:
		return mbase.Encode(base, c.bytesV1())
	default:
		panic("not possible to reach this point")
	}
}

// Hash returns the multihash contained by a Cid.
func (c *Cid) Hash() mh.Multihash {
	return c.hash
}

// Bytes returns the byte representation of a Cid.
// The output of bytes can be parsed back into a Cid
// with Cast().
func (c *Cid) Bytes() []byte {
	switch c.version {
	case 0:
		return c.bytesV0()
	case 1:
		return c.bytesV1()
	default:
		panic("not possible to reach this point")
	}
}

func (c *Cid) bytesV0() []byte {
	return []byte(c.hash)
}

func (c *Cid) bytesV1() []byte {
	// two 8 bytes (max) numbers plus hash
	buf := make([]byte, 2*binary.MaxVarintLen64+len(c.hash))
	n := binary.PutUvarint(buf, c.version)
	n += binary.PutUvarint(buf[n:], c.codec)
	cn := copy(buf[n:], c.hash)
	if cn != len(c.hash) {
		panic("copy hash length is inconsistent")
	}

	return buf[:n+len(c.hash)]
}

// Equals checks that two Cids are the same.
// In order for two Cids to be considered equal, the
// Version, the Codec and the Multihash must match.
func (c *Cid) Equals(o *Cid) bool {
	return c.codec == o.codec &&
		c.version == o.version &&
		bytes.Equal(c.hash, o.hash)
}

// UnmarshalJSON parses the JSON representation of a Cid.
func (c *Cid) UnmarshalJSON(b []byte) error {
	if len(b) < 2 {
		return fmt.Errorf("invalid cid json blob")
	}
	obj := struct {
		CidTarget string `json:"/"`
	}{}
	err := json.Unmarshal(b, &obj)
	if err != nil {
		return err
	}

	if obj.CidTarget == "" {
		return fmt.Errorf("cid was incorrectly formatted")
	}

	out, err := Decode(obj.CidTarget)
	if err != nil {
		return err
	}

	c.version = out.version
	c.hash = out.hash
	c.codec = out.codec
	return nil
}

// MarshalJSON procudes a JSON representation of a Cid, which looks as follows:
//
//    { "/": "<cid-string>" }
//
// Note that this formatting comes from the IPLD specification
// (https://github.com/ipld/specs/tree/master/ipld)
func (c *Cid) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("{\"/\":\"%s\"}", c.String())), nil
}

// KeyString casts the result of cid.Bytes() as a string, and returns it.
func (c *Cid) KeyString() string {
	return string(c.Bytes())
}

// Loggable returns a Loggable (as defined by
// https://godoc.org/github.com/ipfs/go-log).
func (c *Cid) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"cid": c,
	}
}

// Prefix builds and returns a Prefix out of a Cid.
func (c *Cid) Prefix() Prefix {
	dec, _ := mh.Decode(c.hash) // assuming we got a valid multiaddr, this will not error
	return Prefix{
		MhType:   dec.Code,
		MhLength: dec.Length,
		Version:  c.version,
		Codec:    c.codec,
	}
}

// Prefix represents all the metadata of a Cid,
// that is, the Version, the Codec, the Multihash type
// and the Multihash length. It does not contains
// any actual content information.
type Prefix struct {
	Version  uint64
	Codec    uint64
	MhType   uint64
	MhLength int
}

// Sum uses the information in a prefix to perform a multihash.Sum()
// and return a newly constructed Cid with the resulting multihash.
func (p Prefix) Sum(data []byte) (*Cid, error) {
	hash, err := mh.Sum(data, p.MhType, p.MhLength)
	if err != nil {
		return nil, err
	}

	switch p.Version {
	case 0:
		return NewCidV0(hash), nil
	case 1:
		return NewCidV1(p.Codec, hash), nil
	default:
		return nil, fmt.Errorf("invalid cid version")
	}
}

// Bytes returns a byte representation of a Prefix. It looks like:
//
//     <version><codec><mh-type><mh-length>
func (p Prefix) Bytes() []byte {
	buf := make([]byte, 4*binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, p.Version)
	n += binary.PutUvarint(buf[n:], p.Codec)
	n += binary.PutUvarint(buf[n:], uint64(p.MhType))
	n += binary.PutUvarint(buf[n:], uint64(p.MhLength))
	return buf[:n]
}

// PrefixFromBytes parses a Prefix-byte representation onto a
// Prefix.
func PrefixFromBytes(buf []byte) (Prefix, error) {
	r := bytes.NewReader(buf)
	vers, err := binary.ReadUvarint(r)
	if err != nil {
		return Prefix{}, err
	}

	codec, err := binary.ReadUvarint(r)
	if err != nil {
		return Prefix{}, err
	}

	mhtype, err := binary.ReadUvarint(r)
	if err != nil {
		return Prefix{}, err
	}

	mhlen, err := binary.ReadUvarint(r)
	if err != nil {
		return Prefix{}, err
	}

	return Prefix{
		Version:  vers,
		Codec:    codec,
		MhType:   mhtype,
		MhLength: int(mhlen),
	}, nil
}
