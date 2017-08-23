package mcpacked

import (
	"encoding/binary"
)

type Code uint64

const (
	Unknown     = Code(0)
	Git         = Code(0x69)
	DagProtobuf = Code(0x70)
	DagCBOR     = Code(0x71)
	Raw         = Code(0x55)
	RLP         = Code(0x60)
	Bencode     = Code(0x63)

	Multicodec = Code(0x30)
	Multihash  = Code(0x31)
	Multiaddr  = Code(0x32)
	Multibase  = Code(0x33)

	StellarBlock = Code(0xd0)
	StellarTx    = Code(0xd1)

	TorrentInfo = Code(0x7b)
	TorrentFile = Code(0x7c)

	EthBlock           = Code(0x90)
	EthBlockList       = Code(0x91)
	EthTxTrie          = Code(0x92)
	EthTx              = Code(0x93)
	EthTxReceiptTrie   = Code(0x94)
	EthTxReceipt       = Code(0x95)
	EthStateTrie       = Code(0x96)
	EthAccountSnapshot = Code(0x97)
	EthStorageTrie     = Code(0x98)

	BitcoinBlock = Code(0xb0)
	BitcoinTx    = Code(0xb1)
	ZcashBlock   = Code(0xc0)
	ZcashTx      = Code(0xc1)

	Ed25519Pub = Code(0xed)
)

func (c Code) String() string {
	return CodeToString(c)
}

const UnknownMulticodecString = "<Unknown Multicodec>"

func CodeToString(c Code) string {
	switch c {
	case Git:
		return "git"
	case DagProtobuf:
		return "dag-pb"
	case DagCBOR:
		return "dag-cbor"
	case Raw:
		return "bin"
	case RLP:
		return "rlp"
	case Bencode:
		return "bencode"
	case Multicodec:
		return "multicodec"
	case Multihash:
		return "multihash"
	case Multiaddr:
		return "multiaddr"
	case Multibase:
		return "multibase"
	case BitcoinBlock:
		return "bitcoin-block"
	case BitcoinTx:
		return "bitcoin-tx"
	case EthBlock:
		return "eth-block"
	case EthBlockList:
		return "eth-block-list"
	case EthTxTrie:
		return "eth-tx-trie"
	case EthTx:
		return "eth-tx"
	case EthTxReceiptTrie:
		return "eth-tx-receipt-trie"
	case EthTxReceipt:
		return "eth-tx-receipt"
	case EthStateTrie:
		return "eth-state-trie"
	case EthAccountSnapshot:
		return "eth-account-snapshot"
	case EthStorageTrie:
		return "eth-storage-trie"
	case ZcashBlock:
		return "zcash-block"
	case ZcashTx:
		return "zcash-tx"
	case StellarBlock:
		return "stellar-block"
	case StellarTx:
		return "stellar-tx"
	case TorrentInfo:
		return "torrent-info"
	case TorrentFile:
		return "torrent-file"
	case Ed25519Pub:
		return "ed25519-pub"
	default:
		return UnknownMulticodecString
	}
}

func GetCode(data []byte) Code {
	c, _ := binary.Uvarint(data)
	return Code(c)
}

func AddPrefix(c Code, data []byte) []byte {
	buf := make([]byte, len(data)+binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(c))
	copy(buf[n:], data)
	return buf[:n+len(data)]
}

func SplitPrefix(data []byte) (Code, []byte) {
	c, n := binary.Uvarint(data)
	return Code(c), data[n:]
}
