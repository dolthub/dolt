package muxcodec

import (
	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
	cbor "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/cbor"
	json "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/json"
)

func StandardMux() *Multicodec {
	return MuxMulticodec([]mc.Multicodec{
		cbor.Multicodec(),
		json.Multicodec(false),
		json.Multicodec(true),
	}, SelectFirst)
}
