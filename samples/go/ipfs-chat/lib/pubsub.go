// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
	"github.com/ipfs/go-ipfs/core"
	"github.com/jbenet/go-base58"
)

var (
	PubsubUser    = "default"
	seenHash      = map[hash.Hash]bool{}
	seenHashMutex = sync.Mutex{}
)

func lockSeenF() func() {
	seenHashMutex.Lock()
	return func() {
		seenHashMutex.Unlock()
	}
}

// RecieveMessages listens for messages sent by other chat nodes. It filters out
// any msgs that have already been received and adds events to teh events channel
// for any msgs that it hasn't seen yet.
func ReceiveMessages(node *core.IpfsNode, events chan ChatEvent, cInfo ClientInfo) {
	sub, err := node.Floodsub.Subscribe(cInfo.Topic)
	d.Chk.NoError(err)

	listenForAndHandleMessage := func() {
		msg, err := sub.Next(context.Background())
		d.PanicIfError(err)
		sender := base58.Encode(msg.From)
		msgMap := map[string]string{}
		err = json.Unmarshal(msg.Data, &msgMap)
		if err != nil {
			dbg.Debug("ReceiveMessages: received non-json msg: %s from: %s, error: %s", msg.Data, sender, err)
			return
		}
		msgData := msgMap["data"]
		h, err := cInfo.Delegate.HashFromMsgData(msgData)
		if err != nil {
			dbg.Debug("ReceiveMessages: received unknown msg: %s from: %s", msgData, sender)
			return
		}

		defer lockSeenF()()
		if !seenHash[h] {
			events <- ChatEvent{EventType: SyncEvent, Event: msgData}
			seenHash[h] = true
			dbg.Debug("got msgData: %s from: %s(%s)", msgData, sender, msgMap["user"])
		}
	}

	dbg.Debug("start listening for msgs on channel: %s", cInfo.Topic)
	for {
		listenForAndHandleMessage()
	}
	panic("unreachable")
}

// Publish asks the delegate to format a hash/ClientInfo into a suitable msg
// and publishes that using IPFS pubsub.
func Publish(node *core.IpfsNode, cInfo ClientInfo, h hash.Hash) {
    defer func() {
        if r := recover(); r != nil {
            dbg.Debug("Publish failed, error: %s", r)
        }
    }()
	msgData := cInfo.Delegate.GenMessageData(cInfo, h)
	m, err := json.Marshal(map[string]string{"user": cInfo.Username, "data": msgData})
	if err != nil {

	}
	d.PanicIfError(err)
	dbg.Debug("publishing to topic: %s, msg: %s", cInfo.Topic, m)
	node.Floodsub.Publish(cInfo.Topic, append(m, []byte("\r\n")...))

	defer lockSeenF()()
	seenHash[h] = true
}
