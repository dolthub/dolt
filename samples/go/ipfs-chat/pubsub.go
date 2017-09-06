// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
	"github.com/ipfs/go-ipfs/core"
)

// MergeMessages continually listens for commit hashes published by ipfs-chat. It
// merges new messages into it's existing dataset when necessary and if an actual
// merge was necessary, it re-publishes the new commit.
func mergeMessages(node *core.IpfsNode, topic string, ds datas.Dataset, didChange func(ds datas.Dataset)) {
	sub, err := node.Floodsub.Subscribe(topic)
	d.Chk.NoError(err)

	var lastHash hash.Hash
	for {
		dbg.Debug("looking for msgs")
		msg, err := sub.Next(context.Background())
		d.PanicIfError(err)
		hstring := strings.TrimSpace(string(msg.Data))
		h, ok := hash.MaybeParse(hstring)
		if !ok {
			dbg.Debug("MergeMsgs: received unknown msg: %s", hstring)
			continue
		}
		if lastHash == h {
			continue
		}
		lastHash = h

		dbg.Debug("got update: %s from %s", h, base64.StdEncoding.EncodeToString(msg.From))
		db := ds.Database()
		db.Rebase()

		ds = db.GetDataset(ds.ID())
		d.PanicIfFalse(ds.HasHead())

		if h == ds.HeadRef().TargetHash() {
			dbg.Debug("received hash same as current head, nothing to do")
			continue
		}

		sourceCommit := db.ReadValue(h)
		sourceRef := types.NewRef(sourceCommit)
		a, ok := datas.FindCommonAncestor(sourceRef, ds.HeadRef(), db)
		if !ok {
			dbg.Debug("no common ancestor, cannot merge update!")
			continue
		}
		if a.Equals(sourceRef) {
			dbg.Debug("source commit was ancestor, nothing to do")
			continue
		}
		if a.Equals(ds.HeadRef()) {
			dbg.Debug("fast-forward to source commit")
			ds, err = db.SetHead(ds, sourceRef)
			didChange(ds)
			continue
		}

		dbg.Debug("Merging new messages into existing data")
		// we have a mergeable difference
		left := ds.HeadValue()
		right := sourceCommit.(types.Struct).Get("value")
		parent := a.TargetValue(db).(types.Struct).Get("value")

		merged, err := merge.ThreeWay(left, right, parent, db, nil, nil)
		if err != nil {
			dbg.Debug("could not merge received data: " + err.Error())
			continue
		}

		newCommit := datas.NewCommit(merged, types.NewSet(db, ds.HeadRef(), sourceRef), types.EmptyStruct)
		commitRef := db.WriteValue(newCommit)
		dbg.Debug("wrote new commit: %s", commitRef.TargetHash())
		ds, err = db.SetHead(ds, commitRef)
		if err != nil {
			dbg.Debug("call failed to SetHead on destDB, err: %s", err)
		}
		dbg.Debug("set new head ref: %s on ds.ID: %s", commitRef.TargetHash(), ds.ID())
		Publish(node, topic, commitRef.TargetHash())
		didChange(ds)
	}
}

func Publish(node *core.IpfsNode, topic string, h hash.Hash) {
	dbg.Debug("publishing to topic: %s, hash: %s", topic, h)
	node.Floodsub.Publish(topic, []byte(h.String()+"\r\n"))
}
