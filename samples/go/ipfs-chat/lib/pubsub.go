// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
	"github.com/ipfs/go-ipfs/core"
)

// MergeMessages continually listens for commit hashes published by ipfs-chat. It
// merges new messages into it's existing dataset when necessary and if an actual
// merge was necessary, it re-publishes the new commit.
func MergeMessages(node *core.IpfsNode, topic string, sinkSp spec.Spec, sinkDS datas.Dataset, didChange func(nds datas.Dataset)) {
	sub, err := node.Floodsub.Subscribe(topic)
	d.Chk.NoError(err)

	var lastHash hash.Hash
	for {
		dbg.Debug("looking for msgs")
		msg, err := sub.Next(context.Background())
		d.PanicIfError(err)
		sp, err := spec.ForPath(strings.TrimSpace(string(msg.Data)))
		d.PanicIfError(err)

		dbg.Debug("got update: %s from %s", sp.String(), base64.StdEncoding.EncodeToString(msg.From))
		if sp.Path.Hash.IsEmpty() {
			d.Panic("notreached")
		}

		if sp.Path.Hash == lastHash {
			dbg.Debug("same hash as last time")
			continue
		}

		// argh, so ghetto
		sinkDB := sinkDS.Database()
		sinkDB.Rebase()
		sinkDS = sinkDB.GetDataset(sinkDS.ID())
		d.PanicIfFalse(sinkDS.HasHead())

		dbg.Debug("current head hash is: " + sinkDS.HeadRef().TargetHash().String())
		if sp.Path.Hash == sinkDS.HeadRef().TargetHash() {
			dbg.Debug("received hash same as current head, nothing to do")
			continue
		}

		sourceCommit := sp.GetValue()
		dbg.Debug(fmt.Sprintf("source commit hash is: %+v", sourceCommit.Hash()))
		sourceRef := types.NewRef(sourceCommit)
		datas.Pull(sp.GetDatabase(), sinkDB, sourceRef, nil)
		a, ok := datas.FindCommonAncestor(sourceRef, sinkDS.HeadRef(), sinkDB)
		if !ok {
			dbg.Debug("no common ancestor, cannot merge update!")
			continue
		}
		if a.Equals(sourceRef) {
			dbg.Debug("source commit was ancestor, nothing to do")
			continue
		}
		if a.Equals(sinkDS.HeadRef()) {
			dbg.Debug("fast-forward to source commit")
			sinkDS, err = sinkDB.SetHead(sinkDS, sourceRef)
			didChange(sinkDS)
			continue
		}

		dbg.Debug("Merging new messages into existing data")
		// we have a mergeable difference
		left := sinkDS.HeadValue()
		right := sourceCommit.(types.Struct).Get("value")
		parent := a.TargetValue(sinkDB).(types.Struct).Get("value")

		merged, err := merge.ThreeWay(left, right, parent, sinkDB, nil, nil)
		if err != nil {
			dbg.Debug("could not merge received data: " + err.Error())
			continue
		}

		newCommit := datas.NewCommit(merged, types.NewSet(sinkDB, sinkDS.HeadRef(), sourceRef), types.EmptyStruct)
		commitRef := sinkDB.WriteValue(newCommit)
		dbg.Debug("wrote new commit: %s", commitRef.TargetHash())
		sinkDS, err = sinkDB.SetHead(sinkDS, commitRef)
		if err != nil {
			dbg.Debug("call failed to SetHead on destDB, err: %s", err)
		}
		dbg.Debug("set new head ref: %s on ds.ID: %s", commitRef.TargetHash(), sinkDS.ID())
		Publish(node, topic, sinkSp, commitRef.TargetHash())
		didChange(sinkDS)
	}
}

func Publish(node *core.IpfsNode, topic string, sp spec.Spec, h hash.Hash) {
	st := fmt.Sprintf("%s::#%s", sp.String(), h.String())
	dbg.Debug("publishing to topic: %s, hash: %s", topic, st)
	node.Floodsub.Publish(topic, []byte(st+"\r\n"))
}
