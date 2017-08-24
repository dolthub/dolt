// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"context"
	"encoding/base64"

	floodsub "gx/ipfs/QmZdsQf8BiCpAj61nz9NgqVeRUkw9vATvCs7UHFTxoUMDb/floodsub"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
)

func Replicate(sub *floodsub.Subscription, source, dest datas.Dataset, didChange func(ds datas.Dataset)) {
	for {
		dbg.Debug("looking for msgs")
		msg, err := sub.Next(context.Background())
		d.PanicIfError(err)
		h := hash.Parse(string(msg.Data))
		dbg.Debug("got update: %s from %s", h.String(), base64.StdEncoding.EncodeToString(msg.From))

		destDB := dest.Database()
		destDB.Rebase()
		dest = destDB.GetDataset(dest.ID())
		d.PanicIfFalse(dest.HasHead())

		source.Database().Rebase()
		r := types.NewRef(source.Database().ReadValue(h))
		datas.Pull(source.Database(), destDB, r, nil)

		if h == dest.HeadRef().TargetHash() {
			dbg.Debug("received hash same as current head, nothing to do")
			continue
		}
		sourceCommit := destDB.ReadValue(h)
		sourceRef := types.NewRef(sourceCommit)
		a, ok := datas.FindCommonAncestor(sourceRef, dest.HeadRef(), destDB)
		if !ok {
			dbg.Debug("no common ancestor, cannot merge update!")
			continue
		}
		if a.Equals(sourceRef) {
			dbg.Debug("source commit was ancestor, nothing to do")
			continue
		}
		if a.Equals(dest.HeadRef()) {
			dbg.Debug("fast-forward to source commit")
			dest, err = destDB.SetHead(dest, sourceRef)
			didChange(dest)
			continue
		}

		left := dest.HeadValue()
		right := sourceCommit.(types.Struct).Get("value")
		parent := a.TargetValue(destDB).(types.Struct).Get("value")

		merged, err := merge.ThreeWay(left, right, parent, destDB, nil, nil)
		if err != nil {
			dbg.Debug("could not merge received data: " + err.Error())
			continue
		}

		dest, err = destDB.SetHead(dest, destDB.WriteValue(datas.NewCommit(merged, types.NewSet(dest.HeadRef(), sourceRef), types.EmptyStruct)))
		if err != nil {
			dbg.Debug("call failed to SetHead on destDB, err: %s", err)
		}
		didChange(dest)
	}
}

func Publish(sub *floodsub.Subscription, topic string, s string) {
	ipfs.CurrentNode.Floodsub.Publish(topic, []byte(s))
}
