// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"context"
	"fmt"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/math"
	"github.com/attic-labs/noms/samples/go/decent/dbg"
	"github.com/ipfs/go-ipfs/core"
)

const (
	InputEvent  ChatEventType = "input"
	SearchEvent ChatEventType = "search"
	SyncEvent   ChatEventType = "sync"
	QuitEvent   ChatEventType = "quit"
)

type ClientInfo struct {
	Topic    string
	Username string
	Interval time.Duration
	Idx      int
	IsDaemon bool
	Dir      string
	Spec     spec.Spec
	Delegate EventDelegate
}

type ChatEventType string

type ChatEvent struct {
	EventType ChatEventType
	Event     string
}

type EventDelegate interface {
	PinBlocks(node *core.IpfsNode, sourceDB, sinkDB datas.Database, sourceCommit types.Value)
	SourceCommitFromMsgData(db datas.Database, msgData string) (datas.Database, types.Value)
	HashFromMsgData(msgData string) (hash.Hash, error)
	GenMessageData(cInfo ClientInfo, h hash.Hash) string
}

// ProcessChatEvent reads events from the event channel and processes them
// sequentially. Is ClientInfo.IsDaemon is true, it also publishes the current
// head of the dataset continously.
func ProcessChatEvents(node *core.IpfsNode, ds datas.Dataset, events chan ChatEvent, t *TermUI, cInfo ClientInfo) {
	stopChan := make(chan struct{})
	if cInfo.IsDaemon {
		go func() {
			tickChan := time.NewTicker(cInfo.Interval).C
			for {
				select {
				case <-stopChan:
					break
				case <-tickChan:
					Publish(node, cInfo, ds.HeadRef().TargetHash())
				}
			}
		}()
	}

	for event := range events {
		switch event.EventType {
		case SyncEvent:
			ds = processHash(t, node, ds, event.Event, cInfo)
			Publish(node, cInfo, ds.HeadRef().TargetHash())
		case InputEvent:
			ds = processInput(t, node, ds, event.Event, cInfo)
			Publish(node, cInfo, ds.HeadRef().TargetHash())
		case SearchEvent:
			processSearch(t, node, ds, event.Event, cInfo)
		case QuitEvent:
			dbg.Debug("QuitEvent received, stopping program")
			stopChan <- struct{}{}
			return
		}
	}
}

// processHash processes msgs published by other chat nodes and does the work to
// integrate new data into this nodes local database and display it as needed.
func processHash(t *TermUI, node *core.IpfsNode, ds datas.Dataset, msgData string, cInfo ClientInfo) datas.Dataset {
	h, err := cInfo.Delegate.HashFromMsgData(msgData)
	d.PanicIfError(err)
	defer dbg.BoxF("processHash, msgData: %s, hash: %s, cid: %s", msgData, h, ipfs.NomsHashToCID(h))()

	sinkDB := ds.Database()
	d.PanicIfFalse(ds.HasHead())

	headRef := ds.HeadRef()
	if h == headRef.TargetHash() {
		dbg.Debug("received hash same as current head, nothing to do")
		return ds
	}

	dbg.Debug("reading value for hash: %s", h)
	sourceDB, sourceCommit := cInfo.Delegate.SourceCommitFromMsgData(sinkDB, msgData)
	if sourceCommit == nil {
		dbg.Debug("FAILED to read value for hash: %s", h)
		return ds
	}

	sourceRef := types.NewRef(sourceCommit)

	_, isP2P := cInfo.Delegate.(P2PEventDelegate)
	if cInfo.IsDaemon || isP2P {
		cInfo.Delegate.PinBlocks(node, sourceDB, sinkDB, sourceCommit)
	}

	dbg.Debug("Finding common ancestor for merge, sourceRef: %s, headRef: %s", sourceRef.TargetHash(), headRef.TargetHash())
	a, ok := datas.FindCommonAncestor(sourceRef, headRef, sinkDB)
	if !ok {
		dbg.Debug("no common ancestor, cannot merge update!")
		return ds
	}
	dbg.Debug("Checking if source commit is ancestor")
	if a.Equals(sourceRef) {
		dbg.Debug("source commit was ancestor, nothing to do")
		return ds
	}
	if a.Equals(headRef) {
		dbg.Debug("fast-forward to source commit")
		ds, err := sinkDB.SetHead(ds, sourceRef)
		d.Chk.NoError(err)
		if !cInfo.IsDaemon {
			t.UpdateMessagesFromSync(ds)
		}
		return ds
	}

	dbg.Debug("We have a mergeable commit")
	left := ds.HeadValue()
	right := sourceCommit.(types.Struct).Get("value")
	parent := a.TargetValue(sinkDB).(types.Struct).Get("value")

	dbg.Debug("Starting three-way commit")
	merged, err := merge.ThreeWay(left, right, parent, sinkDB, nil, nil)
	if err != nil {
		dbg.Debug("could not merge received data: " + err.Error())
		return ds
	}

	dbg.Debug("setting new datasetHead on localDB")
	newCommit := datas.NewCommit(merged, types.NewSet(sinkDB, ds.HeadRef(), sourceRef), types.EmptyStruct)
	commitRef := sinkDB.WriteValue(newCommit)
	dbg.Debug("wrote new commit: %s", commitRef.TargetHash())
	ds, err = sinkDB.SetHead(ds, commitRef)
	if err != nil {
		dbg.Debug("call to db.SetHead on failed, err: %s", err)
	}
	dbg.Debug("set new head ref: %s on ds.ID: %s", commitRef.TargetHash(), ds.ID())
	newH := ds.HeadRef().TargetHash()
	dbg.Debug("merged commit, dataset: %s, head: %s, cid: %s", ds.ID(), newH, ipfs.NomsHashToCID(newH))
	if cInfo.IsDaemon {
		cInfo.Delegate.PinBlocks(node, sourceDB, sinkDB, newCommit)
	} else {
		t.UpdateMessagesFromSync(ds)
	}
	return ds
}

// processInput adds a new msg (entered through the UI) updates it's dataset.
func processInput(t *TermUI, node *core.IpfsNode, ds datas.Dataset, msg string, cInfo ClientInfo) datas.Dataset {
	defer dbg.BoxF("processInput, msg: %s", msg)()
	t.InSearch = false
	if msg != "" {
		var err error
		ds, err = AddMessage(msg, cInfo.Username, time.Now(), ds)
		d.PanicIfError(err)
	}
	t.UpdateMessagesAsync(ds, nil, nil)
	return ds
}

// updates the UI to display search results.
func processSearch(t *TermUI, node *core.IpfsNode, ds datas.Dataset, terms string, cInfo ClientInfo) {
	defer dbg.BoxF("processSearch")()
	if terms == "" {
		return
	}
	t.InSearch = true
	searchTerms := TermsFromString(terms)
	searchIds := SearchIndex(ds, searchTerms)
	t.UpdateMessagesAsync(ds, &searchIds, searchTerms)
	return
}

// recurses over the chunks originating at 'h' and pins them to the IPFS repo.
func pinBlocks(node *core.IpfsNode, h hash.Hash, db datas.Database, depth, cnt int) (maxDepth, newCnt int) {
	maxDepth, newCnt = depth, cnt

	cid := ipfs.NomsHashToCID(h)
	_, pinned, err := node.Pinning.IsPinned(cid)
	d.Chk.NoError(err)
	if pinned {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	v := db.ReadValue(h)
	d.Chk.NotNil(v)

	v.WalkRefs(func(r types.Ref) {
		var newDepth int
		newDepth, newCnt = pinBlocks(node, r.TargetHash(), db, depth+1, newCnt)
		maxDepth = math.MaxInt(newDepth, maxDepth)
	})

	n, err := node.DAG.Get(ctx, cid)
	d.Chk.NoError(err)
	err = node.Pinning.Pin(ctx, n, false)
	d.Chk.NoError(err)
	newCnt++
	return
}

type IPFSEventDelegate struct{}

func (d IPFSEventDelegate) PinBlocks(node *core.IpfsNode, sourceDB, sinkDB datas.Database, sourceCommit types.Value) {
	h := sourceCommit.Hash()
	dbg.Debug("Starting pinBlocks")
	depth, cnt := pinBlocks(node, h, sinkDB, 0, 0)
	dbg.Debug("Finished pinBlocks, depth: %d, cnt: %d", depth, cnt)
	node.Pinning.Flush()
}

func (d IPFSEventDelegate) SourceCommitFromMsgData(db datas.Database, msgData string) (datas.Database, types.Value) {
	h := hash.Parse(msgData)
	v := db.ReadValue(h)
	return db, v
}

func (d IPFSEventDelegate) HashFromMsgData(msgData string) (hash.Hash, error) {
	var err error
	h, ok := hash.MaybeParse(msgData)
	if !ok {
		err = fmt.Errorf("Failed to parse hash from msgData: %s", msgData)
	}
	return h, err
}

func (d IPFSEventDelegate) GenMessageData(cInfo ClientInfo, h hash.Hash) string {
	return h.String()
}

type P2PEventDelegate struct{}

func (d P2PEventDelegate) PinBlocks(node *core.IpfsNode, sourceDB, sinkDB datas.Database, sourceCommit types.Value) {
	sourceRef := types.NewRef(sourceCommit)
	datas.Pull(sourceDB, sinkDB, sourceRef, nil)
}

func (d P2PEventDelegate) SourceCommitFromMsgData(db datas.Database, msgData string) (datas.Database, types.Value) {
	sp, _ := spec.ForPath(msgData)
	v := sp.GetValue()
	return sp.GetDatabase(), v
}

func (d P2PEventDelegate) HashFromMsgData(msgData string) (hash.Hash, error) {
	sp, err := spec.ForPath(msgData)
	return sp.Path.Hash, err
}

func (d P2PEventDelegate) GenMessageData(cInfo ClientInfo, h hash.Hash) string {
	return fmt.Sprintf("%s::#%s", cInfo.Spec, h)
}
