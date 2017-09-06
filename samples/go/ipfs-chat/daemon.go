// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/writers"
	"github.com/attic-labs/noms/samples/go/ipfs-chat/dbg"
	"github.com/ipfs/go-ipfs/core"
)

func runDaemon(topic string, interval time.Duration, ipfsSpec string, nodeIdx int) {
	dbg.SetLogger(log.New(os.Stdout, "", 0))

	stackDumpOnSIGQUIT()
	sp, err := spec.ForDataset(ipfsSpec)
	d.CheckErrorNoUsage(err)

	// Create/Open a new network chunkstore
	node, nwCS := initChunkStore(sp, nodeIdx)
	nwDB := datas.NewDatabase(nwCS)

	// Use the same ipfs node to create a second chunkstore that restricts data
	// access to it's local blockstore
	csLocal := ipfs.ChunkStoreFromIPFSNode(sp.DatabaseName, true, node)
	localDB := datas.NewDatabase(csLocal)
	localDS := localDB.GetDataset(sp.Path.Dataset)

	// Initial the database, if necessary, to an empty commit. Committing to
	// the local database will also reset the head for the network database.
	localDS, err = InitDatabase(localDS)
	d.PanicIfError(err)

	// Get the head of the network dataset.
	nwDS := nwDB.GetDataset(sp.Path.Dataset)

	dbg.Debug("Storing locally to: %s", sp.String())

	go replicate(node, topic, nwDS, localDS, func(ds1 datas.Dataset) {
		nwDS = ds1
	})

	for {
		Publish(node, topic, nwDS.HeadRef().TargetHash())
		time.Sleep(interval)
	}
}

// replicate continually listens for commit hashes published by ipfs-chat nodes,
// ensures that all nodes are replicated locally, and merges new data into it's
// dataset when necessary.
func replicate(node *core.IpfsNode, topic string, nwDS, localDS datas.Dataset, didChange func(ds datas.Dataset)) {
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
			// if something comes across the pubsub channel that doesn't look
			// like a valid hash, just print a message and ignore it.
			dbg.Debug("replicate: received unknown msg: %s", hstring)
			continue
		}
		// If we just saw this hash, then don't need to do anything
		if lastHash == h {
			continue
		}

		// we're going to process this hash
		dbg.Debug("got update: %s, lastHash: %s, sender %s", h, lastHash, base64.StdEncoding.EncodeToString(msg.From))
		lastHash = h

		// Get the current head of the local dataset
		localDB := localDS.Database()
		destHeadRef := localDS.HeadRef()

		// If the headRef of the local dataset is equal to the hash we just
		// received, there's nothing to do
		if h == localDS.HeadRef().TargetHash() {
			dbg.Debug("received hash same as current head, nothing to do")
			continue
		}

		// PullCommits() iterates through all the commits that are parents of this
		// commit and reads every chunk. This should ensure that all blocks are
		// local.
		dbg.Debug("syncing commits")
		pullCommits(h, nwDS.Database(), localDS.Database(), 0)

		// Everything should be local at this point, now check to see if a
		// merge or fast-forward needs to be performed.
		sourceCommit := localDB.ReadValue(h)
		sourceRef := types.NewRef(sourceCommit)

		dbg.Debug("Finding common ancestor for merge")
		a, ok := datas.FindCommonAncestor(sourceRef, destHeadRef, localDB)
		if !ok {
			dbg.Debug("no common ancestor, cannot merge update!")
			continue
		}
		dbg.Debug("Checking if source commit is ancestor")
		if a.Equals(sourceRef) {
			dbg.Debug("source commit was ancestor, nothing to do")
			continue
		}
		if a.Equals(localDS.HeadRef()) {
			dbg.Debug("fast-forward to source commit")
			localDS, err = localDB.SetHead(localDS, sourceRef)
			didChange(localDS)
			continue
		}

		dbg.Debug("We have mergeable commit")
		left := localDS.HeadValue()
		right := sourceCommit.(types.Struct).Get("value")
		parent := a.TargetValue(localDB).(types.Struct).Get("value")

		dbg.Debug("Starting three-way commit")
		merged, err := merge.ThreeWay(left, right, parent, localDB, nil, nil)
		if err != nil {
			dbg.Debug("could not merge received data: " + err.Error())
			continue
		}

		dbg.Debug("setting new datasetHead on localDB")
		localDS, err = localDB.SetHead(localDS, localDB.WriteValue(datas.NewCommit(merged, types.NewSet(localDB, localDS.HeadRef(), sourceRef), types.EmptyStruct)))
		if err != nil {
			dbg.Debug("call failed to SetHead on localDB, err: %s", err)
		}
		dbg.Debug("merged commit, new dataset head is: %ss", localDS.HeadRef().TargetHash())
		didChange(localDS)
	}
}

// One of the problem that this daemon currently has is that it hangs which
// effectively stops are 'replicate' loop. I can't figure out why that is
// happening.
func pullCommits(h hash.Hash, netDB, localDB datas.Database, level int) {
	// This is so we can easily tell in the log that we are not blocked in this
	// function
	defer func() {
		if level == 0 {
			dbg.Debug("EXITING PULL-COMMITS!!!")
		}
	}()

	// This is an optimization that can lead us to a bad place, if for some
	// reason we find a commit in the localDB that doesn't have all of it's
	// parents then something is wrong.
	dbg.Debug("pullCommits, checking in local, h: %s", h)
	if localDB.ReadValue(h) != nil {
		dbg.Debug("pullCommits, found local h: %s", h)
		return
	}

	dbg.Debug("pullCommits, not found in local, reading from net h: %s", h)
	v := netDB.ReadValue(h)
	d.Chk.NotNil(v)

	dbg.Debug("pullCommits, encoding value from net, h: %s", h)
	s1 := types.EncodedValue(v)
	buf := bytes.Buffer{}
	fmt.Fprintf(&writers.MaxLineWriter{Dest: &bytes.Buffer{}, MaxLines: 10}, s1)
	dbg.Debug("pullCommits, read from net, h: %s", h)
	dbg.Debug("pullCommits, read from net, h: %s, commit: %s", h, buf.String())

	// Call this function recursively on all of this commit's parents.
	commit := v.(types.Struct)
	parents := commit.Get("parents").(types.Set)
	parents.IterAll(func(v types.Value) {
		ph := v.(types.Ref).TargetHash()
		pullCommits(ph, netDB, localDB, level+1)
	})
}

func stackDumpOnSIGQUIT() {
	sigChan := make(chan os.Signal)
	go func() {
		stacktrace := make([]byte, 1024*1024)
		for range sigChan {
			length := runtime.Stack(stacktrace, true)
			fmt.Println(string(stacktrace[:length]))
		}
	}()
	signal.Notify(sigChan, syscall.SIGQUIT)
}
