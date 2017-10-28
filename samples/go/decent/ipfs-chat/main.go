// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/samples/go/decent/dbg"
	"github.com/attic-labs/noms/samples/go/decent/lib"
	"github.com/ipfs/go-ipfs/core"
	"github.com/jroimartin/gocui"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	// allow short (-h) help
	kingpin.CommandLine.HelpFlag.Short('h')

	clientCmd := kingpin.Command("client", "runs the ipfs-chat client UI")
	clientTopic := clientCmd.Flag("topic", "IPFS pubsub topic to publish and subscribe to").Default("ipfs-chat").String()
	username := clientCmd.Flag("username", "username to sign in as").String()
	nodeIdx := clientCmd.Flag("node-idx", "a single digit to be used as last digit in all port values: api, gateway and swarm (must be 0-9 inclusive)").Default("-1").Int()
	clientDS := clientCmd.Arg("dataset", "the dataset spec to store chat data in").Required().String()

	importCmd := kingpin.Command("import", "imports data into a chat")
	importDir := importCmd.Flag("dir", "directory that contains data to import").Default("./data").ExistingDir()
	importDS := importCmd.Arg("dataset", "the dataset spec to import chat data to").Required().String()

	daemonCmd := kingpin.Command("daemon", "runs a daemon that simulates filecoin, eagerly storing all chunks for a chat")
	daemonTopic := daemonCmd.Flag("topic", "IPFS pubsub topic to publish and subscribe to").Default("ipfs-chat").String()
	daemonInterval := daemonCmd.Flag("interval", "amount of time to wait before publishing state to network").Default("5s").Duration()
	daemonNodeIdx := daemonCmd.Flag("node-idx", "a single digit to be used as last digit in all port values: api, gateway and swarm (must be 0-9 inclusive)").Default("-1").Int()
	daemonDS := daemonCmd.Arg("dataset", "the dataset spec indicating ipfs repo to use").Required().String()

	kingpin.CommandLine.Help = "A demonstration of using Noms to build a scalable multiuser collaborative application."

	expandRLimit()
	switch kingpin.Parse() {
	case "client":
		cInfo := lib.ClientInfo{
			Topic:    *clientTopic,
			Username: *username,
			Idx:      *nodeIdx,
			IsDaemon: false,
			Delegate: lib.IPFSEventDelegate{},
		}
		runClient(*clientDS, cInfo)
	case "import":
		lib.RunImport(*importDir, *importDS)
	case "daemon":
		cInfo := lib.ClientInfo{
			Topic:    *daemonTopic,
			Username: "daemon",
			Interval: *daemonInterval,
			Idx:      *daemonNodeIdx,
			IsDaemon: true,
			Delegate: lib.IPFSEventDelegate{},
		}
		runDaemon(*daemonDS, cInfo)
	}
}

func runClient(ipfsSpec string, cInfo lib.ClientInfo) {
	dbg.SetLogger(lib.NewLogger(cInfo.Username))

	sp, err := spec.ForDataset(ipfsSpec)
	d.CheckErrorNoUsage(err)

	if !isIPFS(sp.Protocol) {
		fmt.Println("ipfs-chat requires an 'ipfs' dataset")
		os.Exit(1)
	}

	node, cs := initIPFSChunkStore(sp, cInfo.Idx)
	db := datas.NewDatabase(cs)

	// Get the head of specified dataset.
	ds := db.GetDataset(sp.Path.Dataset)
	ds, err = lib.InitDatabase(ds)
	d.PanicIfError(err)

	events := make(chan lib.ChatEvent, 1024)
	t := lib.CreateTermUI(events)
	defer t.Close()

	d.PanicIfError(t.Layout())
	t.ResetAuthors(ds)
	t.UpdateMessages(ds, nil, nil)

	go lib.ProcessChatEvents(node, ds, events, t, cInfo)
	go lib.ReceiveMessages(node, events, cInfo)

	if err := t.Gui.MainLoop(); err != nil && err != gocui.ErrQuit {
		dbg.Debug("mainloop has exited, err:", err)
		log.Panicln(err)
	}
}

func runDaemon(ipfsSpec string, cInfo lib.ClientInfo) {
	dbg.SetLogger(log.New(os.Stdout, "", 0))

	sp, err := spec.ForDataset(ipfsSpec)
	d.CheckErrorNoUsage(err)

	if !isIPFS(sp.Protocol) {
		fmt.Println("ipfs-chat requires an 'ipfs' dataset")
		os.Exit(1)
	}

	// Create/Open a new network chunkstore
	node, cs := initIPFSChunkStore(sp, cInfo.Idx)
	db := datas.NewDatabase(cs)

	// Get the head of specified dataset.
	ds := db.GetDataset(sp.Path.Dataset)
	ds, err = lib.InitDatabase(ds)
	d.PanicIfError(err)

	events := make(chan lib.ChatEvent, 1024)
	handleSIGQUIT(events)

	go lib.ReceiveMessages(node, events, cInfo)
	lib.ProcessChatEvents(node, ds, events, nil, cInfo)
}

func handleSIGQUIT(events chan<- lib.ChatEvent) {
	sigChan := make(chan os.Signal)
	go func() {
		for range sigChan {
			stacktrace := make([]byte, 1024*1024)
			length := runtime.Stack(stacktrace, true)
			dbg.Debug(string(stacktrace[:length]))
			events <- lib.ChatEvent{EventType: lib.QuitEvent}
		}
	}()
	signal.Notify(sigChan, os.Interrupt)
	signal.Notify(sigChan, syscall.SIGQUIT)
}

// IPFS can use a lot of file decriptors. There are several bugs in the IPFS
// repo about this and plans to improve. For the time being, we bump the limits
// for this process.
func expandRLimit() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	d.Chk.NoError(err, "Unable to query file rlimit: %s", err)
	if rLimit.Cur < rLimit.Max {
		rLimit.Max = 64000
		rLimit.Cur = 64000
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		d.Chk.NoError(err, "Unable to increase number of open files limit: %s", err)
	}
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	d.Chk.NoError(err)

	err = syscall.Getrlimit(8, &rLimit)
	d.Chk.NoError(err, "Unable to query thread rlimit: %s", err)
	if rLimit.Cur < rLimit.Max {
		rLimit.Max = 64000
		rLimit.Cur = 64000
		err = syscall.Setrlimit(8, &rLimit)
		d.Chk.NoError(err, "Unable to increase number of threads limit: %s", err)
	}
	err = syscall.Getrlimit(8, &rLimit)
	d.Chk.NoError(err)
}

func initIPFSChunkStore(sp spec.Spec, nodeIdx int) (*core.IpfsNode, chunks.ChunkStore) {
	// recreate database so that we can have control of chunkstore's ipfs node
	node := ipfs.OpenIPFSRepo(sp.DatabaseName, nodeIdx)
	cs := ipfs.ChunkStoreFromIPFSNode(sp.DatabaseName, sp.Protocol == "ipfs-local", node, 1)
	return node, cs
}

func isIPFS(protocol string) bool {
	return protocol == "ipfs" || protocol == "ipfs-local"
}
