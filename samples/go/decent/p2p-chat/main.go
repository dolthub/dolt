// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/ipfs"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/samples/go/decent/dbg"
	"github.com/attic-labs/noms/samples/go/decent/lib"
	"github.com/jroimartin/gocui"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	// allow short (-h) help
	kingpin.CommandLine.HelpFlag.Short('h')

	clientCmd := kingpin.Command("client", "runs the ipfs-chat client UI")
	clientTopic := clientCmd.Flag("topic", "IPFS pubsub topic to publish and subscribe to").Default("noms-chat-p2p").String()
	username := clientCmd.Flag("username", "username to sign in as").Required().String()
	nodeIdx := clientCmd.Flag("node-idx", "a single digit to be used as last digit in all port values: api, gateway and swarm (must be 0-9 inclusive)").Default("-1").Int()
	clientDir := clientCmd.Arg("path", "local directory to store data in").Required().ExistingDir()

	importCmd := kingpin.Command("import", "imports data into a chat")
	importSrc := importCmd.Flag("dir", "directory that contains data to import").Default("../data").ExistingDir()
	importDir := importCmd.Arg("path", "local directory to store data in").Required().ExistingDir()

	kingpin.CommandLine.Help = "A demonstration of using Noms to build a scalable multiuser collaborative application."

	switch kingpin.Parse() {
	case "client":
		cInfo := lib.ClientInfo{
			Topic:    *clientTopic,
			Username: *username,
			Idx:      *nodeIdx,
			IsDaemon: false,
			Dir:      *clientDir,
			Delegate: lib.P2PEventDelegate{},
		}
		runClient(cInfo)
	case "import":
		err := lib.RunImport(*importSrc, fmt.Sprintf("%s/noms::chat", *importDir))
		d.PanicIfError(err)
	}
}

func runClient(cInfo lib.ClientInfo) {
	dbg.SetLogger(lib.NewLogger(cInfo.Username))

	var err error
	httpPort := 8000 + cInfo.Idx
	sp, err := spec.ForDatabase(fmt.Sprintf("http://%s:%d", getIP(), httpPort))
	d.PanicIfError(err)
	cInfo.Spec = sp

	<-runServer(path.Join(cInfo.Dir, "noms"), httpPort)

	db := cInfo.Spec.GetDatabase()
	ds := db.GetDataset("chat")
	ds, err = lib.InitDatabase(ds)
	d.PanicIfError(err)

	node := ipfs.OpenIPFSRepo(path.Join(cInfo.Dir, "ipfs"), cInfo.Idx)
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

func getIP() string {
	ifaces, err := net.Interfaces()
	d.PanicIfError(err)
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		d.PanicIfError(err)
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				if !v.IP.IsLoopback() {
					ip := v.IP.To4()
					if ip != nil {
						return v.IP.String()
					}
				}
			}
		}
	}
	d.Panic("notreached")
	return ""
}

func runServer(atPath string, port int) (ready chan struct{}) {
	ready = make(chan struct{})
	_ = os.Mkdir(atPath, 0755)
	cfg := config.NewResolver()
	cs, err := cfg.GetChunkStore(atPath)
	d.CheckError(err)
	server := datas.NewRemoteDatabaseServer(cs, port)
	server.Ready = func() {
		ready <- struct{}{}
	}

	// Shutdown server gracefully so that profile may be written
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		server.Stop()
	}()

	go func() {
		d.Try(func() {
			defer profile.MaybeStartProfile().Stop()
			server.Run()
		})
	}()
	return
}
