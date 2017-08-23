package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	commands "github.com/ipfs/go-ipfs/commands"
	core "github.com/ipfs/go-ipfs/core"
	corehttp "github.com/ipfs/go-ipfs/core/corehttp"
	coreunix "github.com/ipfs/go-ipfs/core/coreunix"
	config "github.com/ipfs/go-ipfs/repo/config"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"

	homedir "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/mitchellh/go-homedir"

	process "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"

	fsnotify "gx/ipfs/QmczzCMvJ3HV57WBKDy8b4ucp7quT325JjDbixYRS5Pwvv/fsnotify.v1"
)

var http = flag.Bool("http", false, "expose IPFS HTTP API")
var repoPath = flag.String("repo", os.Getenv("IPFS_PATH"), "IPFS_PATH to use")
var watchPath = flag.String("path", ".", "the path to watch")

func main() {
	flag.Parse()

	// precedence
	// 1. --repo flag
	// 2. IPFS_PATH environment variable
	// 3. default repo path
	var ipfsPath string
	if *repoPath != "" {
		ipfsPath = *repoPath
	} else {
		var err error
		ipfsPath, err = fsrepo.BestKnownPath()
		if err != nil {
			log.Fatal(err)
		}
	}

	if err := run(ipfsPath, *watchPath); err != nil {
		log.Fatal(err)
	}
}

func run(ipfsPath, watchPath string) error {

	proc := process.WithParent(process.Background())
	log.Printf("running IPFSWatch on '%s' using repo at '%s'...", watchPath, ipfsPath)

	ipfsPath, err := homedir.Expand(ipfsPath)
	if err != nil {
		return err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	if err := addTree(watcher, watchPath); err != nil {
		return err
	}

	r, err := fsrepo.Open(ipfsPath)
	if err != nil {
		// TODO handle case: daemon running
		// TODO handle case: repo doesn't exist or isn't initialized
		return err
	}

	node, err := core.NewNode(context.Background(), &core.BuildCfg{
		Online: true,
		Repo:   r,
	})
	if err != nil {
		return err
	}
	defer node.Close()

	if *http {
		addr := "/ip4/127.0.0.1/tcp/5001"
		var opts = []corehttp.ServeOption{
			corehttp.GatewayOption(true, "/ipfs", "/ipns"),
			corehttp.WebUIOption,
			corehttp.CommandsOption(cmdCtx(node, ipfsPath)),
		}
		proc.Go(func(p process.Process) {
			if err := corehttp.ListenAndServe(node, addr, opts...); err != nil {
				return
			}
		})
	}

	interrupts := make(chan os.Signal)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-interrupts:
			return nil
		case e := <-watcher.Events:
			log.Printf("received event: %s", e)
			isDir, err := IsDirectory(e.Name)
			if err != nil {
				continue
			}
			switch e.Op {
			case fsnotify.Remove:
				if isDir {
					if err := watcher.Remove(e.Name); err != nil {
						return err
					}
				}
			default:
				// all events except for Remove result in an IPFS.Add, but only
				// directory creation triggers a new watch
				switch e.Op {
				case fsnotify.Create:
					if isDir {
						addTree(watcher, e.Name)
					}
				}
				proc.Go(func(p process.Process) {
					file, err := os.Open(e.Name)
					if err != nil {
						log.Println(err)
					}
					defer file.Close()
					k, err := coreunix.Add(node, file)
					if err != nil {
						log.Println(err)
					}
					log.Printf("added %s... key: %s", e.Name, k)
				})
			}
		case err := <-watcher.Errors:
			log.Println(err)
		}
	}
}

func addTree(w *fsnotify.Watcher, root string) error {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		isDir, err := IsDirectory(path)
		if err != nil {
			log.Println(err)
			return nil
		}
		switch {
		case isDir && IsHidden(path):
			log.Println(path)
			return filepath.SkipDir
		case isDir:
			log.Println(path)
			if err := w.Add(path); err != nil {
				return err
			}
		default:
			return nil
		}
		return nil
	})
	return err
}

func IsDirectory(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	return fileInfo.IsDir(), err
}

func IsHidden(path string) bool {
	path = filepath.Base(path)
	if path == "." || path == "" {
		return false
	}
	if rune(path[0]) == rune('.') {
		return true
	}
	return false
}

func cmdCtx(node *core.IpfsNode, repoPath string) commands.Context {
	return commands.Context{
		Online:     true,
		ConfigRoot: repoPath,
		LoadConfig: func(path string) (*config.Config, error) {
			return node.Repo.Config()
		},
		ConstructNode: func() (*core.IpfsNode, error) {
			return node, nil
		},
	}
}
