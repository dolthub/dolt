package corerepo

import (
	"fmt"
	"math"

	context "context"
	"github.com/ipfs/go-ipfs/core"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"

	humanize "gx/ipfs/QmPSBJL4momYnE7DcUyk2DVhD6rH488ZmHBGLbxNdhU44K/go-humanize"
)

type Stat struct {
	NumObjects uint64
	RepoSize   uint64 // size in bytes
	RepoPath   string
	Version    string
	StorageMax uint64 // size in bytes
}

// NoLimit represents the value for unlimited storage
const NoLimit uint64 = math.MaxUint64

func RepoStat(n *core.IpfsNode, ctx context.Context) (*Stat, error) {
	r := n.Repo

	usage, err := r.GetStorageUsage()
	if err != nil {
		return nil, err
	}

	allKeys, err := n.Blockstore.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}

	count := uint64(0)
	for range allKeys {
		count++
	}

	path, err := fsrepo.BestKnownPath()
	if err != nil {
		return nil, err
	}

	cfg, err := r.Config()
	if err != nil {
		return nil, err
	}

	storageMax := NoLimit
	if cfg.Datastore.StorageMax != "" {
		storageMax, err = humanize.ParseBytes(cfg.Datastore.StorageMax)
		if err != nil {
			return nil, err
		}
	}

	return &Stat{
		NumObjects: count,
		RepoSize:   usage,
		RepoPath:   path,
		Version:    fmt.Sprintf("fs-repo@%d", fsrepo.RepoVersion),
		StorageMax: storageMax,
	}, nil
}
