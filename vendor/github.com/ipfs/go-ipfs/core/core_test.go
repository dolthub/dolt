package core

import (
	"testing"

	context "context"
	"github.com/ipfs/go-ipfs/repo"
	config "github.com/ipfs/go-ipfs/repo/config"
	ds2 "github.com/ipfs/go-ipfs/thirdparty/datastore2"
)

func TestInitialization(t *testing.T) {
	ctx := context.Background()
	id := testIdentity

	good := []*config.Config{
		{
			Identity: id,
			Addresses: config.Addresses{
				Swarm: []string{"/ip4/0.0.0.0/tcp/4001"},
				API:   "/ip4/127.0.0.1/tcp/8000",
			},
		},

		{
			Identity: id,
			Addresses: config.Addresses{
				Swarm: []string{"/ip4/0.0.0.0/tcp/4001"},
				API:   "/ip4/127.0.0.1/tcp/8000",
			},
		},
	}

	bad := []*config.Config{
		{},
	}

	for i, c := range good {
		r := &repo.Mock{
			C: *c,
			D: ds2.ThreadSafeCloserMapDatastore(),
		}
		n, err := NewNode(ctx, &BuildCfg{Repo: r})
		if n == nil || err != nil {
			t.Error("Should have constructed.", i, err)
		}
	}

	for i, c := range bad {
		r := &repo.Mock{
			C: *c,
			D: ds2.ThreadSafeCloserMapDatastore(),
		}
		n, err := NewNode(ctx, &BuildCfg{Repo: r})
		if n != nil || err == nil {
			t.Error("Should have failed to construct.", i)
		}
	}
}

var testIdentity = config.Identity{
	PeerID:  "QmNgdzLieYi8tgfo2WfTUzNVH5hQK9oAYGVf6dxN12NrHt",
	PrivKey: "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",
}
