// Copyright 2025 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mount

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"context"
	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"os"
	"syscall"
)

// FileSystem is a FUSE file system for accessing data stored in a Dolt repo.
type FileSystem struct {
	dEnv *env.DoltEnv
	db   *doltdb.DoltDB
}

func NewFileSystem(dEnv *env.DoltEnv) FileSystem {
	return FileSystem{
		dEnv: dEnv,
		db:   dEnv.DoltDB,
	}
}

func (f FileSystem) Root() (fs.Node, error) {
	return rootDirectoryNode{
		dEnv: f.dEnv,
		db:   f.db,
	}, nil
}

type Directory interface {
	fs.Node
	fs.NodeStringLookuper
}

type ListableDirectory interface {
	Directory
	fs.HandleReadDirAller
}

type File interface {
	fs.Node
	fs.HandleReadAller
}

type BaseDirectory struct{}

var _ fs.Node = BaseDirectory{}

func (BaseDirectory) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o555
	return nil
}

func lookupHash(ctx context.Context, db *doltdb.DoltDB, h hash.Hash) (fs.Node, error) {
	value, err := db.ValueReadWriter().ReadValue(ctx, h)
	if err != nil {
		return nil, err
	}
	switch v := value.(type) {
	case types.SerialMessage:
		node, fileId, err := tree.NodeFromBytes(v)
		switch fileId {
		case serial.BlobFileID:
			return &blobFileNode{
				ns:   db.NodeStore(),
				node: node,
			}, err
		}
	}
	return nil, syscall.ENOENT
}

func lookupRefSpec(ctx context.Context, db *doltdb.DoltDB, refSpec string) (fs.Node, error) {
	refHash, err := db.GetHashForRefStr(ctx, refSpec)
	if err != nil {
		return nil, err
	}
	{
		return lookupHash(ctx, db, *refHash)
	}

	// Is this function unnecessary?
	datas := doltdb.HackDatasDatabaseFromDoltDB(db)
	datasets, err := datas.DatasetsWithPrefix(ctx, refSpec+"/")
	if err != nil {
		return nil, err
	}
	if len(datasets) == 0 {
		return nil, syscall.ENOENT
	}

	// if the segments resolve to a full ref spec, return it.
	// if there's no ref spec it can resolve to, return an error.
	return partialRefSpecDirectory{db: db, refSpec: refSpec}, nil
}
