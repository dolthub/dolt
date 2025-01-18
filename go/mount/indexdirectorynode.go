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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"syscall"
)

// An index presents as a directory layer for each key, ultimately containing a file for each column.

type indexDirectoryNode struct {
	BaseDirectory
	db         *doltdb.DoltDB
	ns         tree.NodeStore
	index      durable.Index
	keys       []interface{}
	keyBuilder *val.TupleBuilder
	position   int
	bp         pool.BuffPool
}

func (i indexDirectoryNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	//TODO implement me
	panic("implement me")
}

func (idn indexDirectoryNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	// newKeys := append(idn.keys, name)
	prollyMap := durable.ProllyMapFromIndex(idn.index)
	prefixDesc := prollyMap.KeyDesc().PrefixDesc(idn.position + 1)
	// prefixBuilder := val.NewTupleBuilder(prefixDesc)
	/*for i, key := range newKeys {
		err := tree.PutField(ctx, idn.ns, prefixBuilder, i, key)
		if err != nil {
			return nil, err
		}
	}*/
	err := tree.PutField(ctx, idn.ns, idn.keyBuilder, idn.position, name)
	if err != nil {
		return nil, err
	}
	if idn.position+1 == prollyMap.KeyDesc().Count() {
		var childNode fs.Node
		err = prollyMap.GetPrefix(ctx, idn.keyBuilder.BuildPrefix(idn.bp, idn.position+1), prefixDesc,
			func(key val.Tuple, value val.Tuple) error {
				childNode = valueTupleDirectoryNode{}
			})
		if err != nil {
			return nil, err
		}
		if childNode == nil {
			return nil, syscall.ENOENT
		}
		return childNode, nil
	}
	hasPrefix, err := prollyMap.HasPrefix(ctx, idn.keyBuilder.BuildPrefix(idn.bp, idn.position+1), prefixDesc)
	if err != nil {
		return nil, err
	}

	panic("implement me")
}

var _ ListableDirectory = indexDirectoryNode{}
