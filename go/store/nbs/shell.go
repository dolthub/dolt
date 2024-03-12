// Copyright 2024 Dolthub, Inc.
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

package nbs

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/ishell"

	"github.com/dolthub/dolt/go/store/chunks"
)

// An interactive shell for debugging and inspecting a table file store.

type Shell interface {
	Run()
}

type shell struct {
	young chunkSourceSet
	old   chunkSourceSet
	hrcp  HumanReadableChunkPrinter
}

type HumanReadableChunkPrinter func([]byte) string

func NewShell(cs chunks.ChunkStore, hrcp HumanReadableChunkPrinter) Shell {
	if s, ok := cs.(*NomsBlockStore); ok {
		return &shell{
			young: s.tables.upstream,
			old:   make(chunkSourceSet),
			hrcp:  hrcp,
		}
	} else if gs, ok := cs.(*GenerationalNBS); ok {
		return &shell{
			young: gs.newGen.tables.upstream,
			old:   gs.oldGen.tables.upstream,
			hrcp:  hrcp,
		}
	} else {
		panic(fmt.Sprintf("unrecognized cs type for TFShell: %T", cs))
	}
}

func (s *shell) Run() {
	is := ishell.New()
	is.AddCmd(&ishell.Cmd{
		Name: "ls",
		Help: "list table files",
		Func: func(c *ishell.Context) {
			c.Printf("%32s%10s%25s\n", "Name", "Chunks", "File Bytes")
			for addr, cs := range s.young {
				count, err := cs.count()
				if err != nil {
					panic(err)
				}
				c.Printf("%20s%10d%25d\n", addr, count, cs.currentSize())
			}
			for addr, cs := range s.old {
				count, err := cs.count()
				if err != nil {
					panic(err)
				}
				c.Printf("%20s%10d%25d\n", addr, count, cs.currentSize())
			}
		},
	})
	is.AddCmd(&ishell.Cmd{
		Name: "hashes",
		Help: "list hashes of chunks in a table file, potentially matching a prefix",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 0 || len(c.Args) > 2 {
				c.Printf("Usage: hashes TABLE_FILE_ID [HASH_PREFIX]\n")
				return
			}
			tf, err := parseAddr(c.Args[0])
			if err != nil {
				c.Printf("Could not parse %s as table file name.\n", c.Args[0])
				c.Printf("Usage: hashes TABLE_FILE_ID [HASH_PREFIX]\n")
				return
			}
			var prefix string
			if len(c.Args) > 1 {
				prefix = c.Args[1]
			}
			cs := s.young[tf]
			if cs != nil {
				idx, err := cs.index()
				if err != nil {
					panic(err)
				}
				for i := uint32(0); i < idx.chunkCount(); i++ {
					var a addr
					_, err := idx.indexEntry(i, &a)
					if err != nil {
						panic(err)
					}
					as := a.String()
					if strings.HasPrefix(as, prefix) {
						c.Println(as)
					}
				}
			}
			cs = s.old[tf]
			if cs != nil {
				idx, err := cs.index()
				if err != nil {
					panic(err)
				}
				for i := uint32(0); i < idx.chunkCount(); i++ {
					var a addr
					_, err := idx.indexEntry(i, &a)
					if err != nil {
						panic(err)
					}
					as := a.String()
					if strings.HasPrefix(as, prefix) {
						c.Println(as)
					}
				}
			}
		},
	})
	is.AddCmd(&ishell.Cmd{
		Name: "bytes",
		Help: "print the uncompressed bytes of a given chunk in the chunk store",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Printf("Usage: bytes CHUNK_HASH\n")
				return
			}
			addr, err := parseAddr(c.Args[0])
			if err != nil {
				c.Printf("Could not parse %s as chunk hash.\n", c.Args[0])
				c.Printf("Usage: bytes CHUNK_HASH\n")
				return
			}
			var stat Stats
			for _, cs := range s.young {
				bs, err := cs.get(context.Background(), addr, &stat)
				if err != nil {
					panic(err)
				}
				if bs != nil {
					c.Println(bs)
					return
				}
			}
			for _, cs := range s.old {
				bs, err := cs.get(context.Background(), addr, &stat)
				if err != nil {
					panic(err)
				}
				if bs != nil {
					c.Println(bs)
					return
				}
			}
		},
	})
	is.AddCmd(&ishell.Cmd{
		Name: "print",
		Help: "print the human readable format of a given chunk in the chunk store",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				c.Printf("Usage: print CHUNK_HASH\n")
				return
			}
			addr, err := parseAddr(c.Args[0])
			if err != nil {
				c.Printf("Could not parse %s as chunk hash.\n", c.Args[0])
				c.Printf("Usage: print CHUNK_HASH\n")
				return
			}
			var stat Stats
			for _, cs := range s.young {
				bs, err := cs.get(context.Background(), addr, &stat)
				if err != nil {
					panic(err)
				}
				if bs != nil {
					c.Println(s.hrcp(bs))
					return
				}
			}
			for _, cs := range s.old {
				bs, err := cs.get(context.Background(), addr, &stat)
				if err != nil {
					panic(err)
				}
				if bs != nil {
					c.Println(s.hrcp(bs))
					return
				}
			}
		},
	})
	is.Run()
}
