// Copyright 2022 Dolthub, Inc.
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

/*
Package serial defines flatbuffer tables used to persist Dolt data structures.

The StoreRoot is the tip of a database hierarchy, Nodes are the basic recursive
tree type for encoding data, and supportive metadata types like ForeignKey,
TableSchema, Column, ...etc are stored inline. In order of decreasing hierarchy:
  - StoreRoot is the tip of a database. Contains a map from dataset name to HEAD
    rootish in the form of an AddressMap
    - ex:   main -> abcdefghij0123456789
            feature -> abcdefghij0123456789
  - An AddressMap is itself a prolly tree (see NodeStore above) that can contains
    a name->rootish mapping of arbitrary size
  - A Rootish is informally a RootValue hash (like a working or staging hash), a
    Commit hash (that points to a root value hash), or a Tag (which points to a
    commit, and subsequently a root value hash).
    - refer to tag.fbs, workingset.fbs, commit.fbs for details
  - A RootValue is a static database version: tables, foreign keys, and a schema.
    Refer to rootvalue.fbs for details.
  - Schema encodes columns, the primary index, a secondary index, and check
    constraints, all inline as metadata.
  - Table is currently a wrapper for address references to the clustered index
    tree, secondary indexes trees, autoincrement values, and conflict/violations
    associated with a table.

Flatbuffer data structures are only partially self-describing. Additional
metadata exposes initialization hooks and chunkstore lookup:
  - Dolt manifest tracks 1) list of table hashes, 2) GC epoch, 3) write lock for
    transaction serializability, 4) storage version
  - table files store chunks next to indexes that facilitate binary search lookups
*/
package serial
