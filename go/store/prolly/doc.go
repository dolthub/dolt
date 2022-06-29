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
Package prolly includes:
  1) Serialize to and from the flatbuffer messages defined in go/serial
  2) Build trees of messages using a NodeStore abstraction
  2) Traverse and search NodeStore and related data structures

NodeStore is the primary interface for building/reading tree data structures:
- AddressMap, ProllyTreeNode, CommitClosure are the current Node flatbuffer
  message types
- A Node contains at least keys and values
- A Node can be referenced by an address hashed from its message contents
- Nodes can store data besides keys/values, like address references
- Most trees differentiate between 1) internal nodes, whose values are addresses
  that reference other nodes, and 2) leaf nodes, whose values are the main storage
  motivation
- Leaf nodes' values can be addresses.
  - For example, blobs are stored in ProllyTreeNode leaves as value address.
    The value address reference is the root hash of a tree stored separated. In
    these cases, it is important to distinguish between 1) self-contained trees
    of a single type; and 2) the datastore as a whole, comprised of several types
    of trees.

// TODO ProllyTreeNode

// TODO CommitClosure

// TODO AddressMap
*/
package prolly
