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
Package datas includes interfaces bridging database transactions and versioned
commit graph logic.

Datas includes logic for manipulating a versioned database through a backing
chunks.ChunkStore. In the old storage format, that was the NomsBlockStore. In
the new storage format, this is the NodeStore.

// TODO database

// TODO commit transactions

// TODO commit closures

// TODO refwalks

*/
package datas
