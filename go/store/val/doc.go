// Copyright 2021 Dolthub, Inc.
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
Package val includes:
  - Serialization between flatbuffer byte encodings and runtime Go-types
  - Tuple interfaces that support lazy access to specific row fields

Node key and value pairs are written as Tuples. Fields in tuples move to
and from disk using the Encoding types embedded in TupleDesc companions.
There are mappings between Dolt-side Encoding and SQL-side types, and only
Encoding operates at the NodeStore layer. Refer to val/codec.go for more
details.

The main feature of the new flatbuffer format is that individual fields can be
written and accessed directly via offsets, removing the need to materialize
entire tuples prior to SQL exec operations.
*/
package val
