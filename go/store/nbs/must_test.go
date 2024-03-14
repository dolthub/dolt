// Copyright 2019 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/store/d"
)

func mustUint32(val uint32, err error) uint32 {
	d.PanicIfError(err)
	return val
}

func mustUint64(val uint64, err error) uint64 {
	d.PanicIfError(err)
	return val
}
