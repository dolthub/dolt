// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package progressreader provides an io.Reader that reports progress to a callback
package progressreader

import (
	"io"
	"time"

	"github.com/liquidata-inc/ld/dolt/go/store/util/status"
)

type Callback func(seen uint64)

func New(inner io.Reader, cb Callback) io.Reader {
	return &reader{inner, uint64(0), time.Time{}, cb}
}

type reader struct {
	inner    io.Reader
	seen     uint64
	lastTime time.Time
	cb       Callback
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.inner.Read(p)
	r.seen += uint64(n)

	if now := time.Now(); now.Sub(r.lastTime) >= status.Rate || err == io.EOF {
		r.cb(r.seen)
		r.lastTime = now
	}
	return
}
