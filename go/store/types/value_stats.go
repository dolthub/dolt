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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"fmt"
	"io"

	humanize "github.com/dustin/go-humanize"
	"github.com/golang/snappy"
)

type ValueStats interface {
	String() string
}

func WriteValueStats(ctx context.Context, w io.Writer, v Value, vr ValueReader) error {
	return writeUnchunkedValueStats(w, v, vr)
}

func writeUnchunkedValueStats(w io.Writer, v Value, vr ValueReader) error {
	cmpSize, err := compressedSize(vr.Format(), v)

	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "Kind: %s\nCompressedSize: %s\n", v.Kind().String(), humanize.Bytes(cmpSize))
	return err
}

func compressedSize(nbf *NomsBinFormat, v Value) (uint64, error) {
	chunk, err := EncodeValue(v, nbf)

	if err != nil {
		return 0, err
	}

	compressed := snappy.Encode(nil, chunk.Data())
	return uint64(len(compressed)), nil
}
