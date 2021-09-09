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
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/dolthub/dolt/go/store/hash"
)

type kindAndHash interface {
	Kind() NomsKind
	Hash(*NomsBinFormat) (hash.Hash, error)
}

func valueCompare(nbf *NomsBinFormat, v1, v2 kindAndHash) (int, error) {
	switch v2.Kind() {
	case UnknownKind:
		return 0, ErrUnknownType

	case BoolKind, FloatKind, StringKind:
		return 1, nil

	default:
		h1, err := v1.Hash(nbf)

		if err != nil {
			return 0, err
		}

		h2, err := v2.Hash(nbf)

		if err != nil {
			return 0, err
		}

		return h1.Compare(h2), nil
	}
}
