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

package json

import (
	"context"

	types2 "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

func MustNomsJSON(str string) NomsJSON {
	vrw := types.NewMemoryValueStore()
	return MustNomsJSONWithVRW(vrw, str)
}

func MustNomsJSONWithVRW(vrw types.ValueReadWriter, str string) NomsJSON {
	ctx := context.Background()
	doc := types2.MustJSON(str)
	noms, err := NomsJSONFromJSONValue(ctx, vrw, doc)
	if err != nil {
		panic(err)
	}
	return noms
}

func MustTypesJSON(str string) types.JSON {
	noms := MustNomsJSON(str)
	return types.JSON(noms)
}
