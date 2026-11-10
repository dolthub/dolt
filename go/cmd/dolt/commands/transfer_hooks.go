// Copyright 2026 Dolthub, Inc.
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

package commands

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

// openBareRepository returns the cache of the bare repository at |path| that transfer serves.
var openBareRepository = func(ctx context.Context, path string) (remotesrv.DBCache, error) {
	cs, err := nbs.NewLocalStore(ctx, types.Format_DOLT.VersionString(), path, remotesrv.MaxGRPCMessageSize, nbs.NewUnlimitedMemQuotaProvider(), false)
	if err != nil {
		return nil, fmt.Errorf("failed to open bare repository at %s: %w", path, err)
	}
	return &singletonDBCache{cs: cs}, nil
}
