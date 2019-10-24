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

package doltcore

import (
	"github.com/liquidata-inc/dolt/go/store/types"
)

// GetConvFunc takes in a source kind and a destination kind and returns a MarshalCallback which can convert values of the
// source kind to values of the destination kind.
func GetConvFunc(srcKind, destKind types.NomsKind) types.MarshalCallback {
	if emptyVal, ok := types.KindToType[srcKind]; ok && emptyVal != nil {
		return emptyVal.MarshalToKind(destKind)
	}
	return nil
}
