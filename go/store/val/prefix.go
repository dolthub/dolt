// Copyright 2023 Dolthub, Inc.
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

package val

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
)

// TrimValueToPrefixLength trims |value| to |prefixLength| if it is longer
// and if it is either a []byte or string type. If |prefixLength| is zero,
// then |value| will be returned without being trimmed.
func TrimValueToPrefixLength(ctx context.Context, value interface{}, prefixLength uint16) (interface{}, error) {
	if prefixLength == 0 {
		return value, nil
	}

	var err error
	value, err = sql.UnwrapAny(ctx, value)
	if err != nil {
		return value, err
	}
	switch v := value.(type) {
	case string:
		if prefixLength > uint16(len(v)) {
			prefixLength = uint16(len(v))
		}
		value = v[:prefixLength]
	case []uint8:
		if prefixLength > uint16(len(v)) {
			prefixLength = uint16(len(v))
		}
		value = v[:prefixLength]
	}

	return value, nil
}
