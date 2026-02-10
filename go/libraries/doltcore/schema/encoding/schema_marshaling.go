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

package encoding

import (
	"context"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// MarshalSchema takes a Schema and converts it to a types.Value
func MarshalSchema(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Value, error) {
	// Anyone calling this is going to serialize this to disk, so it's our last line of defense against defective schemas.
	// Business logic should catch errors before this point, but this is a failsafe.
	err := schema.ValidateColumnConstraints(sch.GetAllCols())
	if err != nil {
		return nil, err
	}

	err = schema.ValidateForInsert(sch.GetAllCols())
	if err != nil {
		return nil, err
	}

	if vrw.Format().VersionString() != types.Format_DOLT.VersionString() {
		for _, idx := range sch.Indexes().AllIndexes() {
			if idx.IsSpatial() {
				return nil, fmt.Errorf("spatial indexes are only supported in storage format __DOLT__")
			}
		}
	}

	if vrw.Format().UsesFlatbuffers() {
		return SerializeSchema(ctx, vrw, sch)
	}

	panic("only __DOLT__ format is supported")
}

type schCacheData struct {
	schema schema.Schema
}

var schemaCacheMu *sync.Mutex = &sync.Mutex{}
var unmarshalledSchemaCache = map[hash.Hash]schCacheData{}

// UnmarshalSchema takes a types.Value representing a Schema and Unmarshalls it into a schema.Schema.
func UnmarshalSchema(ctx context.Context, nbf *types.NomsBinFormat, schemaVal types.Value) (schema.Schema, error) {
	if nbf.UsesFlatbuffers() {
		return DeserializeSchema(ctx, nbf, schemaVal)
	}

	panic("only __DOLT__ format is supported")
}

// UnmarshalSchemaAtAddr returns the schema at the given address, using the schema cache if possible.
func UnmarshalSchemaAtAddr(ctx context.Context, vr types.ValueReader, addr hash.Hash) (schema.Schema, error) {
	schemaCacheMu.Lock()
	cachedData, ok := unmarshalledSchemaCache[addr]
	schemaCacheMu.Unlock()

	if ok {
		cachedSch := cachedData.schema
		return cachedSch.Copy(), nil
	}

	schemaVal, err := vr.MustReadValue(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema object %s: %w", addr.String(), err)
	}

	sch, err := UnmarshalSchema(ctx, vr.Format(), schemaVal)
	if err != nil {
		return nil, err
	}

	d := schCacheData{
		schema: sch,
	}

	schemaCacheMu.Lock()
	unmarshalledSchemaCache[addr] = d
	schemaCacheMu.Unlock()

	return sch, nil
}
