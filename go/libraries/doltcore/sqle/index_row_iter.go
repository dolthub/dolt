// Copyright 2020 Liquidata, Inc.
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

package sqle

import (
	"io"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/store/types"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

type indexLookupRowIterAdapter struct {
	indexLookup *doltIndexLookup
	ctx         *sql.Context
}

func (i *indexLookupRowIterAdapter) Next() (sql.Row, error) {
	key, err := i.indexLookup.keyIter.NextKey(i.ctx)
	if err != nil {
		return nil, err
	}

	tableData := i.indexLookup.idx.TableData()
	pkTuple := key.NomsTupleForPKCols(tableData.Format(), i.indexLookup.idx.Schema().GetPKCols())
	pkTupleVal, err := pkTuple.Value(i.ctx)
	if err != nil {
		return nil, err
	}

	fieldsVal, _, err := tableData.MaybeGet(i.ctx, pkTupleVal)
	if err != nil {
		return nil, err
	}
	if fieldsVal == nil {
		return nil, io.EOF
	}

	r, err := row.FromNoms(i.indexLookup.idx.Schema(), pkTupleVal.(types.Tuple), fieldsVal.(types.Tuple))
	if err != nil {
		return nil, err
	}

	return doltRowToSqlRow(r, i.indexLookup.idx.Schema())
}

func (*indexLookupRowIterAdapter) Close() error {
	return nil
}
