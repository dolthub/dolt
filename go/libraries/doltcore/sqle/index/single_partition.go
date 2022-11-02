// Copyright 2020 Dolthub, Inc.
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

package index

import (
	"io"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.Partition = SinglePartition{}

type SinglePartition struct {
	RowData durable.Index
}

// Key returns the key for this partition, which must uniquely identity the partition. We have only a single partition
// per table, so we use a constant.
func (sp SinglePartition) Key() []byte {
	return []byte("single")
}

var _ sql.PartitionIter = SinglePartitionIter{}

type SinglePartitionIter struct {
	once    *sync.Once
	RowData durable.Index
}

func SinglePartitionIterFromNomsMap(rowData durable.Index) SinglePartitionIter {
	return SinglePartitionIter{&sync.Once{}, rowData}
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr SinglePartitionIter) Close(*sql.Context) error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr SinglePartitionIter) Next(*sql.Context) (sql.Partition, error) {
	first := false
	itr.once.Do(func() {
		first = true
	})
	if !first {
		return nil, io.EOF
	}
	return SinglePartition{itr.RowData}, nil
}
