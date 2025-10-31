// Copyright 2025 Dolthub, Inc.
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

package binlogreplication

import (
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	gmsbin "github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
)

// doltBinlogConsumer is a lightweight wrapper around the binlog applier that implements the BinlogConsumer interface.
// It provides binlog event processing for BINLOG statement execution.
type doltBinlogConsumer struct {
	applier *binlogReplicaApplier
	engine  *gms.Engine
}

var _ gmsbin.BinlogConsumer = (*doltBinlogConsumer)(nil)

// ProcessEvent implements the BinlogConsumer interface by delegating to the applier.
// Session command management is handled by the TransactionCommittingIter for BINLOG statements.
func (d *doltBinlogConsumer) ProcessEvent(ctx *sql.Context, event mysql.BinlogEvent) error {
	return d.applier.processBinlogEvent(ctx, d.engine, event)
}

// HasFormatDescription implements the BinlogConsumer interface.
func (d *doltBinlogConsumer) HasFormatDescription() bool {
	return d.applier.format != nil
}

// DoltBinlogConsumer is a global singleton that processes binlog events for BINLOG statements.
// It shares the same applier as DoltBinlogReplicaController to maintain consistent state.
var DoltBinlogConsumer = &doltBinlogConsumer{
	applier: DoltBinlogReplicaController.applier,
	engine:  nil, // Will be set via SetEngine
}

// SetEngine sets the engine for the binlog consumer.
func (d *doltBinlogConsumer) SetEngine(engine *gms.Engine) {
	d.engine = engine
}

