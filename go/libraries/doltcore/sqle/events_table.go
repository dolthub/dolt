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

package sqle

import (
	"fmt"

	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/types"
)

// EventsTableSchema is a fixed dolt schema for the 'dolt_events' table. Has 12 columns.
func EventsTableSchema() schema.Schema {
	colColl := schema.NewColCollection(
		schema.NewColumn(doltdb.EventsTableEventNameCol, schema.DoltEventsNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(doltdb.EventsTableDefinerCol, schema.DoltEventsDefinerTag, types.StringKind, false),
		schema.NewColumn(doltdb.EventsTableExecuteAtCol, schema.DoltEventsExecuteAtTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.EventsTableExecuteEveryCol, schema.DoltEventsExecuteEveryTag, types.StringKind, false),
		schema.NewColumn(doltdb.EventsTableStartsCol, schema.DoltEventsStartsTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.EventsTableEndsCol, schema.DoltEventsEndsTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.EventsTablePreserveCol, schema.DoltEventsPreserveTag, types.BoolKind, false),
		schema.NewColumn(doltdb.EventsTableStatusCol, schema.DoltEventsStatusTag, types.StringKind, false),
		schema.NewColumn(doltdb.EventsTableCommentCol, schema.DoltEventsCommentTag, types.StringKind, false),
		schema.NewColumn(doltdb.EventsTableDefinitionCol, schema.DoltEventsDefinitionTag, types.StringKind, false),
		schema.NewColumn(doltdb.EventsTableCreatedCol, schema.DoltEventsCreatedTag, types.TimestampKind, false),
		schema.NewColumn(doltdb.EventsTableLastAlteredCol, schema.DoltEventsLastAlteredTag, types.TimestampKind, false),
	)
	return schema.MustSchemaFromCols(colColl)
}

// GetOrCreateDoltEventsTable returns the `dolt_events` table from the given db, creating it in the db's
// current root if it doesn't exist
func GetOrCreateDoltEventsTable(ctx *sql.Context, db Database) (*WritableDoltTable, error) {
	tbl, found, err := db.GetTableInsensitive(ctx, doltdb.EventsTableName)
	if err != nil {
		return nil, err
	}
	if found {
		return tbl.(*WritableDoltTable), nil
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	err = db.createDoltTable(ctx, doltdb.EventsTableName, root, EventsTableSchema())
	if err != nil {
		return nil, err
	}

	tbl, found, err = db.GetTableInsensitive(ctx, doltdb.EventsTableName)
	if err != nil {
		return nil, err
	}
	// Verify it was created successfully
	if !found {
		return nil, sql.ErrTableNotFound.New(doltdb.EventsTableName)
	}
	return tbl.(*WritableDoltTable), nil
}

// GetDoltEventsTable returns the `dolt_events` table from the given db, or nil if the table doesn't exist
func GetDoltEventsTable(ctx *sql.Context, db Database) (*WritableDoltTable, error) {
	tbl, found, err := db.GetTableInsensitive(ctx, doltdb.EventsTableName)
	if err != nil {
		return nil, err
	}
	if found {
		return tbl.(*WritableDoltTable), nil
	} else {
		return nil, nil
	}
}

// GetAllDoltEvents returns all events for the database.
func GetAllDoltEvents(ctx *sql.Context, db Database) ([]sql.EventDetails, error) {
	eventsTbl, err := GetDoltEventsTable(ctx, db)
	if err != nil {
		return nil, err
	} else if eventsTbl == nil {
		return nil, nil
	}

	indexes, err := eventsTbl.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	if len(indexes) == 0 {
		return nil, fmt.Errorf("missing index for events")
	}
	idx := indexes[0]

	if len(idx.Expressions()) == 0 {
		return nil, fmt.Errorf("missing index expression for events")
	}
	nameExpr := idx.Expressions()[0]

	lookup, err := sql.NewIndexBuilder(idx).IsNotNull(ctx, nameExpr).Build(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := index.RowIterForIndexLookup(ctx, eventsTbl.DoltTable, lookup, eventsTbl.sqlSch, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := iter.Close(ctx); cerr != nil {
			err = cerr
		}
	}()

	var sqlRow sql.Row
	var ed sql.EventDetails
	var details []sql.EventDetails

	for {
		sqlRow, err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		ed, err = getEventDetailsFromDoltEventRow(sqlRow)
		if err != nil {
			return nil, err
		}
		details = append(details, ed)
	}
	return details, nil
}

// GetEventFromDoltEvents returns the event with the given name from `dolt_events` if it exists.
func GetEventFromDoltEvents(ctx *sql.Context, eventsTbl *WritableDoltTable, eventName string) (sql.EventDetails, bool, error) {
	eventName = strings.ToLower(eventName)
	indexes, err := eventsTbl.GetIndexes(ctx)
	if err != nil {
		return sql.EventDetails{}, false, err
	}
	if len(indexes) == 0 {
		return sql.EventDetails{}, false, fmt.Errorf("missing index for events")
	}
	idx := indexes[0]

	if len(idx.Expressions()) == 0 {
		return sql.EventDetails{}, false, fmt.Errorf("missing index expression for events")
	}
	nameExpr := idx.Expressions()[0]

	lookup, err := sql.NewIndexBuilder(idx).Equals(ctx, nameExpr, eventName).Build(ctx)
	if err != nil {
		return sql.EventDetails{}, false, err
	}

	iter, err := index.RowIterForIndexLookup(ctx, eventsTbl.DoltTable, lookup, eventsTbl.sqlSch, nil)
	if err != nil {
		return sql.EventDetails{}, false, err
	}
	defer func() {
		if cerr := iter.Close(ctx); cerr != nil {
			err = cerr
		}
	}()

	sqlRow, err := iter.Next(ctx)
	if err == nil {
		ed, err := getEventDetailsFromDoltEventRow(sqlRow)
		if err != nil {
			return sql.EventDetails{}, false, err
		}
		return ed, true, nil
	} else if err == io.EOF {
		return sql.EventDetails{}, false, nil
	} else {
		return sql.EventDetails{}, false, err
	}
}

// AddEventToDoltEventsTable adds the event to the `dolt_event` table in the given db,
// creating it if it does not exist.
func AddEventToDoltEventsTable(ctx *sql.Context, db Database, ed sql.EventDetails) (retErr error) {
	tbl, err := GetOrCreateDoltEventsTable(ctx, db)
	if err != nil {
		return err
	}
	_, exists, err := GetEventFromDoltEvents(ctx, tbl, ed.Name)
	if err != nil {
		return err
	}
	if exists {
		return sql.ErrEventAlreadyExists.New(ed.Name)
	}
	inserter := tbl.Inserter(ctx)
	defer func() {
		err := inserter.Close(ctx)
		if retErr == nil {
			retErr = err
		}
	}()
	return inserter.Insert(ctx, getDoltEventRowFromEventDetails(ed))
}

// DropEventFromDoltEventsTable removes the event from the `dolt_events` table. The event named must exist.
func DropEventFromDoltEventsTable(ctx *sql.Context, db Database, name string) (retErr error) {
	name = strings.ToLower(name)
	tbl, err := GetDoltEventsTable(ctx, db)
	if err != nil {
		return err
	} else if tbl == nil {
		return sql.ErrEventDoesNotExist.New(name)
	}

	_, exists, err := GetEventFromDoltEvents(ctx, tbl, name)
	if err != nil {
		return err
	}
	if !exists {
		return sql.ErrEventDoesNotExist.New(name)
	}
	deleter := tbl.Deleter(ctx)
	defer func() {
		err := deleter.Close(ctx)
		if retErr == nil {
			retErr = err
		}
	}()
	return deleter.Delete(ctx, sql.Row{name})
}

func getDoltEventRowFromEventDetails(ed sql.EventDetails) sql.Row {
	var at, every, starts, ends interface{}
	if ed.HasExecuteAt {
		at = ed.ExecuteAt
	} else {
		val, field := ed.ExecuteEvery.GetIntervalValAndField()
		every = fmt.Sprintf("%s %s", val, field)
		starts = ed.Starts
		if ed.HasEnds {
			ends = ed.Ends
		}
	}

	var preserve = int8(0)
	if ed.OnCompletionPreserve {
		preserve = 1
	}

	return sql.Row{
		strings.ToLower(ed.Name),
		ed.Definer,
		at,
		every,
		starts,
		ends,
		preserve,
		ed.Status.String(),
		ed.Comment,
		ed.Definition,
		ed.Created.UTC(),
		ed.LastAltered.UTC(),
	}
}

func getEventDetailsFromDoltEventRow(row sql.Row) (sql.EventDetails, error) {
	var ed sql.EventDetails
	var ok bool
	var err error
	missingValue := errors.NewKind("missing `%s` value for event row: (%s)")

	if ed.Name, ok = row[0].(string); !ok {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTableEventNameCol, row)
	}
	if ed.Definer, ok = row[1].(string); !ok {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTableDefinerCol, row)
	}
	if row[2] != nil {
		if ed.ExecuteAt, ok = row[2].(time.Time); !ok {
			return sql.EventDetails{}, missingValue.New(doltdb.EventsTableExecuteAtCol, row)
		}
		ed.HasExecuteAt = true
	} else {
		if every, ok := row[3].(string); ok {
			ed.ExecuteEvery, err = sql.GetEventOnScheduleEveryIntervalFromString(every)
			if err != nil {
				return sql.EventDetails{}, err
			}
		} else {
			return sql.EventDetails{}, missingValue.New(doltdb.EventsTableExecuteEveryCol, row)
		}
		// STARTS should not be nil because we set it to current_timestamp if it is not defined when created.
		if ed.Starts, ok = row[4].(time.Time); !ok {
			return sql.EventDetails{}, missingValue.New(doltdb.EventsTableStartsCol, row)
		}
		if row[5] != nil {
			if ed.Ends, ok = row[5].(time.Time); !ok {
				return sql.EventDetails{}, missingValue.New(doltdb.EventsTableEndsCol, row)
			}
			ed.HasEnds = true
		}
	}

	if ed.OnCompletionPreserve, err = gmstypes.ConvertToBool(row[6]); err != nil {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTablePreserveCol, row)
	}
	if status, ok := row[7].(string); ok {
		ed.Status, err = sql.GetEventStatusFromString(status)
		if err != nil {
			return sql.EventDetails{}, err
		}
	} else {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTableStatusCol, row)
	}
	if ed.Comment, ok = row[8].(string); !ok {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTableCommentCol, row)
	}
	if ed.Definition, ok = row[9].(string); !ok {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTableDefinitionCol, row)
	}
	if ed.Created, ok = row[10].(time.Time); !ok {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTableCreatedCol, row)
	}
	if ed.LastAltered, ok = row[11].(time.Time); !ok {
		return sql.EventDetails{}, missingValue.New(doltdb.EventsTableLastAlteredCol, row)
	}

	// TODO: need to fill data for
	//  LastExecuted   time.Time
	//  ExecutionCount uint64

	return ed, nil
}
