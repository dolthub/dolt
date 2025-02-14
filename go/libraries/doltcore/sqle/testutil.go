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

package sqle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// ExecuteSql executes all the SQL non-select statements given in the string against the root value given and returns
// the updated root, or an error. Statements in the input string are split by `;\n`
func ExecuteSql(ctx context.Context, dEnv *env.DoltEnv, root doltdb.RootValue, statements string) (doltdb.RootValue, error) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}

	opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
	db, err := NewDatabase(context.Background(), "dolt", dEnv.DbData(ctx), opts)
	if err != nil {
		return nil, err
	}

	engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
	if err != nil {
		return nil, err
	}

	err = ExecuteSqlOnEngine(sqlCtx, engine, statements)
	if err != nil {
		return nil, err
	}
	return db.GetRoot(sqlCtx)
}

func ExecuteSqlOnEngine(ctx *sql.Context, engine *sqle.Engine, statements string) error {
	err := ctx.Session.SetSessionVariable(ctx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return err
	}

	for _, query := range strings.Split(statements, ";\n") {
		if len(strings.Trim(query, " ")) == 0 {
			continue
		}

		sqlStatement, err := sqlparser.Parse(query)
		if err != nil {
			return err
		}

		var execErr error
		switch sqlStatement.(type) {
		case *sqlparser.Show:
			return errors.New("Show statements aren't handled")
		case *sqlparser.Select, *sqlparser.OtherRead:
			return errors.New("Select statements aren't handled")
		case *sqlparser.Insert:
			var rowIter sql.RowIter
			_, rowIter, _, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(ctx, rowIter)
			}
		case *sqlparser.DDL, *sqlparser.AlterTable, *sqlparser.DBDDL:
			var rowIter sql.RowIter
			_, rowIter, _, execErr = engine.Query(ctx, query)
			if execErr == nil {
				execErr = drainIter(ctx, rowIter)
			}
		default:
			return fmt.Errorf("Unsupported SQL statement: '%v'.", query)
		}

		if execErr != nil {
			return execErr
		}
	}

	// commit leftover transaction
	trx := ctx.GetTransaction()
	if trx != nil {
		err = dsess.DSessFromSess(ctx.Session).CommitTransaction(ctx, trx)
		if err != nil {
			return err
		}
	}

	return nil
}

func NewTestSQLCtxWithProvider(ctx context.Context, pro dsess.DoltDatabaseProvider, config config.ReadWriteConfig, statsPro sql.StatsProvider, gcSafepointController *dsess.GCSafepointController) *sql.Context {
	s, err := dsess.NewDoltSession(sql.NewBaseSession(), pro, config, branch_control.CreateDefaultController(ctx), statsPro, writer.NewWriteSession, gcSafepointController)
	if err != nil {
		panic(err)
	}

	s.SetCurrentDatabase("dolt")
	return sql.NewContext(
		ctx,
		sql.WithSession(s),
	)
}

// NewTestEngine creates a new default engine, and a *sql.Context and initializes indexes and schema fragments.
func NewTestEngine(dEnv *env.DoltEnv, ctx context.Context, db dsess.SqlDatabase) (*sqle.Engine, *sql.Context, error) {
	b := env.GetDefaultInitBranch(dEnv.Config)
	pro, err := NewDoltDatabaseProviderWithDatabase(b, dEnv.FS, db, dEnv.FS)
	if err != nil {
		return nil, nil, err
	}
	gcSafepointController := dsess.NewGCSafepointController()

	engine := sqle.NewDefault(pro)

	config, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	sqlCtx := NewTestSQLCtxWithProvider(ctx, pro, config, nil, gcSafepointController)
	sqlCtx.SetCurrentDatabase(db.Name())
	return engine, sqlCtx, nil
}

// ExecuteSelect executes the select statement given and returns the resulting rows, or an error if one is encountered.
func ExecuteSelect(ctx context.Context, dEnv *env.DoltEnv, root doltdb.RootValue, query string) ([]sql.Row, error) {
	dbData := env.DbData{
		Ddb: dEnv.DoltDB(ctx),
		Rsw: dEnv.RepoStateWriter(),
		Rsr: dEnv.RepoStateReader(),
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}

	opts := editor.Options{Deaf: dEnv.DbEaFactory(ctx), Tempdir: tmpDir}
	db, err := NewDatabase(context.Background(), "dolt", dbData, opts)
	if err != nil {
		return nil, err
	}

	engine, sqlCtx, err := NewTestEngine(dEnv, context.Background(), db)
	if err != nil {
		return nil, err
	}

	_, rowIter, _, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}

	var (
		rows   []sql.Row
		rowErr error
		row    sql.Row
	)
	for row, rowErr = rowIter.Next(sqlCtx); rowErr == nil; row, rowErr = rowIter.Next(sqlCtx) {
		rows = append(rows, row)
	}

	if rowErr != io.EOF {
		return nil, rowErr
	}

	return rows, nil
}

// Returns the dolt rows given transformed to sql rows. Exactly the columns in the schema provided are present in the
// final output rows, even if the input rows contain different columns. The tag numbers for columns in the row and
// schema given must match.
func ToSqlRows(sch schema.Schema, rs ...row.Row) []sql.Row {
	sqlRows := make([]sql.Row, len(rs))
	compressedSch := CompressSchema(sch)
	for i := range rs {
		sqlRows[i], _ = sqlutil.DoltRowToSqlRow(CompressRow(sch, rs[i]), compressedSch)
	}
	return sqlRows
}

// Rewrites the tag numbers for the schema given to start at 0, just like result set schemas. If one or more column
// names are given, only those column names are included in the compressed schema. The column list can also be used to
// reorder the columns as necessary.
func CompressSchema(sch schema.Schema, colNames ...string) schema.Schema {
	var itag uint64
	var cols []schema.Column

	if len(colNames) > 0 {
		cols = make([]schema.Column, len(colNames))
		for _, colName := range colNames {
			column, ok := sch.GetAllCols().GetByName(colName)
			if !ok {
				panic("No column found for column name " + colName)
			}
			column.Tag = itag
			cols[itag] = column
			itag++
		}
	} else {
		cols = make([]schema.Column, sch.GetAllCols().Size())
		sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			col.Tag = itag
			cols[itag] = col
			itag++
			return false, nil
		})
	}

	colCol := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colCol)
}

// Rewrites the tag numbers for the schemas given to start at 0, just like result set schemas.
func CompressSchemas(schs ...schema.Schema) schema.Schema {
	var itag uint64
	var cols []schema.Column

	cols = make([]schema.Column, 0)
	for _, sch := range schs {
		sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
			col.Tag = itag
			cols = append(cols, col)
			itag++
			return false
		})
	}

	colCol := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colCol)
}

// Rewrites the tag numbers for the row given to begin at zero and be contiguous, just like result set schemas. We don't
// want to just use the field mappings in the result set schema used by sqlselect, since that would only demonstrate
// that the code was consistent with itself, not actually correct.
func CompressRow(sch schema.Schema, r row.Row) row.Row {
	var itag uint64
	compressedRow := make(row.TaggedValues)

	// TODO: this is probably incorrect and will break for schemas where the tag numbering doesn't match the declared order
	sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
		if val, ok := r.GetColVal(tag); ok {
			compressedRow[itag] = val
		}
		itag++
		return false
	})

	// call to compress schema is a no-op in most cases
	r, err := row.New(types.Format_Default, CompressSchema(sch), compressedRow)

	if err != nil {
		panic(err)
	}

	return r
}

// SubsetSchema returns a schema that is a subset of the schema given, with keys and constraints removed. Column names
// must be verified before subsetting. Unrecognized column names will cause a panic.
func SubsetSchema(sch schema.Schema, colNames ...string) schema.Schema {
	srcColls := sch.GetAllCols()

	var cols []schema.Column
	for _, name := range colNames {
		if col, ok := srcColls.GetByName(name); !ok {
			panic("Unrecognized name " + name)
		} else {
			cols = append(cols, col)
		}
	}
	colColl := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colColl)
}

// DoltSchemaFromAlterableTable is a utility for integration tests
func DoltSchemaFromAlterableTable(t *AlterableDoltTable) schema.Schema {
	return t.sch
}

// DoltTableFromAlterableTable is a utility for integration tests
func DoltTableFromAlterableTable(ctx *sql.Context, t *AlterableDoltTable) *doltdb.Table {
	dt, err := t.DoltTable.DoltTable(ctx)
	if err != nil {
		panic(err)
	}
	return dt
}

func drainIter(ctx *sql.Context, iter sql.RowIter) error {
	for {
		_, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			closeErr := iter.Close(ctx)
			if closeErr != nil {
				panic(fmt.Errorf("%v\n%v", err, closeErr))
			}
			return err
		}
	}
	return iter.Close(ctx)
}

func CreateEnvWithSeedData() (*env.DoltEnv, error) {
	const seedData = `
	CREATE TABLE people (
	    id varchar(36) primary key,
	    name varchar(40) not null,
	    age int unsigned,
	    is_married int,
	    title varchar(40),
	    INDEX idx_name (name)
	);
	INSERT INTO people VALUES
		('00000000-0000-0000-0000-000000000000', 'Bill Billerson', 32, 1, 'Senior Dufus'),
		('00000000-0000-0000-0000-000000000001', 'John Johnson', 25, 0, 'Dufus'),
		('00000000-0000-0000-0000-000000000002', 'Rob Robertson', 21, 0, '');`

	ctx := context.Background()
	dEnv := CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	root, err = ExecuteSql(ctx, dEnv, root, seedData)
	if err != nil {
		return nil, err
	}

	err = dEnv.UpdateWorkingRoot(ctx, root)
	if err != nil {
		return nil, err
	}

	return dEnv, nil
}

// CreateEmptyTestDatabase creates a test database without any data in it.
func CreateEmptyTestDatabase() (*env.DoltEnv, error) {
	dEnv := CreateTestEnv()
	err := CreateEmptyTestTable(dEnv, PeopleTableName, PeopleTestSchema)
	if err != nil {
		return nil, err
	}

	err = CreateEmptyTestTable(dEnv, EpisodesTableName, EpisodesTestSchema)
	if err != nil {
		return nil, err
	}

	err = CreateEmptyTestTable(dEnv, AppearancesTableName, AppearancesTestSchema)
	if err != nil {
		return nil, err
	}

	return dEnv, nil
}

const (
	TestHomeDirPrefix = "/user/dolt/"
	WorkingDirPrefix  = "/user/dolt/datasets/"
)

// CreateTestEnv creates a new DoltEnv suitable for testing. The CreateTestEnvWithName
// function should generally be preferred over this method, especially when working
// with tests using multiple databases within a MultiRepoEnv.
func CreateTestEnv() *env.DoltEnv {
	return CreateTestEnvWithName("test")
}

// CreateTestEnvWithName creates a new DoltEnv suitable for testing and uses
// the specified name to distinguish it from other test envs. This function
// should generally be preferred over CreateTestEnv, especially when working with
// tests using multiple databases within a MultiRepoEnv.
func CreateTestEnvWithName(envName string) *env.DoltEnv {
	const name = "billy bob"
	const email = "bigbillieb@fake.horse"
	initialDirs := []string{TestHomeDirPrefix + envName, WorkingDirPrefix + envName}
	homeDirFunc := func() (string, error) { return TestHomeDirPrefix + envName, nil }
	fs := filesys.NewInMemFS(initialDirs, nil, WorkingDirPrefix+envName)
	dEnv := env.Load(context.Background(), homeDirFunc, fs, doltdb.InMemDoltDB+envName, "test")
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	cfg.SetStrings(map[string]string{
		config.UserNameKey:  name,
		config.UserEmailKey: email,
	})

	err := dEnv.InitRepo(context.Background(), types.Format_Default, name, email, env.DefaultInitBranch)

	if err != nil {
		panic("Failed to initialize environment:" + err.Error())
	}

	return dEnv
}

// CreateEmptyTestTable creates a new test table with the name, schema, and rows given.
func CreateEmptyTestTable(dEnv *env.DoltEnv, tableName string, sch schema.Schema) error {
	ctx := context.Background()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	vrw := dEnv.DoltDB(ctx).ValueReadWriter()
	ns := dEnv.DoltDB(ctx).NodeStore()

	rows, err := durable.NewEmptyPrimaryIndex(ctx, vrw, ns, sch)
	if err != nil {
		return err
	}

	indexSet, err := durable.NewIndexSetWithEmptyIndexes(ctx, vrw, ns, sch)
	if err != nil {
		return err
	}

	tbl, err := doltdb.NewTable(ctx, vrw, ns, sch, rows, indexSet, nil)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, doltdb.TableName{Name: tableName}, tbl)
	if err != nil {
		return err
	}

	return dEnv.UpdateWorkingRoot(ctx, newRoot)
}

// CreateTestDatabase creates a test database with the test data set in it. Has a dirty workspace as well.
func CreateTestDatabase() (*env.DoltEnv, error) {
	ctx := context.Background()
	dEnv, err := CreateEmptyTestDatabase()
	if err != nil {
		return nil, err
	}

	const simpsonsRowData = `
	INSERT INTO people VALUES
		(0, "Homer", "Simpson", 1, 40, 8.5, NULL, NULL),
		(1, "Marge", "Simpson", 1, 38, 8, "00000000-0000-0000-0000-000000000001", 111),
		(2, "Bart", "Simpson", 0, 10, 9, "00000000-0000-0000-0000-000000000002", 222),
		(3, "Lisa", "Simpson", 0, 8, 10, "00000000-0000-0000-0000-000000000003", 333),
		(4, "Moe", "Szyslak", 0, 48, 6.5, "00000000-0000-0000-0000-000000000004", 444),
		(5, "Barney", "Gumble", 0, 40, 4, "00000000-0000-0000-0000-000000000005", 555);
	INSERT INTO episodes VALUES 
		(1, "Simpsons Roasting On an Open Fire", "1989-12-18 03:00:00", 8.0),
		(2, "Bart the Genius", "1990-01-15 03:00:00", 9.0),
		(3, "Homer's Odyssey", "1990-01-22 03:00:00", 7.0),
		(4, "There's No Disgrace Like Home", "1990-01-29 03:00:00", 8.5);
	INSERT INTO appearances VALUES 
		(0, 1, "Homer is great in this one"),
		(1, 1, "Marge is here too"),
		(0, 2, "Homer is great in this one too"),
		(2, 2, "This episode is named after Bart"),
		(3, 2, "Lisa is here too"),
		(4, 2, "I think there's a prank call scene"),
		(0, 3, "Homer is in every episode"),
		(1, 3, "Marge shows up a lot too"),
		(3, 3, "Lisa is the best Simpson"),
		(5, 3, "I'm making this all up");`

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	root, err = ExecuteSql(ctx, dEnv, root, simpsonsRowData)
	if err != nil {
		return nil, err
	}

	err = dEnv.UpdateWorkingRoot(ctx, root)
	if err != nil {
		return nil, err
	}

	return dEnv, nil
}

func SqlRowsFromDurableIndex(idx durable.Index, sch schema.Schema) ([]sql.Row, error) {
	ctx := context.Background()
	var sqlRows []sql.Row
	if types.Format_Default == types.Format_DOLT {
		rowData := durable.ProllyMapFromIndex(idx)
		kd, vd := rowData.Descriptors()
		iter, err := rowData.IterAll(ctx)
		if err != nil {
			return nil, err
		}
		for {
			var k, v val.Tuple
			k, v, err = iter.Next(ctx)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			sqlRow, err := sqlRowFromTuples(sch, kd, vd, k, v)
			if err != nil {
				return nil, err
			}
			sqlRows = append(sqlRows, sqlRow)
		}

	} else {
		// types.Format_LD_1
		rowData := durable.NomsMapFromIndex(idx)
		_ = rowData.IterAll(ctx, func(key, value types.Value) error {
			r, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
			if err != nil {
				return err
			}
			sqlRow, err := sqlutil.DoltRowToSqlRow(r, sch)
			if err != nil {
				return err
			}
			sqlRows = append(sqlRows, sqlRow)
			return nil
		})
	}
	return sqlRows, nil
}

func sqlRowFromTuples(sch schema.Schema, kd, vd val.TupleDesc, k, v val.Tuple) (sql.Row, error) {
	var err error
	ctx := context.Background()
	r := make(sql.Row, sch.GetAllCols().Size())
	keyless := schema.IsKeyless(sch)

	for i, col := range sch.GetAllCols().GetColumns() {
		pos, ok := sch.GetPKCols().TagToIdx[col.Tag]
		if ok {
			r[i], err = tree.GetField(ctx, kd, pos, k, nil)
			if err != nil {
				return nil, err
			}
		}

		pos, ok = sch.GetNonPKCols().TagToIdx[col.Tag]
		if keyless {
			pos += 1 // compensate for cardinality field
		}
		if ok {
			r[i], err = tree.GetField(ctx, vd, pos, v, nil)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}
