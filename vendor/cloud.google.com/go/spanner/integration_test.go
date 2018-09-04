/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/internal/testutil"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	// testProjectID specifies the project used for testing.
	// It can be changed by setting environment variable GCLOUD_TESTS_GOLANG_PROJECT_ID.
	testProjectID = testutil.ProjID()
	// testInstanceID specifies the Cloud Spanner instance used for testing.
	testInstanceID = "go-integration-test"

	// admin is a spanner.DatabaseAdminClient.
	admin *database.DatabaseAdminClient
)

var (
	singerDBStatements = []string{
		`CREATE TABLE Singers (
				SingerId	INT64 NOT NULL,
				FirstName	STRING(1024),
				LastName	STRING(1024),
				SingerInfo	BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
		`CREATE INDEX SingerByName ON Singers(FirstName, LastName)`,
		`CREATE TABLE Accounts (
				AccountId	INT64 NOT NULL,
				Nickname	STRING(100),
				Balance		INT64 NOT NULL,
			) PRIMARY KEY (AccountId)`,
		`CREATE INDEX AccountByNickname ON Accounts(Nickname) STORING (Balance)`,
		`CREATE TABLE Types (
				RowID		INT64 NOT NULL,
				String		STRING(MAX),
				StringArray	ARRAY<STRING(MAX)>,
				Bytes		BYTES(MAX),
				BytesArray	ARRAY<BYTES(MAX)>,
				Int64a		INT64,
				Int64Array	ARRAY<INT64>,
				Bool		BOOL,
				BoolArray	ARRAY<BOOL>,
				Float64		FLOAT64,
				Float64Array	ARRAY<FLOAT64>,
				Date		DATE,
				DateArray	ARRAY<DATE>,
				Timestamp	TIMESTAMP,
				TimestampArray	ARRAY<TIMESTAMP>,
			) PRIMARY KEY (RowID)`,
	}

	readDBStatements = []string{
		`CREATE TABLE TestTable (
                    Key          STRING(MAX) NOT NULL,
                    StringValue  STRING(MAX)
            ) PRIMARY KEY (Key)`,
		`CREATE INDEX TestTableByValue ON TestTable(StringValue)`,
		`CREATE INDEX TestTableByValueDesc ON TestTable(StringValue DESC)`,
	}

	simpleDBStatements = []string{
		`CREATE TABLE test (
				a	STRING(1024),
				b	STRING(1024),
			) PRIMARY KEY (a)`,
	}
	simpleDBTableColumns = []string{"a", "b"}

	ctsDBStatements = []string{
		`CREATE TABLE TestTable (
		    Key  STRING(MAX) NOT NULL,
		    Ts   TIMESTAMP OPTIONS (allow_commit_timestamp = true),
	    ) PRIMARY KEY (Key)`,
	}
)

const (
	str1 = "alice"
	str2 = "a@example.com"
)

type testTableRow struct{ Key, StringValue string }

func TestMain(m *testing.M) {
	initIntegrationTest()
	os.Exit(m.Run())
}

func initIntegrationTest() {
	flag.Parse() // needed for testing.Short()
	if testing.Short() {
		return
	}
	if testProjectID == "" {
		log.Print("Integration tests skipped: GCLOUD_TESTS_GOLANG_PROJECT_ID is missing")
		return
	}
	ctx := context.Background()
	ts := testutil.TokenSource(ctx, AdminScope, Scope)
	if ts == nil {
		log.Printf("Integration test skipped: cannot get service account credential from environment variable %v", "GCLOUD_TESTS_GOLANG_KEY")
		return
	}
	var err error
	// Create Admin client and Data client.
	admin, err = database.NewDatabaseAdminClient(ctx, option.WithTokenSource(ts), option.WithEndpoint(endpoint))
	if err != nil {
		log.Fatalf("cannot create admin client: %v", err)
	}
}

var (
	mu    sync.Mutex
	count int
	now   = time.Now()
)

// prepare initializes Cloud Spanner testing DB and clients.
func prepare(ctx context.Context, t *testing.T, statements []string) (client *Client, dbPath string, tearDown func()) {
	if admin == nil {
		t.Skip("Integration tests skipped")
	}
	// Construct a unique test DB name.
	mu.Lock()
	dbName := fmt.Sprintf("gotest_%d_%d", now.UnixNano(), count)
	count++
	mu.Unlock()

	dbPath = fmt.Sprintf("projects/%v/instances/%v/databases/%v", testProjectID, testInstanceID, dbName)
	// Create database and tables.
	op, err := admin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%v/instances/%v", testProjectID, testInstanceID),
		CreateStatement: "CREATE DATABASE " + dbName,
		ExtraStatements: statements,
	})
	if err != nil {
		t.Fatalf("cannot create testing DB %v: %v", dbPath, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("cannot create testing DB %v: %v", dbPath, err)
	}
	client, err = NewClientWithConfig(ctx, dbPath, ClientConfig{
		SessionPoolConfig: SessionPoolConfig{WriteSessions: 0.2},
	}, option.WithTokenSource(testutil.TokenSource(ctx, Scope)), option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("cannot create data client on DB %v: %v", dbPath, err)
	}
	return client, dbPath, func() {
		client.Close()
		if err := admin.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: dbPath}); err != nil {
			t.Logf("failed to drop database %s (error %v), might need a manual removal",
				dbPath, err)
		}
	}
}

// Test SingleUse transaction.
func TestSingleUse(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Set up testing environment.
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	writes := []struct {
		row []interface{}
		ts  time.Time
	}{
		{row: []interface{}{1, "Marc", "Foo"}},
		{row: []interface{}{2, "Tars", "Bar"}},
		{row: []interface{}{3, "Alpha", "Beta"}},
		{row: []interface{}{4, "Last", "End"}},
	}
	// Try to write four rows through the Apply API.
	for i, w := range writes {
		var err error
		m := InsertOrUpdate("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			w.row)
		if writes[i].ts, err = client.Apply(ctx, []*Mutation{m}, ApplyAtLeastOnce()); err != nil {
			t.Fatal(err)
		}
	}

	// For testing timestamp bound staleness.
	<-time.After(time.Second)

	// Test reading rows with different timestamp bounds.
	for i, test := range []struct {
		want    [][]interface{}
		tb      TimestampBound
		checkTs func(time.Time) error
	}{
		{
			// strong
			[][]interface{}{{int64(1), "Marc", "Foo"}, {int64(3), "Alpha", "Beta"}, {int64(4), "Last", "End"}},
			StrongRead(),
			func(ts time.Time) error {
				// writes[3] is the last write, all subsequent strong read should have a timestamp larger than that.
				if ts.Before(writes[3].ts) {
					return fmt.Errorf("read got timestamp %v, want it to be no later than %v", ts, writes[3].ts)
				}
				return nil
			},
		},
		{
			// min_read_timestamp
			[][]interface{}{{int64(1), "Marc", "Foo"}, {int64(3), "Alpha", "Beta"}, {int64(4), "Last", "End"}},
			MinReadTimestamp(writes[3].ts),
			func(ts time.Time) error {
				if ts.Before(writes[3].ts) {
					return fmt.Errorf("read got timestamp %v, want it to be no later than %v", ts, writes[3].ts)
				}
				return nil
			},
		},
		{
			// max_staleness
			[][]interface{}{{int64(1), "Marc", "Foo"}, {int64(3), "Alpha", "Beta"}, {int64(4), "Last", "End"}},
			MaxStaleness(time.Second),
			func(ts time.Time) error {
				if ts.Before(writes[3].ts) {
					return fmt.Errorf("read got timestamp %v, want it to be no later than %v", ts, writes[3].ts)
				}
				return nil
			},
		},
		{
			// read_timestamp
			[][]interface{}{{int64(1), "Marc", "Foo"}, {int64(3), "Alpha", "Beta"}},
			ReadTimestamp(writes[2].ts),
			func(ts time.Time) error {
				if ts != writes[2].ts {
					return fmt.Errorf("read got timestamp %v, want %v", ts, writes[2].ts)
				}
				return nil
			},
		},
		{
			// exact_staleness
			nil,
			// Specify a staleness which should be already before this test because
			// context timeout is set to be 10s.
			ExactStaleness(11 * time.Second),
			func(ts time.Time) error {
				if ts.After(writes[0].ts) {
					return fmt.Errorf("read got timestamp %v, want it to be no earlier than %v", ts, writes[0].ts)
				}
				return nil
			},
		},
	} {
		// SingleUse.Query
		su := client.Single().WithTimestampBound(test.tb)
		got, err := readAll(su.Query(
			ctx,
			Statement{
				"SELECT SingerId, FirstName, LastName FROM Singers WHERE SingerId IN (@id1, @id3, @id4)",
				map[string]interface{}{"id1": int64(1), "id3": int64(3), "id4": int64(4)},
			}))
		if err != nil {
			t.Errorf("%d: SingleUse.Query returns error %v, want nil", i, err)
		}
		if !testEqual(got, test.want) {
			t.Errorf("%d: got unexpected result from SingleUse.Query: %v, want %v", i, got, test.want)
		}
		rts, err := su.Timestamp()
		if err != nil {
			t.Errorf("%d: SingleUse.Query doesn't return a timestamp, error: %v", i, err)
		}
		if err := test.checkTs(rts); err != nil {
			t.Errorf("%d: SingleUse.Query doesn't return expected timestamp: %v", i, err)
		}
		// SingleUse.Read
		su = client.Single().WithTimestampBound(test.tb)
		got, err = readAll(su.Read(ctx, "Singers", KeySets(Key{1}, Key{3}, Key{4}), []string{"SingerId", "FirstName", "LastName"}))
		if err != nil {
			t.Errorf("%d: SingleUse.Read returns error %v, want nil", i, err)
		}
		if !testEqual(got, test.want) {
			t.Errorf("%d: got unexpected result from SingleUse.Read: %v, want %v", i, got, test.want)
		}
		rts, err = su.Timestamp()
		if err != nil {
			t.Errorf("%d: SingleUse.Read doesn't return a timestamp, error: %v", i, err)
		}
		if err := test.checkTs(rts); err != nil {
			t.Errorf("%d: SingleUse.Read doesn't return expected timestamp: %v", i, err)
		}
		// SingleUse.ReadRow
		got = nil
		for _, k := range []Key{{1}, {3}, {4}} {
			su = client.Single().WithTimestampBound(test.tb)
			r, err := su.ReadRow(ctx, "Singers", k, []string{"SingerId", "FirstName", "LastName"})
			if err != nil {
				continue
			}
			v, err := rowToValues(r)
			if err != nil {
				continue
			}
			got = append(got, v)
			rts, err = su.Timestamp()
			if err != nil {
				t.Errorf("%d: SingleUse.ReadRow(%v) doesn't return a timestamp, error: %v", i, k, err)
			}
			if err := test.checkTs(rts); err != nil {
				t.Errorf("%d: SingleUse.ReadRow(%v) doesn't return expected timestamp: %v", i, k, err)
			}
		}
		if !testEqual(got, test.want) {
			t.Errorf("%d: got unexpected results from SingleUse.ReadRow: %v, want %v", i, got, test.want)
		}
		// SingleUse.ReadUsingIndex
		su = client.Single().WithTimestampBound(test.tb)
		got, err = readAll(su.ReadUsingIndex(ctx, "Singers", "SingerByName", KeySets(Key{"Marc", "Foo"}, Key{"Alpha", "Beta"}, Key{"Last", "End"}), []string{"SingerId", "FirstName", "LastName"}))
		if err != nil {
			t.Errorf("%d: SingleUse.ReadUsingIndex returns error %v, want nil", i, err)
		}
		// The results from ReadUsingIndex is sorted by the index rather than primary key.
		if len(got) != len(test.want) {
			t.Errorf("%d: got unexpected result from SingleUse.ReadUsingIndex: %v, want %v", i, got, test.want)
		}
		for j, g := range got {
			if j > 0 {
				prev := got[j-1][1].(string) + got[j-1][2].(string)
				curr := got[j][1].(string) + got[j][2].(string)
				if strings.Compare(prev, curr) > 0 {
					t.Errorf("%d: SingleUse.ReadUsingIndex fails to order rows by index keys, %v should be after %v", i, got[j-1], got[j])
				}
			}
			found := false
			for _, w := range test.want {
				if testEqual(g, w) {
					found = true
				}
			}
			if !found {
				t.Errorf("%d: got unexpected result from SingleUse.ReadUsingIndex: %v, want %v", i, got, test.want)
				break
			}
		}
		rts, err = su.Timestamp()
		if err != nil {
			t.Errorf("%d: SingleUse.ReadUsingIndex doesn't return a timestamp, error: %v", i, err)
		}
		if err := test.checkTs(rts); err != nil {
			t.Errorf("%d: SingleUse.ReadUsingIndex doesn't return expected timestamp: %v", i, err)
		}
	}

	// Reading with limit.
	su := client.Single()
	const limit = 1
	gotRows, err := readAll(su.ReadWithOptions(ctx, "Singers", KeySets(Key{1}, Key{3}, Key{4}),
		[]string{"SingerId", "FirstName", "LastName"}, &ReadOptions{Limit: limit}))
	if err != nil {
		t.Errorf("SingleUse.ReadWithOptions returns error %v, want nil", err)
	}
	if got, want := len(gotRows), limit; got != want {
		t.Errorf("got %d, want %d", got, want)
	}

}

// Test ReadOnlyTransaction. The testsuite is mostly like SingleUse, except it
// also tests for a single timestamp across multiple reads.
func TestReadOnlyTransaction(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// Set up testing environment.
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	writes := []struct {
		row []interface{}
		ts  time.Time
	}{
		{row: []interface{}{1, "Marc", "Foo"}},
		{row: []interface{}{2, "Tars", "Bar"}},
		{row: []interface{}{3, "Alpha", "Beta"}},
		{row: []interface{}{4, "Last", "End"}},
	}
	// Try to write four rows through the Apply API.
	for i, w := range writes {
		var err error
		m := InsertOrUpdate("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			w.row)
		if writes[i].ts, err = client.Apply(ctx, []*Mutation{m}, ApplyAtLeastOnce()); err != nil {
			t.Fatal(err)
		}
	}

	// For testing timestamp bound staleness.
	<-time.After(time.Second)

	// Test reading rows with different timestamp bounds.
	for i, test := range []struct {
		want    [][]interface{}
		tb      TimestampBound
		checkTs func(time.Time) error
	}{
		// Note: min_read_timestamp and max_staleness are not supported by ReadOnlyTransaction. See
		// API document for more details.
		{
			// strong
			[][]interface{}{{int64(1), "Marc", "Foo"}, {int64(3), "Alpha", "Beta"}, {int64(4), "Last", "End"}},
			StrongRead(),
			func(ts time.Time) error {
				if ts.Before(writes[3].ts) {
					return fmt.Errorf("read got timestamp %v, want it to be no later than %v", ts, writes[3].ts)
				}
				return nil
			},
		},
		{
			// read_timestamp
			[][]interface{}{{int64(1), "Marc", "Foo"}, {int64(3), "Alpha", "Beta"}},
			ReadTimestamp(writes[2].ts),
			func(ts time.Time) error {
				if ts != writes[2].ts {
					return fmt.Errorf("read got timestamp %v, expect %v", ts, writes[2].ts)
				}
				return nil
			},
		},
		{
			// exact_staleness
			nil,
			// Specify a staleness which should be already before this test because
			// context timeout is set to be 10s.
			ExactStaleness(11 * time.Second),
			func(ts time.Time) error {
				if ts.After(writes[0].ts) {
					return fmt.Errorf("read got timestamp %v, want it to be no earlier than %v", ts, writes[0].ts)
				}
				return nil
			},
		},
	} {
		// ReadOnlyTransaction.Query
		ro := client.ReadOnlyTransaction().WithTimestampBound(test.tb)
		got, err := readAll(ro.Query(
			ctx,
			Statement{
				"SELECT SingerId, FirstName, LastName FROM Singers WHERE SingerId IN (@id1, @id3, @id4)",
				map[string]interface{}{"id1": int64(1), "id3": int64(3), "id4": int64(4)},
			}))
		if err != nil {
			t.Errorf("%d: ReadOnlyTransaction.Query returns error %v, want nil", i, err)
		}
		if !testEqual(got, test.want) {
			t.Errorf("%d: got unexpected result from ReadOnlyTransaction.Query: %v, want %v", i, got, test.want)
		}
		rts, err := ro.Timestamp()
		if err != nil {
			t.Errorf("%d: ReadOnlyTransaction.Query doesn't return a timestamp, error: %v", i, err)
		}
		if err := test.checkTs(rts); err != nil {
			t.Errorf("%d: ReadOnlyTransaction.Query doesn't return expected timestamp: %v", i, err)
		}
		roTs := rts
		// ReadOnlyTransaction.Read
		got, err = readAll(ro.Read(ctx, "Singers", KeySets(Key{1}, Key{3}, Key{4}), []string{"SingerId", "FirstName", "LastName"}))
		if err != nil {
			t.Errorf("%d: ReadOnlyTransaction.Read returns error %v, want nil", i, err)
		}
		if !testEqual(got, test.want) {
			t.Errorf("%d: got unexpected result from ReadOnlyTransaction.Read: %v, want %v", i, got, test.want)
		}
		rts, err = ro.Timestamp()
		if err != nil {
			t.Errorf("%d: ReadOnlyTransaction.Read doesn't return a timestamp, error: %v", i, err)
		}
		if err := test.checkTs(rts); err != nil {
			t.Errorf("%d: ReadOnlyTransaction.Read doesn't return expected timestamp: %v", i, err)
		}
		if roTs != rts {
			t.Errorf("%d: got two read timestamps: %v, %v, want ReadOnlyTransaction to return always the same read timestamp", i, roTs, rts)
		}
		// ReadOnlyTransaction.ReadRow
		got = nil
		for _, k := range []Key{{1}, {3}, {4}} {
			r, err := ro.ReadRow(ctx, "Singers", k, []string{"SingerId", "FirstName", "LastName"})
			if err != nil {
				continue
			}
			v, err := rowToValues(r)
			if err != nil {
				continue
			}
			got = append(got, v)
			rts, err = ro.Timestamp()
			if err != nil {
				t.Errorf("%d: ReadOnlyTransaction.ReadRow(%v) doesn't return a timestamp, error: %v", i, k, err)
			}
			if err := test.checkTs(rts); err != nil {
				t.Errorf("%d: ReadOnlyTransaction.ReadRow(%v) doesn't return expected timestamp: %v", i, k, err)
			}
			if roTs != rts {
				t.Errorf("%d: got two read timestamps: %v, %v, want ReadOnlyTransaction to return always the same read timestamp", i, roTs, rts)
			}
		}
		if !testEqual(got, test.want) {
			t.Errorf("%d: got unexpected results from ReadOnlyTransaction.ReadRow: %v, want %v", i, got, test.want)
		}
		// SingleUse.ReadUsingIndex
		got, err = readAll(ro.ReadUsingIndex(ctx, "Singers", "SingerByName", KeySets(Key{"Marc", "Foo"}, Key{"Alpha", "Beta"}, Key{"Last", "End"}), []string{"SingerId", "FirstName", "LastName"}))
		if err != nil {
			t.Errorf("%d: ReadOnlyTransaction.ReadUsingIndex returns error %v, want nil", i, err)
		}
		// The results from ReadUsingIndex is sorted by the index rather than primary key.
		if len(got) != len(test.want) {
			t.Errorf("%d: got unexpected result from ReadOnlyTransaction.ReadUsingIndex: %v, want %v", i, got, test.want)
		}
		for j, g := range got {
			if j > 0 {
				prev := got[j-1][1].(string) + got[j-1][2].(string)
				curr := got[j][1].(string) + got[j][2].(string)
				if strings.Compare(prev, curr) > 0 {
					t.Errorf("%d: ReadOnlyTransaction.ReadUsingIndex fails to order rows by index keys, %v should be after %v", i, got[j-1], got[j])
				}
			}
			found := false
			for _, w := range test.want {
				if testEqual(g, w) {
					found = true
				}
			}
			if !found {
				t.Errorf("%d: got unexpected result from ReadOnlyTransaction.ReadUsingIndex: %v, want %v", i, got, test.want)
				break
			}
		}
		rts, err = ro.Timestamp()
		if err != nil {
			t.Errorf("%d: ReadOnlyTransaction.ReadUsingIndex doesn't return a timestamp, error: %v", i, err)
		}
		if err := test.checkTs(rts); err != nil {
			t.Errorf("%d: ReadOnlyTransaction.ReadUsingIndex doesn't return expected timestamp: %v", i, err)
		}
		if roTs != rts {
			t.Errorf("%d: got two read timestamps: %v, %v, want ReadOnlyTransaction to return always the same read timestamp", i, roTs, rts)
		}
		ro.Close()
	}
}

// Test ReadOnlyTransaction with different timestamp bound when there's an update at the same time.
func TestUpdateDuringRead(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	for i, tb := range []TimestampBound{
		StrongRead(),
		ReadTimestamp(time.Now().Add(-time.Minute * 30)), // version GC is 1 hour
		ExactStaleness(time.Minute * 30),
	} {
		ro := client.ReadOnlyTransaction().WithTimestampBound(tb)
		_, err := ro.ReadRow(ctx, "Singers", Key{i}, []string{"SingerId"})
		if ErrCode(err) != codes.NotFound {
			t.Errorf("%d: ReadOnlyTransaction.ReadRow before write returns error: %v, want NotFound", i, err)
		}

		m := InsertOrUpdate("Singers", []string{"SingerId"}, []interface{}{i})
		if _, err := client.Apply(ctx, []*Mutation{m}, ApplyAtLeastOnce()); err != nil {
			t.Fatal(err)
		}

		_, err = ro.ReadRow(ctx, "Singers", Key{i}, []string{"SingerId"})
		if ErrCode(err) != codes.NotFound {
			t.Errorf("%d: ReadOnlyTransaction.ReadRow after write returns error: %v, want NotFound", i, err)
		}
	}
}

// Test ReadWriteTransaction.
func TestReadWriteTransaction(t *testing.T) {
	t.Parallel()
	// Give a longer deadline because of transaction backoffs.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	// Set up two accounts
	accounts := []*Mutation{
		Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(1), "Foo", int64(50)}),
		Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(2), "Bar", int64(1)}),
	}
	if _, err := client.Apply(ctx, accounts, ApplyAtLeastOnce()); err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}

	readBalance := func(iter *RowIterator) (int64, error) {
		defer iter.Stop()
		var bal int64
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				return bal, nil
			}
			if err != nil {
				return 0, err
			}
			if err := row.Column(0, &bal); err != nil {
				return 0, err
			}
		}
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *ReadWriteTransaction) error {
				// Query Foo's balance and Bar's balance.
				bf, e := readBalance(tx.Query(ctx,
					Statement{"SELECT Balance FROM Accounts WHERE AccountId = @id", map[string]interface{}{"id": int64(1)}}))
				if e != nil {
					return e
				}
				bb, e := readBalance(tx.Read(ctx, "Accounts", KeySets(Key{int64(2)}), []string{"Balance"}))
				if e != nil {
					return e
				}
				if bf <= 0 {
					return nil
				}
				bf--
				bb++
				return tx.BufferWrite([]*Mutation{
					Update("Accounts", []string{"AccountId", "Balance"}, []interface{}{int64(1), bf}),
					Update("Accounts", []string{"AccountId", "Balance"}, []interface{}{int64(2), bb}),
				})
			})
			if err != nil {
				t.Fatalf("%d: failed to execute transaction: %v", iter, err)
			}
		}(i)
	}
	// Because of context timeout, all goroutines will eventually return.
	wg.Wait()
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *ReadWriteTransaction) error {
		var bf, bb int64
		r, e := tx.ReadRow(ctx, "Accounts", Key{int64(1)}, []string{"Balance"})
		if e != nil {
			return e
		}
		if ce := r.Column(0, &bf); ce != nil {
			return ce
		}
		bb, e = readBalance(tx.ReadUsingIndex(ctx, "Accounts", "AccountByNickname", KeySets(Key{"Bar"}), []string{"Balance"}))
		if e != nil {
			return e
		}
		if bf != 30 || bb != 21 {
			t.Errorf("Foo's balance is now %v and Bar's balance is now %v, want %v and %v", bf, bb, 30, 21)
		}
		return nil
	})
	if err != nil {
		t.Errorf("failed to check balances: %v", err)
	}
}

const (
	testTable      = "TestTable"
	testTableIndex = "TestTableByValue"
)

var testTableColumns = []string{"Key", "StringValue"}

func TestReads(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// Set up testing environment.
	client, _, tearDown := prepare(ctx, t, readDBStatements)
	defer tearDown()

	// Includes k0..k14. Strings sort lexically, eg "k1" < "k10" < "k2".
	var ms []*Mutation
	for i := 0; i < 15; i++ {
		ms = append(ms, InsertOrUpdate(testTable,
			testTableColumns,
			[]interface{}{fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)}))
	}
	// Don't use ApplyAtLeastOnce, so we can test the other code path.
	if _, err := client.Apply(ctx, ms); err != nil {
		t.Fatal(err)
	}

	// Empty read.
	rows, err := readAllTestTable(client.Single().Read(ctx, testTable,
		KeyRange{Start: Key{"k99"}, End: Key{"z"}}, testTableColumns))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(rows), 0; got != want {
		t.Errorf("got %d, want %d", got, want)
	}

	// Index empty read.
	rows, err = readAllTestTable(client.Single().ReadUsingIndex(ctx, testTable, testTableIndex,
		KeyRange{Start: Key{"v99"}, End: Key{"z"}}, testTableColumns))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(rows), 0; got != want {
		t.Errorf("got %d, want %d", got, want)
	}

	// Point read.
	row, err := client.Single().ReadRow(ctx, testTable, Key{"k1"}, testTableColumns)
	if err != nil {
		t.Fatal(err)
	}
	var got testTableRow
	if err := row.ToStruct(&got); err != nil {
		t.Fatal(err)
	}
	if want := (testTableRow{"k1", "v1"}); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Point read not found.
	_, err = client.Single().ReadRow(ctx, testTable, Key{"k999"}, testTableColumns)
	if ErrCode(err) != codes.NotFound {
		t.Fatalf("got %v, want NotFound", err)
	}

	// No index point read not found, because Go does not have ReadRowUsingIndex.

	rangeReads(ctx, t, client)
	indexRangeReads(ctx, t, client)
}

func rangeReads(ctx context.Context, t *testing.T, client *Client) {
	checkRange := func(ks KeySet, wantNums ...int) {
		if msg, ok := compareRows(client.Single().Read(ctx, testTable, ks, testTableColumns), wantNums); !ok {
			t.Errorf("key set %+v: %s", ks, msg)
		}
	}

	checkRange(Key{"k1"}, 1)
	checkRange(KeyRange{Key{"k3"}, Key{"k5"}, ClosedOpen}, 3, 4)
	checkRange(KeyRange{Key{"k3"}, Key{"k5"}, ClosedClosed}, 3, 4, 5)
	checkRange(KeyRange{Key{"k3"}, Key{"k5"}, OpenClosed}, 4, 5)
	checkRange(KeyRange{Key{"k3"}, Key{"k5"}, OpenOpen}, 4)

	// Partial key specification.
	checkRange(KeyRange{Key{"k7"}, Key{}, ClosedClosed}, 7, 8, 9)
	checkRange(KeyRange{Key{"k7"}, Key{}, OpenClosed}, 8, 9)
	checkRange(KeyRange{Key{}, Key{"k11"}, ClosedOpen}, 0, 1, 10)
	checkRange(KeyRange{Key{}, Key{"k11"}, ClosedClosed}, 0, 1, 10, 11)

	// The following produce empty ranges.
	// TODO(jba): Consider a multi-part key to illustrate partial key behavior.
	// checkRange(KeyRange{Key{"k7"}, Key{}, ClosedOpen})
	// checkRange(KeyRange{Key{"k7"}, Key{}, OpenOpen})
	// checkRange(KeyRange{Key{}, Key{"k11"}, OpenOpen})
	// checkRange(KeyRange{Key{}, Key{"k11"}, OpenClosed})

	// Prefix is component-wise, not string prefix.
	checkRange(Key{"k1"}.AsPrefix(), 1)
	checkRange(KeyRange{Key{"k1"}, Key{"k2"}, ClosedOpen}, 1, 10, 11, 12, 13, 14)

	checkRange(AllKeys(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
}

func indexRangeReads(ctx context.Context, t *testing.T, client *Client) {
	checkRange := func(ks KeySet, wantNums ...int) {
		if msg, ok := compareRows(client.Single().ReadUsingIndex(ctx, testTable, testTableIndex, ks, testTableColumns),
			wantNums); !ok {
			t.Errorf("key set %+v: %s", ks, msg)
		}
	}

	checkRange(Key{"v1"}, 1)
	checkRange(KeyRange{Key{"v3"}, Key{"v5"}, ClosedOpen}, 3, 4)
	checkRange(KeyRange{Key{"v3"}, Key{"v5"}, ClosedClosed}, 3, 4, 5)
	checkRange(KeyRange{Key{"v3"}, Key{"v5"}, OpenClosed}, 4, 5)
	checkRange(KeyRange{Key{"v3"}, Key{"v5"}, OpenOpen}, 4)

	// // Partial key specification.
	checkRange(KeyRange{Key{"v7"}, Key{}, ClosedClosed}, 7, 8, 9)
	checkRange(KeyRange{Key{"v7"}, Key{}, OpenClosed}, 8, 9)
	checkRange(KeyRange{Key{}, Key{"v11"}, ClosedOpen}, 0, 1, 10)
	checkRange(KeyRange{Key{}, Key{"v11"}, ClosedClosed}, 0, 1, 10, 11)

	// // The following produce empty ranges.
	// checkRange(KeyRange{Key{"v7"}, Key{}, ClosedOpen})
	// checkRange(KeyRange{Key{"v7"}, Key{}, OpenOpen})
	// checkRange(KeyRange{Key{}, Key{"v11"}, OpenOpen})
	// checkRange(KeyRange{Key{}, Key{"v11"}, OpenClosed})

	// // Prefix is component-wise, not string prefix.
	checkRange(Key{"v1"}.AsPrefix(), 1)
	checkRange(KeyRange{Key{"v1"}, Key{"v2"}, ClosedOpen}, 1, 10, 11, 12, 13, 14)
	checkRange(AllKeys(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)

	// Read from an index with DESC ordering.
	wantNums := []int{14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	if msg, ok := compareRows(client.Single().ReadUsingIndex(ctx, testTable, "TestTableByValueDesc", AllKeys(), testTableColumns),
		wantNums); !ok {
		t.Errorf("desc: %s", msg)
	}
}

func compareRows(iter *RowIterator, wantNums []int) (string, bool) {
	rows, err := readAllTestTable(iter)
	if err != nil {
		return err.Error(), false
	}
	want := map[string]string{}
	for _, n := range wantNums {
		want[fmt.Sprintf("k%d", n)] = fmt.Sprintf("v%d", n)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Key] = r.StringValue
	}
	if !testEqual(got, want) {
		return fmt.Sprintf("got %v, want %v", got, want), false
	}
	return "", true
}

func TestEarlyTimestamp(t *testing.T) {
	t.Parallel()
	// Test that we can get the timestamp from a read-only transaction as
	// soon as we have read at least one row.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Set up testing environment.
	client, _, tearDown := prepare(ctx, t, readDBStatements)
	defer tearDown()

	var ms []*Mutation
	for i := 0; i < 3; i++ {
		ms = append(ms, InsertOrUpdate(testTable,
			testTableColumns,
			[]interface{}{fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)}))
	}
	if _, err := client.Apply(ctx, ms, ApplyAtLeastOnce()); err != nil {
		t.Fatal(err)
	}

	txn := client.Single()
	iter := txn.Read(ctx, testTable, AllKeys(), testTableColumns)
	defer iter.Stop()
	// In  single-use transaction, we should get an error before reading anything.
	if _, err := txn.Timestamp(); err == nil {
		t.Error("wanted error, got nil")
	}
	// After reading one row, the timestamp should be available.
	_, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Timestamp(); err != nil {
		t.Errorf("got %v, want nil", err)
	}

	txn = client.ReadOnlyTransaction()
	defer txn.Close()
	iter = txn.Read(ctx, testTable, AllKeys(), testTableColumns)
	defer iter.Stop()
	// In an ordinary read-only transaction, the timestamp should be
	// available immediately.
	if _, err := txn.Timestamp(); err != nil {
		t.Errorf("got %v, want nil", err)
	}
}

func TestNestedTransaction(t *testing.T) {
	t.Parallel()
	// You cannot use a transaction from inside a read-write transaction.
	ctx := context.Background()
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *ReadWriteTransaction) error {
		_, err := client.ReadWriteTransaction(ctx,
			func(context.Context, *ReadWriteTransaction) error { return nil })
		if ErrCode(err) != codes.FailedPrecondition {
			t.Fatalf("got %v, want FailedPrecondition", err)
		}
		_, err = client.Single().ReadRow(ctx, "Singers", Key{1}, []string{"SingerId"})
		if ErrCode(err) != codes.FailedPrecondition {
			t.Fatalf("got %v, want FailedPrecondition", err)
		}
		rot := client.ReadOnlyTransaction()
		defer rot.Close()
		_, err = rot.ReadRow(ctx, "Singers", Key{1}, []string{"SingerId"})
		if ErrCode(err) != codes.FailedPrecondition {
			t.Fatalf("got %v, want FailedPrecondition", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Test client recovery on database recreation.
func TestDbRemovalRecovery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, dbPath, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	// Drop the testing database.
	if err := admin.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: dbPath}); err != nil {
		t.Fatalf("failed to drop testing database %v: %v", dbPath, err)
	}

	// Now, send the query.
	iter := client.Single().Query(ctx, Statement{SQL: "SELECT SingerId FROM Singers"})
	defer iter.Stop()
	if _, err := iter.Next(); err == nil {
		t.Errorf("client sends query to removed database successfully, want it to fail")
	}

	// Recreate database and table.
	dbName := dbPath[strings.LastIndex(dbPath, "/")+1:]
	op, err := admin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%v/instances/%v", testProjectID, testInstanceID),
		CreateStatement: "CREATE DATABASE " + dbName,
		ExtraStatements: []string{
			`CREATE TABLE Singers (
				SingerId        INT64 NOT NULL,
				FirstName       STRING(1024),
				LastName        STRING(1024),
				SingerInfo      BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
		},
	})
	if err != nil {
		t.Fatalf("cannot recreate testing DB %v: %v", dbPath, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("cannot recreate testing DB %v: %v", dbPath, err)
	}

	// Now, send the query again.
	iter = client.Single().Query(ctx, Statement{SQL: "SELECT SingerId FROM Singers"})
	defer iter.Stop()
	_, err = iter.Next()
	if err != nil && err != iterator.Done {
		t.Errorf("failed to send query to database %v: %v", dbPath, err)
	}
}

// Test encoding/decoding non-struct Cloud Spanner types.
func TestBasicTypes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()
	t1, _ := time.Parse(time.RFC3339Nano, "2016-11-15T15:04:05.999999999Z")
	// Boundaries
	t2, _ := time.Parse(time.RFC3339Nano, "0001-01-01T00:00:00.000000000Z")
	t3, _ := time.Parse(time.RFC3339Nano, "9999-12-31T23:59:59.999999999Z")
	d1, _ := civil.ParseDate("2016-11-15")
	// Boundaries
	d2, _ := civil.ParseDate("0001-01-01")
	d3, _ := civil.ParseDate("9999-12-31")

	tests := []struct {
		col  string
		val  interface{}
		want interface{}
	}{
		{col: "String", val: ""},
		{col: "String", val: "", want: NullString{"", true}},
		{col: "String", val: "foo"},
		{col: "String", val: "foo", want: NullString{"foo", true}},
		{col: "String", val: NullString{"bar", true}, want: "bar"},
		{col: "String", val: NullString{"bar", false}, want: NullString{"", false}},
		{col: "StringArray", val: []string(nil), want: []NullString(nil)},
		{col: "StringArray", val: []string{}, want: []NullString{}},
		{col: "StringArray", val: []string{"foo", "bar"}, want: []NullString{{"foo", true}, {"bar", true}}},
		{col: "StringArray", val: []NullString(nil)},
		{col: "StringArray", val: []NullString{}},
		{col: "StringArray", val: []NullString{{"foo", true}, {}}},
		{col: "Bytes", val: []byte{}},
		{col: "Bytes", val: []byte{1, 2, 3}},
		{col: "Bytes", val: []byte(nil)},
		{col: "BytesArray", val: [][]byte(nil)},
		{col: "BytesArray", val: [][]byte{}},
		{col: "BytesArray", val: [][]byte{{1}, {2, 3}}},
		{col: "Int64a", val: 0, want: int64(0)},
		{col: "Int64a", val: -1, want: int64(-1)},
		{col: "Int64a", val: 2, want: int64(2)},
		{col: "Int64a", val: int64(3)},
		{col: "Int64a", val: 4, want: NullInt64{4, true}},
		{col: "Int64a", val: NullInt64{5, true}, want: int64(5)},
		{col: "Int64a", val: NullInt64{6, true}, want: int64(6)},
		{col: "Int64a", val: NullInt64{7, false}, want: NullInt64{0, false}},
		{col: "Int64Array", val: []int(nil), want: []NullInt64(nil)},
		{col: "Int64Array", val: []int{}, want: []NullInt64{}},
		{col: "Int64Array", val: []int{1, 2}, want: []NullInt64{{1, true}, {2, true}}},
		{col: "Int64Array", val: []int64(nil), want: []NullInt64(nil)},
		{col: "Int64Array", val: []int64{}, want: []NullInt64{}},
		{col: "Int64Array", val: []int64{1, 2}, want: []NullInt64{{1, true}, {2, true}}},
		{col: "Int64Array", val: []NullInt64(nil)},
		{col: "Int64Array", val: []NullInt64{}},
		{col: "Int64Array", val: []NullInt64{{1, true}, {}}},
		{col: "Bool", val: false},
		{col: "Bool", val: true},
		{col: "Bool", val: false, want: NullBool{false, true}},
		{col: "Bool", val: true, want: NullBool{true, true}},
		{col: "Bool", val: NullBool{true, true}},
		{col: "Bool", val: NullBool{false, false}},
		{col: "BoolArray", val: []bool(nil), want: []NullBool(nil)},
		{col: "BoolArray", val: []bool{}, want: []NullBool{}},
		{col: "BoolArray", val: []bool{true, false}, want: []NullBool{{true, true}, {false, true}}},
		{col: "BoolArray", val: []NullBool(nil)},
		{col: "BoolArray", val: []NullBool{}},
		{col: "BoolArray", val: []NullBool{{false, true}, {true, true}, {}}},
		{col: "Float64", val: 0.0},
		{col: "Float64", val: 3.14},
		{col: "Float64", val: math.NaN()},
		{col: "Float64", val: math.Inf(1)},
		{col: "Float64", val: math.Inf(-1)},
		{col: "Float64", val: 2.78, want: NullFloat64{2.78, true}},
		{col: "Float64", val: NullFloat64{2.71, true}, want: 2.71},
		{col: "Float64", val: NullFloat64{1.41, true}, want: NullFloat64{1.41, true}},
		{col: "Float64", val: NullFloat64{0, false}},
		{col: "Float64Array", val: []float64(nil), want: []NullFloat64(nil)},
		{col: "Float64Array", val: []float64{}, want: []NullFloat64{}},
		{col: "Float64Array", val: []float64{2.72, 3.14, math.Inf(1)}, want: []NullFloat64{{2.72, true}, {3.14, true}, {math.Inf(1), true}}},
		{col: "Float64Array", val: []NullFloat64(nil)},
		{col: "Float64Array", val: []NullFloat64{}},
		{col: "Float64Array", val: []NullFloat64{{2.72, true}, {math.Inf(1), true}, {}}},
		{col: "Date", val: d1},
		{col: "Date", val: d1, want: NullDate{d1, true}},
		{col: "Date", val: NullDate{d1, true}},
		{col: "Date", val: NullDate{d1, true}, want: d1},
		{col: "Date", val: NullDate{civil.Date{}, false}},
		{col: "DateArray", val: []civil.Date(nil), want: []NullDate(nil)},
		{col: "DateArray", val: []civil.Date{}, want: []NullDate{}},
		{col: "DateArray", val: []civil.Date{d1, d2, d3}, want: []NullDate{{d1, true}, {d2, true}, {d3, true}}},
		{col: "Timestamp", val: t1},
		{col: "Timestamp", val: t1, want: NullTime{t1, true}},
		{col: "Timestamp", val: NullTime{t1, true}},
		{col: "Timestamp", val: NullTime{t1, true}, want: t1},
		{col: "Timestamp", val: NullTime{}},
		{col: "TimestampArray", val: []time.Time(nil), want: []NullTime(nil)},
		{col: "TimestampArray", val: []time.Time{}, want: []NullTime{}},
		{col: "TimestampArray", val: []time.Time{t1, t2, t3}, want: []NullTime{{t1, true}, {t2, true}, {t3, true}}},
	}

	// Write rows into table first.
	var muts []*Mutation
	for i, test := range tests {
		muts = append(muts, InsertOrUpdate("Types", []string{"RowID", test.col}, []interface{}{i, test.val}))
	}
	if _, err := client.Apply(ctx, muts, ApplyAtLeastOnce()); err != nil {
		t.Fatal(err)
	}

	for i, test := range tests {
		row, err := client.Single().ReadRow(ctx, "Types", []interface{}{i}, []string{test.col})
		if err != nil {
			t.Fatalf("Unable to fetch row %v: %v", i, err)
		}
		// Create new instance of type of test.want.
		want := test.want
		if want == nil {
			want = test.val
		}
		gotp := reflect.New(reflect.TypeOf(want))
		if err := row.Column(0, gotp.Interface()); err != nil {
			t.Errorf("%d: col:%v val:%#v, %v", i, test.col, test.val, err)
			continue
		}
		got := reflect.Indirect(gotp).Interface()

		// One of the test cases is checking NaN handling.  Given
		// NaN!=NaN, we can't use reflect to test for it.
		if isNaN(got) && isNaN(want) {
			continue
		}

		// Check non-NaN cases.
		if !testEqual(got, want) {
			t.Errorf("%d: col:%v val:%#v, got %#v, want %#v", i, test.col, test.val, got, want)
			continue
		}
	}
}

// Test decoding Cloud Spanner STRUCT type.
func TestStructTypes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	tests := []struct {
		q    Statement
		want func(r *Row) error
	}{
		{
			q: Statement{SQL: `SELECT ARRAY(SELECT STRUCT(1, 2))`},
			want: func(r *Row) error {
				// Test STRUCT ARRAY decoding to []NullRow.
				var rows []NullRow
				if err := r.Column(0, &rows); err != nil {
					return err
				}
				if len(rows) != 1 {
					return fmt.Errorf("len(rows) = %d; want 1", len(rows))
				}
				if !rows[0].Valid {
					return fmt.Errorf("rows[0] is NULL")
				}
				var i, j int64
				if err := rows[0].Row.Columns(&i, &j); err != nil {
					return err
				}
				if i != 1 || j != 2 {
					return fmt.Errorf("got (%d,%d), want (1,2)", i, j)
				}
				return nil
			},
		},
		{
			q: Statement{SQL: `SELECT ARRAY(SELECT STRUCT(1 as foo, 2 as bar)) as col1`},
			want: func(r *Row) error {
				// Test Row.ToStruct.
				s := struct {
					Col1 []*struct {
						Foo int64 `spanner:"foo"`
						Bar int64 `spanner:"bar"`
					} `spanner:"col1"`
				}{}
				if err := r.ToStruct(&s); err != nil {
					return err
				}
				want := struct {
					Col1 []*struct {
						Foo int64 `spanner:"foo"`
						Bar int64 `spanner:"bar"`
					} `spanner:"col1"`
				}{
					Col1: []*struct {
						Foo int64 `spanner:"foo"`
						Bar int64 `spanner:"bar"`
					}{
						{
							Foo: 1,
							Bar: 2,
						},
					},
				}
				if !testEqual(want, s) {
					return fmt.Errorf("unexpected decoding result: %v, want %v", s, want)
				}
				return nil
			},
		},
	}
	for i, test := range tests {
		iter := client.Single().Query(ctx, test.q)
		defer iter.Stop()
		row, err := iter.Next()
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}
		if err := test.want(row); err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}
	}
}

func TestStructParametersUnsupported(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _, tearDown := prepare(ctx, t, nil)
	defer tearDown()

	for _, test := range []struct {
		param       interface{}
		wantCode    codes.Code
		wantMsgPart string
	}{
		{
			struct {
				Field int
			}{10},
			codes.Unimplemented,
			"Unsupported query shape: " +
				"A struct value cannot be returned as a column value. " +
				"Rewrite the query to flatten the struct fields in the result.",
		},
		{
			[]struct {
				Field int
			}{{10}, {20}},
			codes.Unimplemented,
			"Unsupported query shape: " +
				"This query can return a null-valued array of struct, " +
				"which is not supported by Spanner.",
		},
	} {
		iter := client.Single().Query(ctx, Statement{
			SQL:    "SELECT @p",
			Params: map[string]interface{}{"p": test.param},
		})
		_, err := iter.Next()
		iter.Stop()
		if msg, ok := matchError(err, test.wantCode, test.wantMsgPart); !ok {
			t.Fatal(msg)
		}
	}
}

// Test queries of the form "SELECT expr".
func TestQueryExpressions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _, tearDown := prepare(ctx, t, nil)
	defer tearDown()

	newRow := func(vals []interface{}) *Row {
		row, err := NewRow(make([]string, len(vals)), vals)
		if err != nil {
			t.Fatal(err)
		}
		return row
	}

	tests := []struct {
		expr string
		want interface{}
	}{
		{"1", int64(1)},
		{"[1, 2, 3]", []NullInt64{{1, true}, {2, true}, {3, true}}},
		{"[1, NULL, 3]", []NullInt64{{1, true}, {0, false}, {3, true}}},
		{"IEEE_DIVIDE(1, 0)", math.Inf(1)},
		{"IEEE_DIVIDE(-1, 0)", math.Inf(-1)},
		{"IEEE_DIVIDE(0, 0)", math.NaN()},
		// TODO(jba): add IEEE_DIVIDE(0, 0) to the following array when we have a better equality predicate.
		{"[IEEE_DIVIDE(1, 0), IEEE_DIVIDE(-1, 0)]", []NullFloat64{{math.Inf(1), true}, {math.Inf(-1), true}}},
		{"ARRAY(SELECT AS STRUCT * FROM (SELECT 'a', 1) WHERE 0 = 1)", []NullRow{}},
		{"ARRAY(SELECT STRUCT(1, 2))", []NullRow{{Row: *newRow([]interface{}{1, 2}), Valid: true}}},
	}
	for _, test := range tests {
		iter := client.Single().Query(ctx, Statement{SQL: "SELECT " + test.expr})
		defer iter.Stop()
		row, err := iter.Next()
		if err != nil {
			t.Errorf("%q: %v", test.expr, err)
			continue
		}
		// Create new instance of type of test.want.
		gotp := reflect.New(reflect.TypeOf(test.want))
		if err := row.Column(0, gotp.Interface()); err != nil {
			t.Errorf("%q: Column returned error %v", test.expr, err)
			continue
		}
		got := reflect.Indirect(gotp).Interface()
		// TODO(jba): remove isNaN special case when we have a better equality predicate.
		if isNaN(got) && isNaN(test.want) {
			continue
		}
		if !testEqual(got, test.want) {
			t.Errorf("%q\n got  %#v\nwant %#v", test.expr, got, test.want)
		}
	}
}

func TestQueryStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	accounts := []*Mutation{
		Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(1), "Foo", int64(50)}),
		Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(2), "Bar", int64(1)}),
	}
	if _, err := client.Apply(ctx, accounts, ApplyAtLeastOnce()); err != nil {
		t.Fatal(err)
	}
	const sql = "SELECT Balance FROM Accounts"

	qp, err := client.Single().AnalyzeQuery(ctx, Statement{sql, nil})
	if err != nil {
		t.Fatal(err)
	}
	if len(qp.PlanNodes) == 0 {
		t.Error("got zero plan nodes, expected at least one")
	}

	iter := client.Single().QueryWithStats(ctx, Statement{sql, nil})
	defer iter.Stop()
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if iter.QueryPlan == nil {
		t.Error("got nil QueryPlan, expected one")
	}
	if iter.QueryStats == nil {
		t.Error("got nil QueryStats, expected some")
	}
}

func isNaN(x interface{}) bool {
	f, ok := x.(float64)
	if !ok {
		return false
	}
	return math.IsNaN(f)
}

func TestInvalidDatabase(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Integration tests skipped in short mode")
	}
	if testProjectID == "" {
		t.Skip("Integration tests skipped: GCLOUD_TESTS_GOLANG_PROJECT_ID is missing")
	}
	ctx := context.Background()
	ts := testutil.TokenSource(ctx, Scope)
	if ts == nil {
		t.Skip("Integration test skipped: cannot get service account credential from environment variable GCLOUD_TESTS_GOLANG_KEY")
	}
	db := fmt.Sprintf("projects/%v/instances/%v/databases/invalid", testProjectID, testInstanceID)
	c, err := NewClient(ctx, db, option.WithTokenSource(ts))
	// Client creation should succeed even if the database is invalid.
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Single().ReadRow(ctx, "TestTable", Key{1}, []string{"col1"})
	if msg, ok := matchError(err, codes.NotFound, ""); !ok {
		t.Fatal(msg)
	}
}

func TestReadErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _, tearDown := prepare(ctx, t, readDBStatements)
	defer tearDown()

	// Read over invalid table fails
	_, err := client.Single().ReadRow(ctx, "badTable", Key{1}, []string{"StringValue"})
	if msg, ok := matchError(err, codes.NotFound, "badTable"); !ok {
		t.Error(msg)
	}
	// Read over invalid column fails
	_, err = client.Single().ReadRow(ctx, "TestTable", Key{1}, []string{"badcol"})
	if msg, ok := matchError(err, codes.NotFound, "badcol"); !ok {
		t.Error(msg)
	}

	// Invalid query fails
	iter := client.Single().Query(ctx, Statement{SQL: "SELECT Apples AND Oranges"})
	defer iter.Stop()
	_, err = iter.Next()
	if msg, ok := matchError(err, codes.InvalidArgument, "unrecognized name"); !ok {
		t.Error(msg)
	}

	// Read should fail on cancellation.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = client.Single().ReadRow(cctx, "TestTable", Key{1}, []string{"StringValue"})
	if msg, ok := matchError(err, codes.Canceled, ""); !ok {
		t.Error(msg)
	}
	// Read should fail if deadline exceeded.
	dctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()
	<-dctx.Done()
	_, err = client.Single().ReadRow(dctx, "TestTable", Key{1}, []string{"StringValue"})
	if msg, ok := matchError(err, codes.DeadlineExceeded, ""); !ok {
		t.Error(msg)
	}
}

func matchError(got error, wantCode codes.Code, wantMsgPart string) (string, bool) {
	if ErrCode(got) != wantCode || !strings.Contains(strings.ToLower(ErrDesc(got)), strings.ToLower(wantMsgPart)) {
		return fmt.Sprintf("got error <%v>\n"+`want <code = %q, "...%s...">`, got, wantCode, wantMsgPart), false
	}
	return "", true
}

func rowToValues(r *Row) ([]interface{}, error) {
	var x int64
	var y, z string
	if err := r.Column(0, &x); err != nil {
		return nil, err
	}
	if err := r.Column(1, &y); err != nil {
		return nil, err
	}
	if err := r.Column(2, &z); err != nil {
		return nil, err
	}
	return []interface{}{x, y, z}, nil
}

func readAll(iter *RowIterator) ([][]interface{}, error) {
	defer iter.Stop()
	var vals [][]interface{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return vals, nil
		}
		if err != nil {
			return nil, err
		}
		v, err := rowToValues(row)
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
}

func readAllTestTable(iter *RowIterator) ([]testTableRow, error) {
	defer iter.Stop()
	var vals []testTableRow
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return vals, nil
		}
		if err != nil {
			return nil, err
		}
		var ttr testTableRow
		if err := row.ToStruct(&ttr); err != nil {
			return nil, err
		}
		vals = append(vals, ttr)
	}
}

// Test TransactionRunner. Test that transactions are aborted and retried as expected.
func TestTransactionRunner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, _, tearDown := prepare(ctx, t, singerDBStatements)
	defer tearDown()

	// Test 1: User error should abort the transaction.
	_, _ = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *ReadWriteTransaction) error {
		tx.BufferWrite([]*Mutation{
			Insert("Accounts", []string{"AccountId", "Nickname", "Balance"}, []interface{}{int64(1), "Foo", int64(50)})})
		return errors.New("user error")
	})
	// Empty read.
	rows, err := readAllTestTable(client.Single().Read(ctx, "Accounts", Key{1}, []string{"AccountId", "Nickname", "Balance"}))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(rows), 0; got != want {
		t.Errorf("Empty read, got %d, want %d.", got, want)
	}

	// Test 2: Expect abort and retry.
	// We run two ReadWriteTransactions concurrently and make txn1 abort txn2 by committing writes to the column txn2 have read,
	// and expect the following read to abort and txn2 retries.

	// Set up two accounts
	accounts := []*Mutation{
		Insert("Accounts", []string{"AccountId", "Balance"}, []interface{}{int64(1), int64(0)}),
		Insert("Accounts", []string{"AccountId", "Balance"}, []interface{}{int64(2), int64(1)}),
	}
	if _, err := client.Apply(ctx, accounts, ApplyAtLeastOnce()); err != nil {
		t.Fatal(err)
	}

	var (
		cTxn1Start  = make(chan struct{})
		cTxn1Commit = make(chan struct{})
		cTxn2Start  = make(chan struct{})
		wg          sync.WaitGroup
	)

	// read balance, check error if we don't expect abort.
	readBalance := func(tx interface {
		ReadRow(ctx context.Context, table string, key Key, columns []string) (*Row, error)
	}, key int64, expectAbort bool) (int64, error) {
		var b int64
		r, e := tx.ReadRow(ctx, "Accounts", Key{int64(key)}, []string{"Balance"})
		if e != nil {
			if expectAbort && !isAbortErr(e) {
				t.Errorf("ReadRow got %v, want Abort error.", e)
			}
			return b, e
		}
		if ce := r.Column(0, &b); ce != nil {
			return b, ce
		}
		return b, nil
	}

	wg.Add(2)
	// Txn 1
	go func() {
		defer wg.Done()
		var once sync.Once
		_, e := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *ReadWriteTransaction) error {
			b, e := readBalance(tx, 1, false)
			if e != nil {
				return e
			}
			// txn 1 can abort, in that case we skip closing the channel on retry.
			once.Do(func() { close(cTxn1Start) })
			e = tx.BufferWrite([]*Mutation{
				Update("Accounts", []string{"AccountId", "Balance"}, []interface{}{int64(1), int64(b + 1)})})
			if e != nil {
				return e
			}
			// Wait for second transaction.
			<-cTxn2Start
			return nil
		})
		close(cTxn1Commit)
		if e != nil {
			t.Errorf("Transaction 1 commit, got %v, want nil.", e)
		}
	}()
	// Txn 2
	go func() {
		// Wait until txn 1 starts.
		<-cTxn1Start
		defer wg.Done()
		var (
			once sync.Once
			b1   int64
			b2   int64
			e    error
		)
		_, e = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *ReadWriteTransaction) error {
			if b1, e = readBalance(tx, 1, false); e != nil {
				return e
			}
			// Skip closing channel on retry.
			once.Do(func() { close(cTxn2Start) })
			// Wait until txn 1 successfully commits.
			<-cTxn1Commit
			// Txn1 has committed and written a balance to the account.
			// Now this transaction (txn2) reads and re-writes the balance.
			// The first time through, it will abort because it overlaps with txn1.
			// Then it will retry after txn1 commits, and succeed.
			if b2, e = readBalance(tx, 2, true); e != nil {
				return e
			}
			return tx.BufferWrite([]*Mutation{
				Update("Accounts", []string{"AccountId", "Balance"}, []interface{}{int64(2), int64(b1 + b2)})})
		})
		if e != nil {
			t.Errorf("Transaction 2 commit, got %v, want nil.", e)
		}
	}()
	wg.Wait()
	// Check that both transactions' effects are visible.
	for i := int64(1); i <= int64(2); i++ {
		if b, e := readBalance(client.Single(), i, false); e != nil {
			t.Fatalf("ReadBalance for key %d error %v.", i, e)
		} else if b != i {
			t.Errorf("Balance for key %d, got %d, want %d.", i, b, i)
		}
	}
}

// createClient creates Cloud Spanner data client.
func createClient(ctx context.Context, dbPath string) (client *Client, err error) {
	client, err = NewClientWithConfig(ctx, dbPath, ClientConfig{
		SessionPoolConfig: SessionPoolConfig{WriteSessions: 0.2},
	}, option.WithTokenSource(testutil.TokenSource(ctx, Scope)), option.WithEndpoint(endpoint))
	if err != nil {
		return nil, fmt.Errorf("cannot create data client on DB %v: %v", dbPath, err)
	}
	return client, nil
}

// populate prepares the database with some data.
func populate(ctx context.Context, client *Client) error {
	// Populate data
	var err error
	m := InsertMap("test", map[string]interface{}{
		"a": str1,
		"b": str2,
	})
	_, err = client.Apply(ctx, []*Mutation{m})
	return err
}

// Test PartitionQuery of BatchReadOnlyTransaction, create partitions then
// serialize and deserialize both transaction and partition to be used in
// execution on another client, and compare results.
func TestBatchQuery(t *testing.T) {
	t.Parallel()
	// Set up testing environment.
	var (
		client2 *Client
		err     error
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, dbPath, tearDown := prepare(ctx, t, simpleDBStatements)
	defer tearDown()
	if err = populate(ctx, client); err != nil {
		t.Fatal(err)
	}
	if client2, err = createClient(ctx, dbPath); err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// PartitionQuery
	var (
		txn        *BatchReadOnlyTransaction
		partitions []*Partition
		stmt       = Statement{SQL: "SELECT * FROM test;"}
	)

	if txn, err = client.BatchReadOnlyTransaction(ctx, StrongRead()); err != nil {
		t.Fatal(err)
	}
	defer txn.Cleanup(ctx)
	if partitions, err = txn.PartitionQuery(ctx, stmt, PartitionOptions{0, 3}); err != nil {
		t.Fatal(err)
	}

	// Reconstruct BatchReadOnlyTransactionID and execute partitions
	var (
		tid2      BatchReadOnlyTransactionID
		data      []byte
		gotResult bool // if we get matching result from two separate txns
	)
	if data, err = txn.ID.MarshalBinary(); err != nil {
		t.Fatalf("encoding failed %v", err)
	}
	if err = tid2.UnmarshalBinary(data); err != nil {
		t.Fatalf("decoding failed %v", err)
	}
	txn2 := client2.BatchReadOnlyTransactionFromID(tid2)

	// Execute Partitions and compare results
	for i, p := range partitions {
		iter := txn.Execute(ctx, p)
		defer iter.Stop()
		p2 := serdesPartition(t, i, p)
		iter2 := txn2.Execute(ctx, &p2)
		defer iter2.Stop()

		row1, err1 := iter.Next()
		row2, err2 := iter2.Next()
		if err1 != err2 {
			t.Fatalf("execution failed for different reasons: %v, %v", err1, err2)
			continue
		}
		if !testEqual(row1, row2) {
			t.Fatalf("execution returned different values: %v, %v", row1, row2)
			continue
		}
		if row1 == nil {
			continue
		}
		var a, b string
		if err = row1.Columns(&a, &b); err != nil {
			t.Fatalf("failed to parse row %v", err)
			continue
		}
		if a == str1 && b == str2 {
			gotResult = true
		}
	}
	if !gotResult {
		t.Fatalf("execution didn't return expected values")
	}
}

// Test PartitionRead of BatchReadOnlyTransaction, similar to TestBatchQuery
func TestBatchRead(t *testing.T) {
	t.Parallel()
	// Set up testing environment.
	var (
		client2 *Client
		err     error
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, dbPath, tearDown := prepare(ctx, t, simpleDBStatements)
	defer tearDown()
	if err = populate(ctx, client); err != nil {
		t.Fatal(err)
	}
	if client2, err = createClient(ctx, dbPath); err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// PartitionRead
	var (
		txn        *BatchReadOnlyTransaction
		partitions []*Partition
	)

	if txn, err = client.BatchReadOnlyTransaction(ctx, StrongRead()); err != nil {
		t.Fatal(err)
	}
	defer txn.Cleanup(ctx)
	if partitions, err = txn.PartitionRead(ctx, "test", AllKeys(), simpleDBTableColumns, PartitionOptions{0, 3}); err != nil {
		t.Fatal(err)
	}

	// Reconstruct BatchReadOnlyTransactionID and execute partitions
	var (
		tid2      BatchReadOnlyTransactionID
		data      []byte
		gotResult bool // if we get matching result from two separate txns
	)
	if data, err = txn.ID.MarshalBinary(); err != nil {
		t.Fatalf("encoding failed %v", err)
	}
	if err = tid2.UnmarshalBinary(data); err != nil {
		t.Fatalf("decoding failed %v", err)
	}
	txn2 := client2.BatchReadOnlyTransactionFromID(tid2)

	// Execute Partitions and compare results
	for i, p := range partitions {
		iter := txn.Execute(ctx, p)
		defer iter.Stop()
		p2 := serdesPartition(t, i, p)
		iter2 := txn2.Execute(ctx, &p2)
		defer iter2.Stop()

		row1, err1 := iter.Next()
		row2, err2 := iter2.Next()
		if err1 != err2 {
			t.Fatalf("execution failed for different reasons: %v, %v", err1, err2)
			continue
		}
		if !testEqual(row1, row2) {
			t.Fatalf("execution returned different values: %v, %v", row1, row2)
			continue
		}
		if row1 == nil {
			continue
		}
		var a, b string
		if err = row1.Columns(&a, &b); err != nil {
			t.Fatalf("failed to parse row %v", err)
			continue
		}
		if a == str1 && b == str2 {
			gotResult = true
		}
	}
	if !gotResult {
		t.Fatalf("execution didn't return expected values")
	}
}

// Test normal txReadEnv method on BatchReadOnlyTransaction.
func TestBROTNormal(t *testing.T) {
	t.Parallel()
	// Set up testing environment and create txn.
	var (
		txn *BatchReadOnlyTransaction
		err error
		row *Row
		i   int64
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, _, tearDown := prepare(ctx, t, simpleDBStatements)
	defer tearDown()

	if txn, err = client.BatchReadOnlyTransaction(ctx, StrongRead()); err != nil {
		t.Fatal(err)
	}
	defer txn.Cleanup(ctx)
	if _, err := txn.PartitionRead(ctx, "test", AllKeys(), simpleDBTableColumns, PartitionOptions{0, 3}); err != nil {
		t.Fatal(err)
	}
	// Normal query should work with BatchReadOnlyTransaction
	stmt2 := Statement{SQL: "SELECT 1"}
	iter := txn.Query(ctx, stmt2)
	defer iter.Stop()

	row, err = iter.Next()
	if err != nil {
		t.Errorf("query failed with %v", err)
	}
	if err = row.Columns(&i); err != nil {
		t.Errorf("failed to parse row %v", err)
	}
}

func TestCommitTimestamp(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, _, tearDown := prepare(ctx, t, ctsDBStatements)
	defer tearDown()

	type testTableRow struct {
		Key string
		Ts  NullTime
	}

	var (
		cts1, cts2, ts1, ts2 time.Time
		err                  error
	)

	// Apply mutation in sequence, expect to see commit timestamp in good order, check also the commit timestamp returned
	for _, it := range []struct {
		k string
		t *time.Time
	}{
		{"a", &cts1},
		{"b", &cts2},
	} {
		tt := testTableRow{Key: it.k, Ts: NullTime{CommitTimestamp, true}}
		m, err := InsertStruct("TestTable", tt)
		if err != nil {
			t.Fatal(err)
		}
		*it.t, err = client.Apply(ctx, []*Mutation{m}, ApplyAtLeastOnce())
		if err != nil {
			t.Fatal(err)
		}
	}

	txn := client.ReadOnlyTransaction()
	for _, it := range []struct {
		k string
		t *time.Time
	}{
		{"a", &ts1},
		{"b", &ts2},
	} {
		if r, e := txn.ReadRow(ctx, "TestTable", Key{it.k}, []string{"Ts"}); e != nil {
			t.Fatal(err)
		} else {
			var got testTableRow
			if err := r.ToStruct(&got); err != nil {
				t.Fatal(err)
			}
			*it.t = got.Ts.Time
		}
	}
	if !cts1.Equal(ts1) {
		t.Errorf("Expect commit timestamp returned and read to match for txn1, got %v and %v.", cts1, ts1)
	}
	if !cts2.Equal(ts2) {
		t.Errorf("Expect commit timestamp returned and read to match for txn2, got %v and %v.", cts2, ts2)
	}

	// Try writing a timestamp in the future to commit timestamp, expect error
	_, err = client.Apply(ctx, []*Mutation{InsertOrUpdate("TestTable", []string{"Key", "Ts"}, []interface{}{"a", time.Now().Add(time.Hour)})}, ApplyAtLeastOnce())
	if msg, ok := matchError(err, codes.FailedPrecondition, "Cannot write timestamps in the future"); !ok {
		t.Error(msg)
	}
}
