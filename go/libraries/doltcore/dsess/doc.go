// Copyright 2022 Dolthub, Inc.
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

package dsess

/*

The dsess package is responsible for storing the state of every database in each session.

The major players in this process are:

* sqle.Database: The database implementation we provide to integrate with go-mysql-server's interface is mostly a
	wrapper to provide access to the actual storage of tables and rows that are held by dsess.Session.
* sqle.DatabaseProvider: Responsible for creating a new sqle.Database for each database name asked for by the engine,
	as well as for managing the details of replication on the databases it returns.
* dsess.Session: Responsible for maintaining the state of each session, including the data access for any row data.
	Each physical dolt database in the provider can have the state of multiple branch heads managed by a session. This
	state is loaded on demand from the provider as the client asks for different databases by name, as `dolt_checkout`
  is called, etc.
* dsess.DoltTransaction: Records a start state (noms root) for each database managed by the transaction. Responsible
	for committing new data as the result of a COMMIT or dolt_commit() by merging this start state with session changes
	as appropriate.

The rough flow of data between the engine and this package:

1) START TRANSACTION calls dsess.Session.StartTransaction() to create a new dsess.DoltTransaction. This transaction
	takes a snapshot of the current noms root for each database known to the provider and records these as part of the
	transaction. This method clears out all cached state.
2) The engine calls DatabaseProvider.Database() to get a sqle.Database for each database name included in a query,
	including statements like `USE db`.
3) Databases have access to tables, views, and other schema elements that they provide to the engine upon request as
	part of query analysis, row iteration, etc. As a rule, this data is loaded from the session when asked for. Databases,
	tables, views, and other structures in the sqle package are best understood as pass-through entities that always
	defer to the session for their actual data.
4) When actual data is required, a table or other schema element asks the session for the data. The primary interface
	for this exchange is Session.LookupDbState(), which takes a database name.
5) Eventually, the client session issues a COMMIT or DOLT_COMMIT() statement. This calls Session.CommitTransaction(),
	which enforces business logic rules around the commit and then calls DoltTransaction.Commit() to persist the changes.

Databases managed by the provider and the session can be referred to by either a base name (myDb) or a fully qualified
name (myDb/myBranch). The details of this are a little bit subtle:

* Database names that aren't qualified with a revision specifier resolve to either a) the default branch head, or
	b) whatever branch head was last checked out with dolt_checkout(). Changing the branch head referred to by an
	unqualified database name is the primary purpose of dolt_checkout().
* `mydb/branch` always resolves to that branch head, as it existed at transaction start
* Database names exposed to the engine are always `mydb`, never `mydb/branch`. This includes the result of
	`select database()`. This is because the engine expects base database names when e.g. checking GRANTs, returning
	information the information schema table, etc.
* sqle.Database has an external name it exposes to the engine via Name(), as well as an internal name that includes a
	revision qualifier, RevisionQualifiedName(). The latter should always be used internally when accessing session data,
	including rows and all other table data. It's only appropriate to use an unqualified database name when you want
  the current checked out HEAD.

It's possible to alter the data on multiple HEADS in a single session, but we currently restrict the users to
committing a single one. It doesn't need to be the checked out head -- we simply look for a single dirty branch head
state and commit that one. If there is more than one, it's an error. We may allow multiple branch heads to be updated
in a single transaction in the future.

*/
