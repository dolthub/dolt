/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * MySQL.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of MySQL Connector/C++, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#include <test.h>
#include <iostream>
#include <list>

using std::cout;
using std::endl;
using namespace mysqlx;

class Batch : public mysqlx::test::Xplugin
{
};


unsigned show_docs(Collection &coll)
{
  DocResult res = coll.find().execute();

  unsigned count;
  DbDoc doc;

  cout << "== documents in the collection ==" << endl;

  for (count = 0; (doc = res.fetchOne()); ++count)
  {
    cout << "doc#" << count << ": " << doc << endl;
  }

  cout << "== there are " << count << " documents ==" << endl;
  return count;
}


/*
  Test CRUD multi operations such as inserting several documents
  or performing several modifications by a single CRUD operation.
*/

TEST_F(Batch, crud)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Creating session..." << endl;

  Session sess(this);

  cout << "Session accepted, creating collection..." << endl;

  Schema sch = sess.getSchema("test");
  Collection coll = sch.createCollection("c1", true);

  coll.remove("true").execute();

  {
    RowResult res = sql("select count(*) from test.c1");
    unsigned  cnt = res.fetchOne()[0];
    EXPECT_EQ(0, cnt);
  }

  cout << "Inserting documents..." << endl;

  // Add several documents in single add operation.

  static string docs[] = {
    "{ \"_id\": \"id1\", \"name\": \"foo\", \"age\": 1 }",
    "{ \"_id\": \"id2\", \"name\": \"bar\", \"age\": 2 }",
    "{ \"_id\": \"id3\", \"name\": \"baz\", \"age\": 3, \"date\": { \"day\": 20, \"month\": \"Apr\" }}",
    "{ \"_id\": \"id4\", \"name\": \"foo\", \"age\": 7 }",
    "{ \"_id\": \"id5\", \"name\": \"buz\", \"age\": 17 }"
  };

  {
    CollectionAdd add(coll);

    for (unsigned pos = 0; pos < sizeof(docs) / sizeof(string); ++pos)
    {
      add.add(docs[pos]);
    }

    add.execute();
  }

  {
    RowResult res = sql("select count(*) from test.c1");
    unsigned  cnt = res.fetchOne()[0];
    EXPECT_EQ(5, cnt);
    cout << "Documents added" << endl;
  }

  cout << "Atomicity check..." << endl;

  /*
    Check that multi-add operation is atomic.

    None of the documents below should be added because the third
    document has non-unique id.
  */

  static string docs1[] = {
    "{ \"_id\": \"id6\", \"name\": \"atomic test\" }",
    "{ \"_id\": \"id7\", \"name\": \"atomic test\" }",
    "{ \"_id\": \"id1\", \"name\": \"atomic test\" }",
    "{ \"_id\": \"id8\", \"name\": \"atomic test\" }",
    "{ \"_id\": \"id6\", \"name\": \"atomic test\" }",
  };

  {
    CollectionAdd add = coll.add(docs1[0]);

    for (unsigned pos = 1; pos < sizeof(docs1) / sizeof(string); ++pos)
    {
      add.add(docs1[pos]);
    }

    EXPECT_THROW(add.execute(),Error);
  }

  {
    DocResult find = coll.find("name = 'atomic test'").execute();
    EXPECT_FALSE(find.fetchOne());
    RowResult res = sql("select count(*) from test.c1");
    unsigned  cnt = res.fetchOne()[0];
    EXPECT_EQ(5, cnt);
  }

  cout << "Modifying documents..." << endl;

  // Perform several modifications in single modify operation.

  {
    CollectionModify modify(coll, "true");

    modify.set("age", expr("2*age"));
    modify.unset("date");
    modify.set("food", expr("[]"));
    modify.arrayAppend("food", "milk");
    modify.arrayAppend("food", "soup");
    modify.arrayAppend("food", "potatoes").execute();
  }

  {
    DocResult find = coll.find().execute();
    unsigned pos = 0;


    for (DbDoc doc; (doc = find.fetchOne()); ++pos)
    {
      cout << " -doc#" << pos <<": " << doc << endl;
      EXPECT_NO_THROW(doc["food"]);
      EXPECT_THROW(doc["date"], std::out_of_range);
    }
    EXPECT_EQ(5, pos);
  }

  cout << "Done!" << endl;
}


TEST_F(Batch, multi_add)
{
  SKIP_IF_NO_XPLUGIN;

  Collection coll = getSchema("test").createCollection("multi_add", true);

  std::vector<string> docs = {
    "{ \"foo\": 1 }",
    "{ \"foo\": 2 }",
    "{ \"foo\": 3 }",
    "{ \"foo\": 4 }",
    "{ \"foo\": 5 }",
  };

  cout << endl << "1. Adding documents from a container" << endl;

  coll.remove("true").execute();
  coll.add(docs).execute();
  EXPECT_EQ(5, show_docs(coll));

  cout << endl << "2. Add range of documents from 1 to 3" << endl;

  coll.remove("true").execute();
  coll.add(docs.begin(), docs.begin() + 3).execute();
  EXPECT_EQ(3, show_docs(coll));

  cout << endl << "3. Mixed inserts" << endl;

  coll.remove("true").execute();
  coll.add(docs)
    .add(docs[0])
    .add(docs.begin(), docs.begin() + 3)
    .add(docs[4])
    .add(docs).execute();
  EXPECT_EQ(15, show_docs(coll));

  cout << endl << "4. Add documents in a loop" << endl;

  {
    coll.remove("true").execute();
    CollectionAdd add_op(coll);
    for (const string &json : docs)
    {
      add_op.add(json);
    }
    add_op.execute();
    EXPECT_EQ(5, show_docs(coll));
  }

  cout << endl << "5. Using custom iterator" << endl;

  struct It
  {
    unsigned m_pos;

    DbDoc operator*()
    {
      std::ostringstream buf;
      buf << "{ \"bar" << m_pos << "\": " << m_pos << " }";
      return DbDoc(buf.str());
    }

    void operator++()
    {
      if (m_pos > 0)
        m_pos--;
    }

    It(unsigned size = 0) : m_pos(size)
    {}

    bool operator==(const It &other)
    {
      return (other.m_pos == 0) && (m_pos == 0);
    }

    bool operator!=(const It &other)
    {
      return !(*this == other);
    }
  };

  coll.remove("true").execute();
  coll.add(It(5), It()).execute();
  EXPECT_EQ(5, show_docs(coll));

}


TEST_F(Batch, table_insert)
{
  SKIP_IF_NO_XPLUGIN;

  sql("DROP TABLE IF EXISTS test.table_insert");
  sql(
    "CREATE TABLE test.table_insert("
    "  a INT,"
    "  b VARCHAR(32)"
    ")");

  Table tbl = getSchema("test").getTable("table_insert");

  std::vector<Row> rows;

  rows.emplace_back(1, "foo");
  rows.emplace_back(2, "bar");
  rows.emplace_back(3, "baz");
  rows.emplace_back(4, "buz");
  rows.emplace_back(5, "bum");

  tbl.insert().rows(rows).rows(rows[0],rows[1],rows[2]).execute();

  {
    RowResult res = sql("SELECT a,b FROM test.table_insert");

    unsigned count;
    Row row;

    cout << "== rows in the table ==" << endl;

    for (count = 0; (row = res.fetchOne()); ++count)
    {
      cout << "row#" << count << ": " << row[0] << ", " << row[1] << endl;
    }

    EXPECT_EQ(8, count);
  }

  tbl.insert().rows(rows.begin(), rows.end()).values(6, "new").execute();

  {
    RowResult res = sql("SELECT a,b FROM test.table_insert");

    unsigned count;
    Row row;

    cout << "== rows in the table ==" << endl;

    for (count = 0; (row = res.fetchOne()); ++count)
    {
      cout << "row#" << count << ": " << row[0] << ", " << row[1] << endl;
    }

    EXPECT_EQ(14, count);
  }
}

