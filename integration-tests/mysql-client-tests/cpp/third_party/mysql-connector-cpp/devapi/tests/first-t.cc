/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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
#include <array>


using std::cout;
using std::endl;
using namespace mysqlx;

class First : public mysqlx::test::Xplugin
{
};


TEST_F(First, first)
{
  SKIP_IF_NO_XPLUGIN;

  SqlResult res = get_sess().sql("SELECT @@version").execute();
  cout << "Talking to MySQL Server: " << res.fetchOne()[0] << endl;
  cout << "Done!" << endl;
}


TEST_F(First, sql)
{
  SKIP_IF_NO_XPLUGIN;

  sql("DROP TABLE IF EXISTS test.t");
  sql("CREATE TABLE test.t(c0 INT, c1 TEXT)");

  auto sql = get_sess().sql("INSERT INTO test.t VALUES (?,?)");

  sql.bind(33, "foo")
    .execute();
  sql.bind(13)
      .bind("bar")
      .execute();
  sql.bind(1)
      .bind("baz")
      .execute();

  std::array<int,2> args = { 7, 30 };

  RowResult res = get_sess().sql("SELECT *,? FROM test.t WHERE c0 > ?")
                            .bind(args)
                            .execute();

  Row row = res.fetchOne();

  cout << "col#0: " << row[0] << endl;
  cout << "col#1: " << row[1] << endl;
  cout << "col#2: " << row[2] << endl;

  EXPECT_EQ(args[0], (int)row[2]);
  EXPECT_LT(args[1], (int)row[0]);

  cout << "Done!" << endl;
}


TEST_F(First, sql_multi)
{
  // Testing multi result sets
  SKIP_IF_NO_XPLUGIN;

  sql("DROP PROCEDURE IF EXISTS test.test");
  sql("CREATE PROCEDURE test.test() BEGIN SELECT 1; SELECT 2, 'foo'; END");

  {
    SqlResult res = get_sess().sql("CALL test.test()").execute();

    EXPECT_TRUE(res.hasData());

    cout << "-- first rset --" << endl;

    EXPECT_EQ(1U, res.getColumnCount());

    Row row = res.fetchOne();
    EXPECT_TRUE(row);

    for (unsigned i = 0; i < res.getColumnCount(); ++i)
      cout << "- col#" << i << ": " << row[i] << endl;

    EXPECT_TRUE(res.nextResult());
    EXPECT_TRUE(res.hasData());

    cout << "-- second rset --" << endl;

    EXPECT_EQ(2U, res.getColumnCount());

    row = res.fetchOne();
    EXPECT_TRUE(row);

    for (unsigned i = 0; i < res.getColumnCount(); ++i)
      cout << "- col#" << i << ": " << row[i] << endl;

    EXPECT_FALSE(res.nextResult());
  }

  // with buffering

  {
    SqlResult res = get_sess().sql("CALL test.test()").execute();

    cout << "-- first rset --" << endl;

    std::vector<Row> rows = res.fetchAll();
    EXPECT_EQ(1U, rows.size());

    EXPECT_TRUE(res.nextResult());

    cout << "-- second rset --" << endl;
    EXPECT_EQ(2U, res.getColumnCount());

    rows = res.fetchAll();
    EXPECT_EQ(1U, rows.size());

    EXPECT_FALSE(res.nextResult());
  }

  // check discarding of multi-rset

  {
    SqlResult res = get_sess().sql("CALL test.test()").execute();
    EXPECT_TRUE(res.fetchOne());
  }

  {
    SqlResult res = get_sess().sql("CALL test.test()").execute();
  }

  cout << "Done!" << endl;
}


TEST_F(First, value)
{
  // Value object and conversions.

  {
    Value val = "foo";
    EXPECT_EQ(Value::STRING, val.getType());
    EXPECT_EQ(val.get<std::string>(), "foo");
    EXPECT_EQ(val.get<std::wstring>(), L"foo");
    EXPECT_EQ(val.get<mysqlx::string>(), u"foo");
  }

  {
    Value val = L"foo";
    EXPECT_EQ(Value::STRING, val.getType());
    EXPECT_EQ(val.get<std::string>(), "foo");
    EXPECT_EQ(val.get<std::wstring>(), L"foo");
    EXPECT_EQ(val.get<mysqlx::string>(), u"foo");
  }

  {
    Value val;
    EXPECT_EQ(Value::VNULL, val.getType());
    val = std::numeric_limits<uint64_t>::max();
    EXPECT_THROW(val.get<int32_t>(),Error);
    EXPECT_THROW(val.get<int64_t>(),Error);
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), val.get<uint64_t>());
  }

  {
    Value val = std::numeric_limits<float>::max();
    EXPECT_EQ(Value::FLOAT, val.getType());
    EXPECT_THROW(val.get<uint64_t>(),Error);
    EXPECT_THROW(val.get<int64_t>(),Error);
    EXPECT_THROW(val.get<string>(),Error);
    EXPECT_THROW(val["dumb"],Error);
    EXPECT_EQ(std::numeric_limits<float>::max(), val.get<float>());
    EXPECT_EQ(std::numeric_limits<float>::max(), val.get<double>());
  }

  {
    Value val = std::numeric_limits<double>::max();
    EXPECT_EQ(Value::DOUBLE, val.getType());
    EXPECT_THROW(val.get<float>(),Error);
    EXPECT_THROW(val["dumb"],Error);
    EXPECT_EQ(std::numeric_limits<double>::max(), val.get<double>());
  }

  {
    Value val;
    val = DbDoc(R"({"arr" : [1,2,3,4], "doc" : {"arr2":[{"val1":1}]}})");
    EXPECT_EQ(Value::DOCUMENT, val.getType());
    Value arr = val["arr"];
    EXPECT_EQ(Value::ARRAY, arr.getType());
    int i = 0;
    for (auto el : arr)
    {
      ++i;
      EXPECT_EQ(i, el.get<int>());
    }
    Value doc = val["doc"];
    EXPECT_EQ(Value::DOCUMENT, doc.getType());
    Value arr2 = doc["arr2"];
    EXPECT_EQ(1 ,arr2[0]["val1"].get<int>());
  }

}



TEST_F(First, api)
{
  // Check that assignment works for database objects.

  SKIP_IF_NO_XPLUGIN;

  Schema s = get_sess().getSchema("foo");
  s = get_sess().getSchema("test");

  Table t = s.getTable("t1");
  t = s.getTable("t");

  Collection c = s.getCollection("c1");
  c = s.getCollection("c");

  sql("DROP TABLE IF EXISTS test.t");
  sql("CREATE TABLE test.t(c0 INT, c1 TEXT)");
  s.createCollection("c", true);

  {
    RowResult res;
    EXPECT_THROW(res.fetchOne(),Error);
    res = t.select().execute();
  }

  {
    DocResult res;
    EXPECT_THROW(res.fetchOne(),Error);
    res = c.find().execute();
  }

  // Test copy semantics for collection operations.

  {
    CollectionFind find = c.find().fields("a");
    find = c.find().fields("a");
    find = c.find().groupBy("...");
    find = c.find().groupBy("...").having("...");
    auto x = find.sort("...");
    find = c.find().groupBy("...").having("...").sort("...");
    find = c.find().sort("...").limit(0);
    find = c.find().sort("...").limit(0).offset(0);
    find = c.find().bind("...",0);
    CollectionFind find1 = find;
    find1 = x;
    CollectionFind find2 = x;
  }

  {
    CollectionAdd add = c.add("...");
    add = c.add("...").add("...");
    auto x = add.add("...");
    CollectionAdd add1 = add;
    add1 = x;
    CollectionAdd add2 = x;
  }

  {
    CollectionRemove rm = c.remove("...");
    rm = c.remove("...");
    auto x = c.remove("...").sort("...");
    rm = c.remove("...").sort("...").limit(0);
    rm = c.remove("...").bind("...", 0);
    CollectionRemove rm1 = rm;
    rm1 = x;
    CollectionRemove rm2 = x;
  }

  {
    CollectionModify modify = c.modify("...").set("..", 0);
    modify = modify.arrayAppend("...", 0);
    auto x = modify.bind("...", 0);
    modify = x.bind("...", 0);
    CollectionModify modify1 = modify;
    CollectionModify modify2 = x;
  }

  // Test copy semantics for table operations

  {
    TableInsert ins = t.insert("a");
    ins = t.insert("a");
    ins = ins.values(1);
    auto x = ins.values(2);
    ins = x;
    TableInsert ins1 = ins;
    TableInsert ins2 = x;
  }

  {
    TableSelect sel = t.select("a").where("...");
    sel = t.select("a");
    sel = sel.orderBy("...").limit(1);
    auto x = sel.offset(2);
    sel = x;
    TableSelect sel1 = sel;
    TableSelect sel2 = x;
  }

  {
    TableUpdate upd = t.update();
    upd = t.update().where("...");
    upd = upd.orderBy("...").limit(0);
    auto x = upd.bind("...", 0);
    upd = x;
    TableUpdate upd1 = upd;
    TableUpdate upd2 = x;
  }

  {
    TableRemove rm = t.remove().where("...");
    rm = t.remove();
    rm = rm.orderBy("...").limit(0);
    auto x = rm.bind("...", 0);
    rm = x;
    TableRemove rm1 = rm;
    TableRemove rm2 = x;
  }
}


/*
  Test different forms of session constructor.

  The goal of this test is to check that session can be constructed
  given session parameters of appropriate types. Different forms of
  constructors are tested as well as whether implicit conversions for
  parameter types work as expected.

  The S_ctor_test<> template defines static method test() which calls
  t0<T>::test() for different types T of the first session parameter.
  The t0<T>::test() is defined in similar way, testing different possible
  types of the second session parameter and so on.

  Tests create a session for invalid IP address 0.0.0.0 expecting session
  constructor to throw error.
*/

template <class Session>
struct S_ctor_test
{
  template <typename A>
  struct t0
  {
    template <typename B>
    struct t1
    {
      template <typename C>
      struct t2
      {
        template <typename D>
        struct t3
        {
          template <typename E>
          struct t4
          {
            static void test(A host, B port, C user, D pwd, E db)
            {
              try {
                Session s(host, port, user, pwd, db);
              }
              catch (const Error&)
              {}

              try {
                Session s(host, port, user, NULL, db);
              }
              catch (const Error&)
              {}

              try {
                Session s(port, user, pwd, db);
              }
              catch (const Error&)
              {
              }

              try {
                Session s(port, user, NULL, db);
              }
              catch (const Error&)
              {
              }

              try {
                Session s(host, user, pwd, db);
              }
              catch (const Error&)
              {
              }

              try {
                Session s(host, user, NULL, db);
              }
              catch (const Error&)
              {
              }
            }
          };

          static void test(A host, B port, C user, D pwd)
          {
            t4<string>::test(host, port, user, pwd, "db");
            t4<std::string>::test(host, port, user, pwd, "db");
            t4<const char*>::test(host, port, user, pwd, "db");
            //t4<const wchar_t*>::test(host, port, user, pwd, L"db");

            try {
              Session s(host, port, user, pwd);
            }
            catch (const Error&)
            {}

            try {
              Session s(host, port, user, NULL);
            }
            catch (const Error&)
            {}

            try {
              Session s(port, user, pwd);
            }
            catch (const Error&)
            {
            }

            try {
              Session s(port, user, NULL);
            }
            catch (const Error&)
            {
            }

            try {
              Session s(host, user, pwd);
            }
            catch (const Error&)
            {
            }

            try {
              Session s(host, user, NULL);
            }
            catch (const Error&)
            {
            }
          }
        };

        static void test(A host, B port, C user)
        {
          t3<const char*>::test(host, port, user, "pwd");
          t3<const char*>::test(host, port, user, NULL);
          t3<std::string>::test(host, port, user, "pwd");

          try {
            Session s(host, port, user);
          }
          catch (const Error&)
          {
          }

          try {
            Session s(port, user);
          }
          catch (const Error&)
          {
          }

          try {
            Session s(host, user);
          }
          catch (const Error&)
          {
          }
        }
      };

      static void test(A host, B port)
      {
        t2<string>::test(host, port, "user");
        t2<std::string>::test(host, port, "user");
        t2<const char*>::test(host, port, "user");
        //t2<const wchar_t*>::test(host, port, L"user");
      }
    };

    static void test(A host)
    {
      t1<unsigned>::test(host, 0);
      t1<unsigned short>::test(host, 0);
      t1<int>::test(host, 0);

      // Treat argument as URL

      try {
        Session s(host);
      }
      catch (const Error&)
      {
      }
    }

  };

  static void test()
  {
    /*
      Note: using invalid host name so that session constructor
      fails early (preferably before doing any real i/o).
    */
    t0<string>::test("");
    t0<std::string>::test("");
    t0<const char*>::test("");
  }
};


TEST_F(First, api_session)
{
  S_ctor_test<mysqlx::Session>::test();
}


TEST_F(First, warnings_multi_rset)
{

  SKIP_IF_NO_XPLUGIN;

  mysqlx::Session &sess = get_sess();

  get_sess().createSchema("test", true);

  sql("DROP PROCEDURE IF EXISTS test.p");

  sql(
    "CREATE PROCEDURE test.p()"
    "BEGIN"
    "  SELECT 1;"
    "  SELECT 1/0;"
    "  SELECT 2/'a';"
    "END"
  );

  {
    SqlResult res = sql("call test.p()");

    std::vector<Row> rows = res.fetchAll();

    /*
      We are in the middle of processing query result (only
      1st rset has been consumed).
    */

    EXPECT_EQ(2, res.getWarningsCount());

    std::vector<Warning> warnings = res.getWarnings();
    EXPECT_EQ(2, warnings.size());

    for(auto warn : warnings)
    {
      std::cout << warn << std::endl;
    }
  }

  {
    // getWarnings() without getWarningsCount()

    SqlResult res = sql("call test.p()");

    unsigned cnt = 0;
    for (Warning warn : res.getWarnings())
    {
      std::cout << warn << std::endl;
      cnt++;
    }
    EXPECT_EQ(2, cnt);

    // Check that results are still available.

    EXPECT_EQ(1, res.fetchOne()[0].get<int>());
  }

  {
    // getWarning() without getWarningsCount()

    SqlResult res = sql("call test.p()");

    EXPECT_NE(0, res.getWarning(0).getCode());
  }
}


TEST_F(First, parser_xplugin)
{

  SKIP_IF_NO_XPLUGIN;


  // Initialize table;
  sql("DROP TABLE IF EXISTS test.t");
  sql("CREATE TABLE test.t(c0 INT, c1 TEXT)");

  mysqlx::Session &sess = get_sess();

  sess.createSchema("test", true);

  Schema sch = sess.getSchema("test", true);

  Table tbl = sch.getTable("t", true);

  // Add data
  tbl.insert("c0", "c1").values(1, "Foo").execute();

  {
    RowResult res = tbl.select("~c0").execute();

    std::cout << static_cast<uint64_t>(res.fetchOne()[0]) << std::endl;
  }


  {
    RowResult res = tbl.select("2^~c0").execute();

    EXPECT_EQ(2^~1, static_cast<uint64_t>(res.fetchOne()[0]) );
  }

  {
    RowResult res = tbl.select("~c0").execute();
    EXPECT_EQ(~1, static_cast<uint64_t>(res.fetchOne()[0]) );
  }

  {
    RowResult res = tbl.select("c0").where("c0 < cast(11 as signed Integer)").execute();

    EXPECT_EQ(1, static_cast<uint64_t>(res.fetchOne()[0]));
  }

  {
    RowResult res = tbl.select("c0").where("c0 < cast(14.01 as decimal(3, 2))").execute();

    EXPECT_EQ(1, static_cast<uint64_t>(res.fetchOne()[0]));
  }

  {
    RowResult res = tbl.select("X'65'").execute();

    EXPECT_EQ(0x65, static_cast<uint64_t>(res.fetchOne()[0]));
  }

  //TODO: ADD this test when possible on xplugin

//  {
//    RowResult res = tbl.select("CHARSET(CHAR(X'65'))").execute();

//    EXPECT_EQ(1, static_cast<uint64_t>(res.fetchOne()[0]));
//  }

  {
    RowResult res = tbl.select("0x65").where("c0 < cast(14.01 as decimal(3, 2))").execute();

    EXPECT_EQ(0x65, static_cast<uint64_t>(res.fetchOne()[0]));
  }

  //TODO: ADD this test when possible on xplugin
//  { parser::Parser_mode::TABLE   , L"CHARSET(CHAR(0x65))"},
//  { parser::Parser_mode::TABLE   , L"CHARSET(CHAR(X'65' USING utf8))"},
//  { parser::Parser_mode::TABLE   , L"TRIM(BOTH 'x' FROM 'xxxbarxxx')"},
//  { parser::Parser_mode::TABLE   , L"TRIM(LEADING 'x' FROM 'xxxbarxxx')"},
//  { parser::Parser_mode::TABLE   , L"TRIM(TRAILING 'xyz' FROM 'barxxyz')"},

  {
    RowResult res = tbl.select("c1").where("c1 NOT LIKE 'ABC1'").execute();

    //EXPECT_EQ(string("Foo"), (string)(res.fetchOne()[0]));
  }

  //TODO: ADD this test when possible on xplugin
//  { parser::Parser_mode::TABLE   , L"'a' RLIKE '^[a-d]'"},

  {
    RowResult res = tbl.select("c1").where("c1 REGEXP '^[a-d]'").execute();

    EXPECT_TRUE(res.fetchOne().isNull());

//    EXPECT_EQ(string("Foo"), static_cast<string>(res.fetchOne()[0]));
  }

  //TODO: ADD this test when possible on xplugin
//  { parser::Parser_mode::TABLE   , L"POSITION('bar' IN 'foobarbar')"},
//  { parser::Parser_mode::TABLE   , L"'Heoko' SOUNDS LIKE 'h1aso'"}


}

TEST_F(First, sqlresult)
{
  SKIP_IF_NO_XPLUGIN;


  // Initialize table;
  sql("DROP TABLE IF EXISTS test.t");
  sql("CREATE TABLE test.t(id INT NOT NULL AUTO_INCREMENT,\
                           c1 TEXT,\
                           PRIMARY KEY (id))");


  {
    SqlResult res =  get_sess().sql("INSERT INTO test.t(c1) \
                                    VALUES (?),\
                                    (?),\
                                    (?)")
                                    .bind(L"foo")
                                    .bind(L"bar")
                                    .bind(L"baz")
                                    .execute();

    EXPECT_EQ(3, res.getAffectedItemsCount());
    EXPECT_EQ(1, res.getAutoIncrementValue());
  }

  {
    SqlResult res =  get_sess().sql("INSERT INTO test.t(c1) \
                                    VALUES (?),\
                                    (?),\
                                    (?)")
                                    .bind(L"foo")
                                    .bind(L"bar")
                                    .bind(L"baz")
                                    .execute();

    EXPECT_EQ(3, res.getAffectedItemsCount());
    EXPECT_EQ(4, res.getAutoIncrementValue());
  }

  {
    SqlResult res =  get_sess().sql("SELECT * from test.t")
                     .execute();

    EXPECT_THROW(res.getAffectedItemsCount(), Error);
    EXPECT_THROW(res.getAutoIncrementValue(), Error);

    res.nextResult();
    EXPECT_FALSE(res.nextResult());

    EXPECT_EQ(0, res.getAffectedItemsCount());
    EXPECT_EQ(0, res.getAutoIncrementValue());
  }

}
