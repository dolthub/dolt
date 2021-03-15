/*
 * Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.
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
#include <vector>

using std::cout;
using std::endl;
using std::vector;
using namespace mysqlx;

class Bugs : public mysqlx::test::Xplugin
{
};


TEST_F(Bugs,bug30989042_cdk_reply_error)
{
  SKIP_IF_NO_XPLUGIN;

  // Create procedure that returns 2 row sets

  sql("DROP PROCEDURE IF EXISTS test.p");
  sql(
    "CREATE PROCEDURE test.p(error INT)"
    "BEGIN"
    "  SELECT 1;"
    "  IF error = 0 THEN"
    "    SELECT 2;"
    "  ELSE"
    "    SELECT 1/point(1,0);"    // trigger error
    "  END IF;"
    "END"
  );

  auto res = sql("CALL test.p(0)");

  // res.count() will consume the first row set, but another
  // one is pending.

  EXPECT_EQ(1,res.count());

  // As we execute new statement, all remaining rows from the previous
  // reply should be cached. It was not the case before bug was fixed,
  // and the next statement was throwing error, because previous reply
  // was not fully consumed.

  EXPECT_NO_THROW(sql("select 1"));

  // Check that remaining rows from the reply to CALL are cached
  // and still available.

  EXPECT_TRUE(res.nextResult());
  EXPECT_EQ(1,res.count());

  // Check that in case of error, it is reported only when moving to
  // the next result in the reply (should not be reported while accessing
  // the first result).

  res = sql("CALL test.p(1)");
  EXPECT_EQ(1, res.count());
  EXPECT_THROW(res.nextResult(), Error);
}


TEST_F(Bugs, failover_error)
{

  try {

    // None of these servers can be connected, so this
    // will throw error.

    mysqlx::Session sess(
      SessionOption::USER, "user",
      SessionOption::HOST, "bad_host_1",
      SessionOption::HOST, "bad_host_2"
    );

  }
  catch (const Error & err)
  {
    cout << "Expected error: " << err.what() << endl;

    // In case of multiple hosts, the error thrown should say
    // "Could not connect to any of the given data sources".

    EXPECT_TRUE(
      std::string::npos !=
      std::string(err.what()).find(
        "Could not connect to any of the given data sources"
      )
    );
  };

}


TEST_F(Bugs, bug25505482)
{
  SKIP_IF_NO_XPLUGIN;

  const vector<const char*> good =
  {
    "CHARSET(CHAR(X'65'))",
    "'abc' NOT LIKE 'ABC1'",
    "'a' RLIKE '^[a-d]'",
    "'a' REGEXP '^[a-d]'",
    "POSITION('bar' IN 'foobarbar')",
  };

  const vector<const char*> not_supported =
  {
    "CHARSET(CHAR(X'65' USING utf8))",
    "TRIM(BOTH 'x' FROM 'xxxbarxxx')",
    "TRIM(LEADING 'x' FROM 'xxxbarxxx')",
    "TRIM(TRAILING 'xyz' FROM 'barxxyz')",
    "'Heoko' SOUNDS LIKE 'h1aso'",
  };

  get_sess().createSchema("test", true);
  get_sess().sql("DROP TABLE IF EXISTS test.bug25505482").execute();
  get_sess().sql("CREATE TABLE test.bug25505482 (a int)").execute();

  Table t = get_sess().getSchema("test").getTable("bug25505482");

  cout << "== testing supported expressions ==" << endl << endl;

  for (const char *expr : good)
  {
    cout << "- testing expression: " << expr << endl;
    EXPECT_NO_THROW(t.select().where(expr).execute());
  }

  cout << endl << "== testing not supported expressions ==" << endl << endl;

  for (const char *expr : not_supported)
  {
    cout << "- testing not supported expression: " << expr << endl;
    try {
      t.select().where(expr).execute();
      FAIL() << "Expected error when parsing expression";
    }
    catch (const Error &e)
    {
      std::string msg(e.what());
      cout << "-- got error: " << msg << endl;
      EXPECT_NE(msg.find("not supported yet"), std::string::npos);
    }
  }

  cout << "Done!" << endl;
}

TEST_F(Bugs, bug26130226_crash_update)
{
  SKIP_IF_NO_XPLUGIN;

  get_sess().dropSchema("crash_update");
  get_sess().createSchema("crash_update");
  Schema sch = get_sess().getSchema("crash_update");
  Collection coll = sch.createCollection("c1", true);

  coll.add("{ \"name\": \"abc\", \"age\": 1 , \"misc\": 1.2}").execute();
  Table tabNew = sch.getCollectionAsTable("c1");

  EXPECT_THROW(
    tabNew.update().set((char *)0, expr("")).execute(), // SegFault
    Error);
}

TEST_F(Bugs, bug_26962725_double_bind)
{
  SKIP_IF_NO_XPLUGIN;

  get_sess().dropSchema("bug_26962725_double_bind");
  Schema db = get_sess().createSchema("bug_26962725_double_bind");
  /// Collection.find() function with fixed values

  db.dropCollection("my_collection");
  Collection myColl = db.createCollection("my_collection");

  myColl.add(R"({"name":"mike", "age":39})")
        .add(R"({"name":"johannes", "age":28})")
        .execute();

  EXPECT_EQ(2, myColl.find().execute().count());

  // Create Collection.remove() operation, but do not run it yet
  auto myRemove = myColl.remove("name = :param1 AND age = :param2");

  // Binding parameters to the prepared function and .execute()
  myRemove.bind("param1", "mike").bind("param2", 39).execute();
  myRemove.bind("param1", "johannes").bind("param2", 28).execute();

  EXPECT_EQ(0, myColl.find().execute().count());
}

TEST_F(Bugs, bug_27727505_multiple_results)
{
  SKIP_IF_NO_XPLUGIN;

  mysqlx::Session &sess = get_sess();
  sess.dropSchema("bug_27727505_multiple_results");
  sess.createSchema("bug_27727505_multiple_results");

  /* ddl */
  std::string strValue = "";
  sess.sql("use bug_27727505_multiple_results").execute();
  sess.sql("drop table if exists bug_27727505_multiple_results").execute();
  sess.sql("create table newtable(f0 int, f1 varchar(1024))").execute();
  for(int i=0;i<100;i++)
  {
    strValue.resize(1024, 'a');
    sess.sql("insert into newtable values(?,?)")
        .bind(i)
        .bind(strValue.c_str())
        .execute();
  }
  sess.sql("drop procedure if exists test").execute();
  sess.sql("CREATE PROCEDURE test() BEGIN select f0, f1 from newtable where"
           " f0 <= 33; select f0, f1 from newtable where f0 <= 10; END")
      .execute();
  SqlResult res = sess.sql("call test").execute();

  //Force result caching
  SqlResult res2 = sess.sql("call test").execute();

  Row row;
  int setNo = 0;
  do
  {
    std::vector<Row> rowAll = res.fetchAll();
    unsigned int j=0;
    for(j = 0;j < rowAll.size();j++)
    {
      string data = (string)rowAll[j][1];
      int num = rowAll[j][0];
      if((unsigned int)num!=j || strValue.compare(data))
      {
        std::stringstream ss;
        ss << "Fetch fail in set : "<<setNo<<" row : "<<num ;
        throw ss.str();
      }
      else
      {
        cout << "Fetch pass in set : "<<setNo<<" row : "<<num << endl;
      }
    }
    if((setNo == 0 && j != 34) || (setNo == 1 && j != 11))
    {
      throw "Not all results fetched";
    }
    std::vector<Type> expcType;
    expcType.push_back (Type::INT);
    expcType.push_back (Type::STRING);
    std::vector<string> expcName;
    expcName.push_back ("f0");
    expcName.push_back ("f1");

    const Columns &cc = res.getColumns();
    for(unsigned int i=0;i < res.getColumnCount();i++)
    {
      if(expcName[i].compare(cc[i].getColumnName()))
      {
        throw "Column Name mismatch";
      }
      if(expcType[i] != cc[i].getType())
      {
        throw "Column Type mismatch";
      }
      if(0 != cc[i].getFractionalDigits())
      {
        throw "getFractionalDigits is not zero";
      }
      cout << cc[i].getColumnName() << endl;
      cout << cc[i].getType() << endl;
      cout << cc[i].isNumberSigned() << endl;
      cout << cc[i].getFractionalDigits() << endl;
    }

    setNo++;
  }
  while(res.nextResult());

  EXPECT_EQ(2, setNo);

  sess.sql("drop procedure if exists test").execute();
  sess.sql("CREATE PROCEDURE test() BEGIN select f0, f1 from newtable "
           "where f0 > 1000; select f0, f1  from newtable where f0 <= 10;"
           " END").execute();
  res = sess.sql("call test").execute();
  setNo = 0;
  do
  {
    unsigned int j=0;
    std::vector<Row> rowAll = res.fetchAll();
    for(j = 0;j < rowAll.size();j++)
    {
      string data = (string)rowAll[j][1];
      int num = rowAll[j][0];
      if((unsigned int)num!=j || strValue.compare(data))
      {
        std::stringstream ss;
        ss << "Fetch fail in set : "<<setNo<<" row : "<<num ;
        throw ss.str();
      }
      else
      {
        cout << "Fetch pass in set : "<<setNo<<" row : "<<num << endl;
      }
    }
    if((setNo == 0 && j != 0) || (setNo == 1 && j != 11))
    {
      throw "Not all results fetched";
    }

    setNo++;
  }
  while(res.nextResult());

  EXPECT_EQ(2, setNo);

  sess.sql("drop procedure if exists test").execute();
  sess.sql("CREATE PROCEDURE test() BEGIN select f0 from newtable; select f1 from newtable "
           "where f0 > 100; select f0 as new_f0  from newtable where f0 <= 10;"
           " END").execute();

  {
    SqlResult res = sess.sql("call test").execute();

    //Force result caching
    SqlResult res2 = sess.sql("call test").execute();

    //All resultsets are now cached
    EXPECT_EQ(100, res.count());
    EXPECT_EQ(string("f0"), res.getColumn(0).getColumnName());
    EXPECT_TRUE(res.nextResult());
    EXPECT_EQ(static_cast<unsigned long>(0), res.count());
    EXPECT_EQ(string("f1"), res.getColumn(0).getColumnName());
    EXPECT_TRUE(res.nextResult());
    EXPECT_EQ(11, res.count());
    EXPECT_EQ(string("new_f0"), res.getColumn(0).getColumnLabel());
    EXPECT_FALSE(res.nextResult());
  }
}


TEST_F(Bugs, bug_hang_send_maxpacket)
{
  SKIP_IF_NO_XPLUGIN;

  auto schema = get_sess().createSchema("bug_hang_maxpacket",true);
  schema.dropCollection("test");
  auto coll = schema.createCollection("max_packet",true);

  auto query_max_packet = sql("show variables like '%mysqlx_max_allowed_packet%'");

  size_t maxpacket = std::stoul(query_max_packet.fetchOne()[1].get<std::string>());

  std::string name(maxpacket,L'A');

  std::stringstream buffer;
  buffer << R"({ "name": ")" << name << R"("})";

  try{
    coll.add(buffer.str()).execute();
    FAIL() << "Should have thrown error!";
  }
  catch (Error &e)
  {
    std::cout << "Expected: " << e << std::endl;
  }

}

TEST_F(Bugs, modify_clone)
{
  SKIP_IF_NO_XPLUGIN;

  auto coll = get_sess().getSchema("test", true).createCollection("modify_clone");
  CollectionModify cModify = coll.modify("true").set("$.name", "Data_New");
  //Should not crash
  Result mod = cModify.execute();
}

TEST_F(Bugs, list_initializer)
{
  SKIP_IF_NO_XPLUGIN;

  auto sch = get_sess().getSchema("test", true);

  auto coll = sch.createCollection("c1");
  coll.remove("true").execute();

  for(auto collection : sch.getCollectionNames())
  {
    std::cout << collection << std::endl;
  }

  for(auto collections : sch.getCollections())
  {
    std::cout << collections.getName() << std::endl;
  }

  for(auto tables : sch.getTables())
  {
    std::cout << tables.getName() << std::endl;
  }

  Result add_res = coll.add(
                 "{ \"_id\": \"myuuid-1\", \"name\": \"foo\", \"age\": 7 }",
                 "{ \"name\": \"buz\", \"age\": 17 }",
                 "{ \"name\": \"bar\", \"age\": 3 }"
                 ).execute();

  int count = 0;
  for(const string& id : add_res.getGeneratedIds())
  {
    std::cout << id << std::endl;
    ++count;
  }
  EXPECT_EQ(2, count);

  for(auto w : add_res.getWarnings())
  {
    std::cout << w.getCode() << ": " << w.getMessage() << std::endl;
  }

  count = 0;
  for(const std::string& id : add_res.getGeneratedIds())
  {
    std::cout << id << std::endl;
    ++count;
  }
  EXPECT_EQ(2, count);

  count = 0;
  for(auto id : add_res.getGeneratedIds())
  {
    std::cout << id << std::endl;
    ++count;
  }
  EXPECT_EQ(2, count);

  DocResult fin_res = coll.find().execute();

  for(auto doc : fin_res)
  {
    std::cout << doc << std::endl;
  }

  auto tbl = sch.getCollectionAsTable("c1");

  auto tbl_res = tbl.select("_id").execute();
  for (Row r : tbl_res)
  {
    std::cout << r.get(0).get<string>() << std::endl;
  }

  RowResult sql_res = get_sess().sql("select _id from test.c1").execute();
  for(Row r : sql_res)
  {
    std::cout << r.get(0).get<string>() << std::endl;
  }
}

TEST_F(Bugs, crud_move)
{
  SKIP_IF_NO_XPLUGIN;

  auto coll = get_sess().createSchema("test",true).createCollection("c1",true);

  coll.remove("true").execute();

  Result add_res = coll.add(
                 "{ \"_id\": \"myuuid-1\", \"name\": \"foo\", \"age\": 7 }",
                 "{ \"name\": \"buz\", \"age\": 17 }",
                 "{ \"name\": \"bar\", \"age\": 3 }"
                 ).execute();

  auto find = coll.find();
  // query
  find.execute();
  // prepare+execute
  find.execute();

  EXPECT_EQ(1,
            sql("select count(*) from performance_schema.prepared_statements_instances").fetchOne()[0].get<int>());

  {
    auto tmp_find = find;
    //Since limit will prepare+execute right away, we should test it here:
    find.limit(2);
    // prepare+execute
    find.execute();
    // execute
    find.execute();
    find = find.limit(1);
    find.execute();

    EXPECT_EQ(2,
              sql("select count(*) from performance_schema.prepared_statements_instances").fetchOne()[0].get<int>());
  }

  //Force stmt_id cleanup
  find.sort("name ASC");
  find.execute();
  find.execute();

  { //Find2 scope
    CollectionFind find2 = find.limit(1);
    //With assign, both point to same implementation (also same PS id), untill one
    //changes some parameter, which in that case, will create a clone.

    // execute
    find.execute();

    find2.limit(2);
    // execute
    find.execute();

    // execute
    find2.execute();
    find2.execute();

    EXPECT_EQ(1,
              sql("select count(*) from performance_schema.prepared_statements_instances").fetchOne()[0].get<int>());

    //Move works just like assignment (same as shared_ptr behaviour)
    find = std::move(find2);
    { //find3 scope
      auto find3 = find;

      // execute
      find2.execute();
      find2.execute();
      // execute
      find2.execute();
      find2.execute();
      // execute
      find3.execute();
      find3.execute();

      EXPECT_EQ(1,
                sql("select count(*) from performance_schema.prepared_statements_instances").fetchOne()[0].get<int>());

      find.sort("name ASC");

      // query
      find.execute();
      // prepare+execute
      find.execute();
      // execute
      find2.execute();
      // execute
      find3.execute();

      EXPECT_EQ(2,
                sql("select count(*) from performance_schema.prepared_statements_instances").fetchOne()[0].get<int>());

    } //find3 scope

    EXPECT_EQ(2,
              sql("select count(*) from performance_schema.prepared_statements_instances").fetchOne()[0].get<int>());

  }// find2 scope

  find.sort("name DESC");

  find.execute();
  find.execute();

  EXPECT_EQ(1,
            sql("select count(*) from performance_schema.prepared_statements_instances").fetchOne()[0].get<int>());


}

TEST_F(Bugs, not_accumulate)
{
  SKIP_IF_NO_XPLUGIN;

  auto sch = get_sess().createSchema("test",true);
  auto coll = sch.createCollection("c1",true);
  auto tbl = sch.getCollectionAsTable("c1");

  coll.remove("true").execute();

  coll.add("{ \"_id\": \"myuuid-1\", \"name\": \"foo\", \"age\": 7 }",
           "{ \"name\": \"buz\", \"age\": 17 }",
           "{ \"name\": \"bar\", \"age\": 3 }",
           "{ \"name\": \"baz\", \"age\": 3 }"
           ).execute();


  //FIND

  auto find = coll.find();
  find.fields("notfound");
  find.fields("name as name", "age as age");
  find.groupBy("notfound");
  find.groupBy("age","name");
  find.sort("notfound");
  find.sort("age ASC");

  auto doc = find.execute().fetchOne();
  EXPECT_EQ(3, doc["age"].get<int>());
  EXPECT_EQ(string("bar"), doc["name"].get<string>());


  // MODIFY

  auto modify = coll.modify("true");
  modify.set("food", expr("[]"));
  modify.arrayAppend("food", "milk");
  modify.arrayAppend("food", "soup");
  modify.arrayAppend("food", "potatoes");
  modify.sort("notfound");
  modify.sort("age ASC");
  modify.limit(2);
  //only age=3 will be modified
  modify.execute();

  auto check_changes = coll.find().sort("age ASC").execute();
  EXPECT_TRUE(check_changes.fetchOne().hasField("food"));
  EXPECT_TRUE(check_changes.fetchOne().hasField("food"));
  EXPECT_FALSE(check_changes.fetchOne().hasField("food"));
  EXPECT_FALSE(check_changes.fetchOne().hasField("food"));

  //REMOVE

  auto remove = coll.remove("true");
  remove.sort("name DESC");
  remove.sort("age ASC");
  remove.limit(2);
  //only age=3 will be removed
  remove.execute();

  check_changes = coll.find().execute();
  for(auto doc : check_changes)
  {
    EXPECT_NE(3, doc["age"].get<int>());
  }

  // TABLE
  auto select = tbl.select("doc->$.age");
  select.orderBy("notfound ASC");
  select.orderBy("doc->$.age ASC");
  select.groupBy("notfound");
  select.groupBy("doc->$.age");

  select.lockExclusive();
  EXPECT_EQ(2, select.execute().count());

  auto update = tbl.update();
  update.set("doc->$.age",1);
  update.where("doc->$.age > 7");
  update.orderBy("notfound ASC");
  update.orderBy("doc->$.age ASC");
  EXPECT_EQ(1, update.execute().getAffectedItemsCount());

  auto tbl_remove = tbl.remove();
  tbl_remove.orderBy("notfound ASC");
  tbl_remove.orderBy("doc->$.age ASC");
  EXPECT_EQ(2, tbl_remove.execute().getAffectedItemsCount());
}


TEST_F(Bugs, bug29525077)
{
  SKIP_IF_NO_XPLUGIN;

  mysqlx::Session &sess = get_sess();
  sess.dropSchema("bug29525077_int_types");
  sess.createSchema("bug29525077_int_types");

  sess.sql(
    "CREATE TABLE bug29525077_int_types.int_types ("
    "c1 TINYINT, c2 SMALLINT, c3 MEDIUMINT,"
    "c4 INT, c5 BIGINT,"
    "c6 TINYINT UNSIGNED, c7 SMALLINT UNSIGNED,"
    "c8 MEDIUMINT UNSIGNED, c9 INT UNSIGNED,"
    "c10 BIGINT UNSIGNED)"
  ).execute();
  sess.sql(
    "INSERT INTO bug29525077_int_types.int_types "
    "VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"
  ).execute();
  mysqlx::Table tab = sess.getSchema("bug29525077_int_types").
                  getTable("int_types");

  auto res = tab.select().execute();
  auto columns = &res.getColumns();
  col_count_t num_columns = res.getColumnCount();
  EXPECT_EQ(10, num_columns);
  while (Row r = res.fetchOne())
  {
    for (col_count_t i = 0; i < num_columns; ++i)
    {
      auto col_type = (*columns)[i].getType();
      switch (i)
      {
      case 0:
      case 5:
        EXPECT_EQ(Type::TINYINT, col_type);
        break;
      case 1:
      case 6:
        EXPECT_EQ(Type::SMALLINT, col_type);
        break;
      case 2:
      case 7:
        EXPECT_EQ(Type::MEDIUMINT, col_type);
        break;
      case 3:
      case 8:
        EXPECT_EQ(Type::INT, col_type);
        break;
      case 4:
      case 9:
        EXPECT_EQ(Type::BIGINT, col_type);
        break;
      default:
        break;
      }
    }
  }
  sess.dropSchema("bug29525077_int_types");
}

TEST_F(Bugs, is_false)
{
  SKIP_IF_NO_XPLUGIN
  auto schema = get_sess().createSchema("test", true);
  auto coll = schema.createCollection("is_false", true);
  coll.remove("true").execute();
  coll.add(DbDoc(R"({"val": 0 })")).execute();
  coll.add(DbDoc(R"({"val": 1 })")).execute();
  coll.add(DbDoc(R"({"val": 1 })")).execute();
  // Since boolean is not an expected type, it should throw error
  // Had a segmentation fault issue
  EXPECT_THROW(coll.find("cast(val as boolean) is false").execute(),
               mysqlx::Error);
  EXPECT_EQ(1, coll.find("val is false").execute().count());
  auto tbl = schema.getCollectionAsTable("is_false");
  EXPECT_EQ(1, tbl.select().where("doc->$.val is false").execute().count());
}

TEST_F(Bugs, bug29394723)
{
  SKIP_IF_NO_XPLUGIN
  // connection attributes supported only from 8.0.15
  SKIP_IF_SERVER_VERSION_LESS(8,0,15)

  // Check that the _os session attribute is present and not empty
  string _os = get_sess().sql("SELECT ATTR_VALUE FROM "
                            "performance_schema.session_account_connect_attrs "
                            "WHERE ATTR_NAME = '_os' AND "
                            "PROCESSLIST_ID = CONNECTION_ID() AND "
                            "LENGTH(ATTR_VALUE) > 0").execute().
                            fetchOne()[0].get<string>();

  cout << "_os: " << _os << endl;
  EXPECT_NE("", _os);

  // Check that the _platform session attribute is present and not empty
  string _platform = get_sess().sql("SELECT ATTR_VALUE FROM "
                            "performance_schema.session_account_connect_attrs "
                            "WHERE ATTR_NAME = '_platform' AND "
                            "PROCESSLIST_ID = CONNECTION_ID() AND "
                            "LENGTH(ATTR_VALUE) > 0").execute().
                            fetchOne()[0].get<string>();

  cout << "_platform: " << _platform << endl;
  EXPECT_NE("", _platform);
}

TEST_F(Bugs, bug29847865)
{
  SKIP_IF_NO_XPLUGIN

  get_sess().sql("DROP TABLE IF EXISTS test.t").execute();
  get_sess().sql("CREATE TABLE test.t(a TEXT)").execute();
  Table t = get_sess().getSchema("test").getTable("t");

  string foo = u"\x00000281\x00000282\x00000283\x00000284\x00000285\x00000286";

  t.insert().values(foo).execute();
  Row r = t.select().limit(1).execute().fetchOne();
  string bar = r.get(0);

  EXPECT_EQ(foo.size(), bar.size());
  EXPECT_EQ(foo, bar);
}
