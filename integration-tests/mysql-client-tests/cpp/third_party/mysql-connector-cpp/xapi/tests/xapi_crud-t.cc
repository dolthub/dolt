/*
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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

#include <stdio.h>
#include <string.h>
#include <thread>
#include "test.h"


  const char *queries[5] = {
     "DROP DATABASE IF EXISTS cc_crud_test",
     "CREATE DATABASE cc_crud_test",
     "CREATE TABLE cc_crud_test.crud_basic (id int auto_increment primary key, vctext varchar(32))",
     "INSERT INTO cc_crud_test.crud_basic (id, vctext) VALUES (2, '012345'), (10, 'abcdef'), (20, 'ghijkl'), (30, 'mnopqr')",
     "DROP TABLE cc_crud_test.crud_basic"
  };

  const char *json_row[5] = {
       "{\"_id\": \"C8B27676E8A1D1E12C250850273BD110\", \"a_key\": 1, \"b_key\": \"hello world\", \"c_key\": 3.89}",
       "{\"_id\": \"C8B27676E8A1D1E12C250850273BD111\", \"a_key\": 2, \"b_key\": \"how are you world\", \"c_key\": 4.321}",
       "{\"_id\": \"C8B27676E8A1D1E12C250850273BD112\", \"a_key\": 3, \"b_key\": \"bye world\", \"c_key\": 13.8901}",
       "{\"_id\": \"C8B27676E8A1D1E12C250850273BD113\", \"a_key\": 4, \"b_key\": \"hello again world\", \"c_key\": 7.00092}",
       "{\"_id\": \"C8B27676E8A1D1E12C250850273BD114\", \"a_key\": 5, \"b_key\": \"so long world\", \"c_key\": 88.888}"
  };

TEST_F(xapi, test_count)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  mysqlx_table_t *table;
  mysqlx_stmt_t *stmt;
  mysqlx_row_t *row;
  const char *schema_name = "cc_crud_test";
  const char *coll_name = "coll_test";
  const char *tab_name = "tab_test";
  char buf[512];
  int i, j;
  size_t count = 0;
  uint64_t rec_count = 0;

  AUTHENTICATE();

  mysqlx_schema_drop(get_session(), schema_name);
  ERR_CHECK(mysqlx_schema_create(get_session(), schema_name),
            get_session());
  schema = mysqlx_get_schema(get_session(), schema_name, 0);
  ERR_CHECK(mysqlx_collection_create(schema, coll_name), schema);
  collection = mysqlx_get_collection(schema, coll_name, 0);

  ERR_CHECK(mysqlx_collection_count(collection, &rec_count), collection);
  EXPECT_EQ(0, rec_count);

  stmt = mysqlx_collection_add_new(collection);
  for (i = 0; i < 100; ++i)
  {
    sprintf(buf, "{\"name\" : \"name %02d\"}", i);
    ERR_CHECK(mysqlx_set_add_document(stmt, buf), stmt);
  }
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  rec_count = 0;
  ERR_CHECK(mysqlx_collection_count(collection, &rec_count), collection);
  EXPECT_EQ(100, rec_count);

  sprintf(buf, "CREATE TABLE %s.%s (id int)",
            schema_name, tab_name);
  CRUD_CHECK(res = mysqlx_sql(get_session(), buf, MYSQLX_NULL_TERMINATED),
             get_session());
  table = mysqlx_get_table(schema, tab_name, 0);
  ERR_CHECK(mysqlx_table_count(table, &rec_count), table);
  EXPECT_EQ(0, rec_count);

  stmt = mysqlx_table_insert_new(table);
  for (i = 0; i < 100; ++i)
  {
    ERR_CHECK(mysqlx_set_insert_row(stmt, PARAM_UINT(i), PARAM_END), stmt);
  }
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  ERR_CHECK(mysqlx_table_count(table, &rec_count), table);
  EXPECT_EQ(100, rec_count);

  stmt = mysqlx_table_select_new(table);
  ERR_CHECK(mysqlx_set_select_where(stmt, "id < 10"), stmt);
  ERR_CHECK(mysqlx_set_select_order_by(stmt, "id", SORT_ORDER_ASC, PARAM_END), stmt);
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  ERR_CHECK(mysqlx_get_count(res, &count), res);
  EXPECT_EQ(10, count);

  ERR_CHECK(mysqlx_get_count(res, &count), res);
  EXPECT_EQ(10, count);

  j = 0;
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    // Call again to make sure rows are inact
    int64_t id = 0;

    ERR_CHECK(mysqlx_get_count(res, &count), res);
    EXPECT_EQ((9 - j), count);
    ERR_CHECK(mysqlx_get_sint(row, 0, &id), row);
    EXPECT_EQ(j, id);
    ++j;
  }
  EXPECT_EQ(10, j);

  // Check how mysqlx_get_count() handles next result
  EXPECT_EQ(RESULT_NULL, mysqlx_next_result(res));
  ERR_CHECK(mysqlx_get_count(res, &count), res);
  EXPECT_EQ(0, count);
}

TEST_F(xapi, test_merge_patch)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  mysqlx_stmt_t *stmt;
  const char *schema_name = "cc_crud_test";
  const char *coll_name = "coll_test";
  const char *json[] = {
           "{\"name_arr\": {\"first\" : \"Bob\", \"last\" : \"Smith\"}, \"user_id\" : \"bsmith987\"}",
           "{\"name_arr\": {\"first\" : \"Alice\", \"last\" : \"Jones\"}, \"user_id\" : \"ajones765\"}"
  };
  const char *patch = "{\"first_name\" : name_arr.first, \"last_name\" : name_arr.last, " \
                       "\"full_name\" : concat(name_arr.first, ' ', name_arr.last), \"name_arr\" : NULL }";
  const char *json_string;
  size_t json_len = 0;

  AUTHENTICATE();
  SKIP_IF_SERVER_VERSION_LESS(8, 0, 3);

  mysqlx_schema_drop(get_session(), schema_name);
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(get_session(), schema_name));
  schema = mysqlx_get_schema(get_session(), schema_name, 0);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, coll_name));
  collection = mysqlx_get_collection(schema, coll_name, 0);

  stmt = mysqlx_collection_add_new(collection);
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json[0]));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json[1]));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  // Execute short version of _modify_patch()
  CRUD_CHECK(
    res = mysqlx_collection_modify_patch(collection,
                                         "user_id='ajones765'",
                                         patch),
    collection);

  CRUD_CHECK(res = mysqlx_collection_find(collection, "first_name='Alice'"), collection);
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    EXPECT_TRUE(json_string != NULL);
    printf("\n[json: %s]", json_string);
    EXPECT_TRUE(strstr(json_string, "\"full_name\": \"Alice Jones\"") != NULL);
    EXPECT_TRUE(strstr(json_string, "\"first_name\": \"Alice\"") != NULL);
    EXPECT_TRUE(strstr(json_string, "\"last_name\": \"Jones\"") != NULL);
    EXPECT_TRUE(strstr(json_string, "name_arr") == NULL);
  }

  // Execute _set_modify_patch()
  stmt = mysqlx_collection_modify_new(collection);
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_patch(stmt, patch));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_criteria(stmt, "user_id='bsmith987'"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  CRUD_CHECK(res = mysqlx_collection_find(collection, "first_name='Bob'"), collection);
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    EXPECT_TRUE(json_string != NULL);
    printf("\n[json: %s]", json_string);
    EXPECT_TRUE(strstr(json_string, "\"full_name\": \"Bob Smith\"") != NULL);
    EXPECT_TRUE(strstr(json_string, "\"first_name\": \"Bob\"") != NULL);
    EXPECT_TRUE(strstr(json_string, "\"last_name\": \"Smith\"") != NULL);
    EXPECT_TRUE(strstr(json_string, "name_arr") == NULL);
  }
}

TEST_F(xapi, test_create_collection_index)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  mysqlx_stmt_t *stmt;
  const char *schema_name = "cc_crud_test";
  const char *coll_name = "index_test";
  const char *json[] = {
    "{\"zip\": [\"34239\", \"23456\"], \"zcount\": \"10\", \"some_text\": \"just some text\"}",
    "{\"zip\": [\"00001\", \"23456\"], \"zcount\": \"20\", \"some_text\": \"some more text\"}"
  };

  const char *geo_json =
  "{\"zip\": \"34239\", \"coords\" : { \"type\": \"Point\", \"coordinates\": [102.0, 0.0] }}";

  const char *json_idx = "{"\
             "\"fields\": ["\
             "{ \"field\": \"$.zip\", \"required\" : true , \"type\" : \"TEXT(10)\"},"\
             "{ \"field\": \"$.zcount\", \"type\" : \"INT UNSIGNED\" }]}";


  const char *geo_json_idx = "{"
             "\"type\" : \"SPATIAL\","
             "\"fields\": [{"
                "\"field\": \"$.coords\","
                "\"type\" : \"GEOJSON\","
                "\"required\" : true,"
                "\"options\": 2,"
                "\"srid\": 4326"
             "}]}";

  AUTHENTICATE();

  mysqlx_schema_drop(get_session(), schema_name);
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(get_session(), schema_name));
  schema = mysqlx_get_schema(get_session(), schema_name, 0);

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, coll_name));
  collection = mysqlx_get_collection(schema, coll_name, 0);

  stmt = mysqlx_collection_add_new(collection);
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json[0]));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json[1]));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  EXPECT_EQ(RESULT_OK,
            mysqlx_collection_create_index(collection, "custom_idx1", json_idx));

  EXPECT_EQ(RESULT_OK,
            mysqlx_collection_drop_index(collection, "custom_idx1"));

  /* Drop old collection and create a new one */
  EXPECT_EQ(RESULT_OK, mysqlx_collection_drop(schema, coll_name));
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, coll_name));
  collection = mysqlx_get_collection(schema, coll_name, 0);

  /*
    First we create a spatial index, then we insert the document.
    Otherwise the server-side reports error:

    "Collection contains document missing required field"
    Looks like it is an issue in xplugin.

    Also, the server 5.7 doesn't seem to handle spatial indexes
  */

  SKIP_IF_SERVER_VERSION_LESS(8, 0, 4);

  EXPECT_EQ(RESULT_OK,
            mysqlx_collection_create_index(collection, "geo_idx1", geo_json_idx));

  CRUD_CHECK(res = mysqlx_collection_add(collection, geo_json, PARAM_END),
             collection);

  EXPECT_EQ(RESULT_OK,
            mysqlx_collection_drop_index(collection, "geo_idx1"));

  SKIP_IF_SERVER_VERSION_LESS(8, 0, 17);

  EXPECT_EQ(RESULT_OK, mysqlx_collection_drop(schema, coll_name));
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, coll_name));
  collection = mysqlx_get_collection(schema, coll_name, 0);

  const char *multival_idx = "{"\
            "\"fields\": ["\
            "{ \"field\": \"$.zip\", \"type\" : \"CHAR(10)\", \"array\" : true}]}";
  printf("\nCreate multivalue index.");
  EXPECT_EQ(RESULT_OK,
            mysqlx_collection_create_index(collection, "multival_idx1", multival_idx));
  EXPECT_EQ(RESULT_OK, mysqlx_collection_drop(schema, coll_name));
}


TEST_F(xapi, test_row_locking)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  mysqlx_stmt_t *stmt;
  mysqlx_row_t *row;

  AUTHENTICATE();
  SKIP_IF_SERVER_VERSION_LESS(8, 0, 3);

  mysqlx_schema_drop(get_session(), "cc_crud_test");
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(get_session(), "cc_crud_test"));

  res = mysqlx_sql(get_session(), "CREATE TABLE cc_crud_test.row_locking" \
                   "(id int primary key)", MYSQLX_NULL_TERMINATED);
  EXPECT_TRUE(res != NULL);
  res = mysqlx_sql(get_session(), "INSERT INTO cc_crud_test.row_locking" \
                   "(id) VALUES (1),(2),(3)", MYSQLX_NULL_TERMINATED);
  EXPECT_TRUE(res != NULL);

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "row_locking", 1)) != NULL);

  EXPECT_EQ(RESULT_OK, mysqlx_transaction_begin(get_session()));
  stmt = mysqlx_table_select_new(table);
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_row_locking(stmt,
                                                     ROW_LOCK_EXCLUSIVE,
                                                     LOCK_CONTENTION_DEFAULT));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  printf("\nRows data:");
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t id = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &id));
    printf ("\n%d", (int)id);
  }

  res = mysqlx_sql(get_session(), "select trx_rows_locked " \
                   "from information_schema.innodb_trx " \
                   "where trx_mysql_thread_id = connection_id()",
                   MYSQLX_NULL_TERMINATED);
  EXPECT_TRUE(res != NULL);
  printf("\nLooking for locked rows:");
  int64_t rownum = 0;
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &rownum));
    printf(" %d", (int)rownum);
  }
  EXPECT_EQ(4, rownum);
  EXPECT_EQ(RESULT_OK, mysqlx_transaction_commit(get_session()));
  mysqlx_schema_drop(get_session(), "cc_crud_test");
}

TEST_F(xapi, lock_contention)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *sch;
  mysqlx_schema_t *sch_nolock;
  mysqlx_table_t *tbl;
  mysqlx_table_t *tbl_nolock;
  mysqlx_collection_t *coll;
  mysqlx_collection_t *coll_nolock;
  mysqlx_stmt_t *stmt;
  mysqlx_stmt_t *stmt2;
  size_t res_num;

  AUTHENTICATE();

  exec_sql("SET SESSION innodb_lock_wait_timeout = 5");
  exec_sql("SET GLOBAL innodb_lock_wait_timeout = 5");

  EXPECT_TRUE((sch = mysqlx_get_schema(get_session(), "test", 1)) != NULL);

  mysqlx_collection_drop(sch, "c1");
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(sch, "c1"));

  tbl = mysqlx_get_table(sch, "c1" , false);
  coll = mysqlx_get_collection(sch, "c1", true);

  mysqlx_collection_remove(coll, "true");

  stmt = mysqlx_collection_add_new(coll);
  for(int i = 0; i < 10; ++i)
  {
    std::stringstream doc;
    doc << R"({"name":"Luis", "_id":)" << i+1 << "}";
    mysqlx_set_add_document(stmt, doc.str().c_str());
  }
  mysqlx_execute(stmt);
  mysqlx_free(stmt);

  /*
    First session lock the rows, second one, tries to read/write values
  */
  auto s_nolock = mysqlx_get_session(m_xplugin_host,
                              m_port,
                              m_xplugin_usr,
                              m_xplugin_pwd,
                              NULL,
                              NULL);

  EXPECT_TRUE((sch_nolock = mysqlx_get_schema(s_nolock, "test", 1)) != NULL);

  coll_nolock = mysqlx_get_collection(sch_nolock,"c1", true);
  tbl_nolock = mysqlx_get_table(sch_nolock,"c1", false);

  mysqlx_transaction_begin(get_session());
  mysqlx_transaction_begin(s_nolock);

  stmt = mysqlx_table_select_new(tbl);
  mysqlx_set_where(stmt, "_id like '2'");
  mysqlx_set_row_locking(stmt, ROW_LOCK_EXCLUSIVE, LOCK_CONTENTION_DEFAULT);
  res = mysqlx_execute(stmt);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(1, res_num);
  mysqlx_free(res);
//  mysqlx_free(stmt);

  stmt2 = mysqlx_table_select_new(tbl_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_EXCLUSIVE, LOCK_CONTENTION_SKIP_LOCKED);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(9, res_num);
  mysqlx_free(res);
  mysqlx_free(stmt2);

  stmt2 = mysqlx_collection_find_new(coll_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_EXCLUSIVE, LOCK_CONTENTION_SKIP_LOCKED);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(9, res_num);
  mysqlx_free(res);
  mysqlx_free(stmt2);

  stmt2 = mysqlx_table_select_new(tbl_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_EXCLUSIVE, LOCK_CONTENTION_NOWAIT);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_ERROR,mysqlx_store_result(res, &res_num));
  mysqlx_free(res);
  mysqlx_free(stmt2);

  stmt2 = mysqlx_collection_find_new(coll_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_EXCLUSIVE, LOCK_CONTENTION_NOWAIT);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_ERROR,mysqlx_store_result(res, &res_num));
  mysqlx_free(res);
  mysqlx_free(stmt2);

  mysqlx_free(stmt);

  mysqlx_transaction_rollback(get_session());
  mysqlx_transaction_rollback(s_nolock);

  /*
    Shared lock tests
  */

  mysqlx_transaction_begin(get_session());
  mysqlx_transaction_begin(s_nolock);


  stmt = mysqlx_table_select_new(tbl);
  mysqlx_set_where(stmt, "_id like '3'");
  mysqlx_set_row_locking(stmt, ROW_LOCK_SHARED, LOCK_CONTENTION_DEFAULT);
  res = mysqlx_execute(stmt);
  EXPECT_TRUE(NULL != res);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(1, res_num);
  mysqlx_free(res);
  mysqlx_free(stmt);

  stmt2 = mysqlx_table_select_new(tbl_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_SHARED, LOCK_CONTENTION_SKIP_LOCKED);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(10, res_num);
  mysqlx_free(res);
  mysqlx_free(stmt2);

  stmt2 = mysqlx_collection_find_new(coll_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_SHARED, LOCK_CONTENTION_SKIP_LOCKED);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(10, res_num);
  mysqlx_free(res);
  mysqlx_free(stmt2);

  stmt2 = mysqlx_table_select_new(tbl_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_SHARED, LOCK_CONTENTION_NOWAIT);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(10, res_num);
  mysqlx_free(res);
  mysqlx_free(stmt2);

  stmt2 = mysqlx_collection_find_new(coll_nolock);
  mysqlx_set_row_locking(stmt2, ROW_LOCK_SHARED, LOCK_CONTENTION_NOWAIT);
  res = mysqlx_execute(stmt2);
  EXPECT_EQ(RESULT_OK,mysqlx_store_result(res, &res_num));
  EXPECT_EQ(10, res_num);
  mysqlx_free(res);
  mysqlx_free(stmt2);

  //Should timeout!
  stmt2 = mysqlx_collection_modify_new(coll_nolock);
  mysqlx_set_modify_set(stmt2, "name",PARAM_STRING("Bogdan"),PARAM_END);
  EXPECT_EQ(NULL, mysqlx_execute(stmt2));

  std::thread thread_modify([&] {
   stmt2 = mysqlx_collection_modify_new(coll_nolock);
   mysqlx_set_modify_set(stmt2, "name",PARAM_STRING("Bogdan"),PARAM_END);
   res = mysqlx_execute(stmt2);
   EXPECT_TRUE(NULL != res);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  mysqlx_transaction_rollback(get_session());

  thread_modify.join();

  mysqlx_free(stmt2);

  mysqlx_transaction_rollback(s_nolock);

}


TEST_F(xapi, test_having_group_by)
{
  SKIP_TEST("bug#26310713");
  SKIP_IF_NO_XPLUGIN;

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  mysqlx_stmt_t *stmt;
  mysqlx_row_t *row;
  mysqlx_collection_t *collection;
  int row_num = 1;
  const char *json_string = NULL;
  size_t json_len = 0;

  AUTHENTICATE();

  //TODO: Remove this when  Bug #86754 is fixed
  SKIP_IF_SERVER_VERSION_LESS(5,7,19);

  mysqlx_schema_drop(get_session(), "cc_crud_test");
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(get_session(), "cc_crud_test"));

  res = mysqlx_sql(get_session(), "CREATE TABLE cc_crud_test.group_test" \
                                  "(id int primary key," \
                                  "user_name varchar(32))",
                                  MYSQLX_NULL_TERMINATED);
  EXPECT_TRUE(res !=  NULL);
  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "group_test", 1)) != NULL);

  stmt = mysqlx_table_insert_new(table);
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_columns(stmt, "id", "user_name", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(1), PARAM_STRING("John"), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(2), PARAM_STRING("Mary"), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(3), PARAM_STRING("Alan"), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(4), PARAM_STRING("Anna"), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(5), PARAM_STRING("Peter"), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(6), PARAM_STRING("Anna"), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(7), PARAM_STRING("Peter"), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(8), PARAM_STRING("Anna"), PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  stmt = mysqlx_table_select_new(table);
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_items(stmt, "COUNT(*) AS cnt", "user_name", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_group_by(stmt, "user_name", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_having(stmt, "COUNT(*) > 1"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_order_by(stmt, "user_name", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  /*
    This is the expected result
    +-----+-----------+
    | cnt | user_name |
    +-----+-----------+
    |   3 | Anna      |
    |   2 | Peter     |
    +-----+-----------+
  */

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t cnt = 0;
    char buf[256];
    size_t buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &cnt));
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));

    printf("\n Row # %d: ", row_num);
    printf("[ %d ] [ %s ]", (int)cnt, buf);

    switch (row_num)
    {
      case 1:
        EXPECT_EQ(cnt, 3);
        EXPECT_EQ(buflen, 5);
        EXPECT_STREQ(buf, "Anna");
      break;
      case 2:
        EXPECT_EQ(cnt, 2);
        EXPECT_EQ(buflen, 6);
        EXPECT_STREQ(buf, "Peter");
        break;
      default:
        FAIL();
    }
    ++row_num;
  }

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "coll_group"));
  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "coll_group", 1)) != NULL);
  stmt = mysqlx_collection_add_new(collection);
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 1, \"user_name\" : \"John\"}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 2, \"user_name\" : \"Mary\"}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 3, \"user_name\" : \"Alan\"}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 4, \"user_name\" : \"Anna\"}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 5, \"user_name\" : \"Peter\"}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 6, \"user_name\" : \"Anna\"}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 7, \"user_name\" : \"Peter\"}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"num\": 8, \"user_name\" : \"Anna\"}"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  stmt = mysqlx_collection_find_new(collection);
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_projection(stmt, "{cnt: COUNT(*), user_name: user_name}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_group_by(stmt, "user_name", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_having(stmt, "cnt>1"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, "user_name", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  row_num = 1;
  /*
    This is the expected result:
    {"cnt": 3, "user_name": "Anna"}
    {"cnt": 2, "user_name": "Peter"}
  */
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);
    switch (row_num)
    {
      case 1:
        EXPECT_STREQ("{\"cnt\": 3, \"user_name\": \"Anna\"}", json_string);
        break;
      case 2:
        EXPECT_STREQ("{\"cnt\": 2, \"user_name\": \"Peter\"}", json_string);
        break;
      default:FAIL();
    }
    ++row_num;
  }
}

TEST_F(xapi, schema)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_schema_t *schema, *schema2;
  mysqlx_collection_t *coll;
  const char *schema_name = "simple_schema111";
  const char *schema_non_existing = "non_existing_schema";
  const char *coll_name = "simple_collection";
  const char *coll_non_existing = "non_existing_collection";

  AUTHENTICATE();

  mysqlx_schema_drop(get_session(), schema_name);
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(get_session(), schema_name));

  schema = mysqlx_get_schema(get_session(), schema_name, 1);
  EXPECT_TRUE(schema != NULL);

  EXPECT_EQ(NULL, mysqlx_get_schema(get_session(), schema_non_existing, 1));

  // Do not check if schema exists
  schema2 = mysqlx_get_schema(get_session(), schema_non_existing, 0);
  EXPECT_TRUE(schema2 != NULL);

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, coll_name));

  coll = mysqlx_get_collection(schema, coll_name, 1);
  EXPECT_TRUE(coll != NULL);

  EXPECT_EQ(NULL, mysqlx_get_collection(schema, coll_non_existing, 1));

  printf("\n Got the schema %s", schema_name);
}


TEST_F(xapi, basic)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int row_num = 0, col_num = 0;
  int i = 0;

  const char *col_names[2] = { "id", "vctext" };
  int64_t ids[2] = { 10, 20 };
  const char *vctexts[2] = { "abcdef", "ghijkl" };

  AUTHENTICATE();

  for (i = 0; i < 4; i++)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_basic", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_limit_and_offset(stmt, 2, 0));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_where(stmt, "(id / 2) > 4"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  col_num = mysqlx_column_get_count(res);
  EXPECT_EQ(col_num, 2);

  for (i = 0; i < col_num; i++)
  {
    const char *col_name = mysqlx_column_get_name(res, i);
    const char *col_orig_name = mysqlx_column_get_original_name(res, i);
    const char *col_table = mysqlx_column_get_table(res, i);
    const char *col_orig_table = mysqlx_column_get_original_table(res, i);
    const char *col_schema = mysqlx_column_get_schema(res, i);
    const char *col_cat = mysqlx_column_get_catalog(res, i);

    EXPECT_NE(nullptr, col_cat);

    printf("\n Column # %d", i + 1);
    printf("\n * name: %s, orig name: %s, table: %s, orig table: %s, schema: %s, catalog: %s",
      col_name, col_orig_name, col_table, col_orig_table, col_schema, col_cat);

    EXPECT_STREQ(col_name, col_names[i]);
    EXPECT_STREQ(col_orig_name, col_names[i]);
    EXPECT_STREQ(col_table, "crud_basic");
    EXPECT_STREQ(col_orig_table, "crud_basic");
    EXPECT_STREQ(col_schema, "cc_crud_test");
  }

  printf("\n\nRows:");
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t id = 0;
    char buf[256];
    size_t buflen = sizeof(buf);
    void *dummy_ptr = (void*)&buf;

    // This should give error when column index is out of range
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_sint(row, 20, (int64_t*)dummy_ptr));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_uint(row, 20, (uint64_t*)dummy_ptr));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_bytes(row, 20, 0, dummy_ptr, &buflen));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_double(row, 20, (double*)dummy_ptr));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_float(row, 20, (float*)dummy_ptr));

    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &id));

    EXPECT_EQ(id, ids[row_num]);
    printf("\n Row # %d: ", row_num);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    EXPECT_EQ(buflen, strlen(vctexts[row_num]) + 1);
    printf ("[ %d ] [ %s ]", (int)id, buf);
    EXPECT_STREQ(buf, vctexts[row_num]);
    ++row_num;
  }

  EXPECT_EQ(row_num, 2); // we expect only two rows
  printf("\n");

  RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), queries[4], strlen(queries[4])));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
}


TEST_F(xapi, deleting)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int row_num = 0;
  int i = 0;

  AUTHENTICATE();

  // Skip drop/create database
  for (i = 2; i < 4; i++)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_basic", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_delete_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_delete_where(stmt, "(id = 10) OR (id = 20) OR (id = 30)"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  EXPECT_EQ(mysqlx_get_affected_count(res), 3);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  row_num = 0;
  printf("\n\nRows:");
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t id = 0;
    char buf[256];
    size_t buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &id));

    EXPECT_EQ(id, 2);
    printf("\n Row # %d: ", row_num);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    EXPECT_EQ(buflen, 7);

    printf ("[ %d ] [ %s ]", (int)id, buf);
    EXPECT_STREQ(buf, "012345");
    ++row_num;
  }
  EXPECT_EQ(row_num, 1);

  printf("\n");
}


TEST_F(xapi, order_by_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int row_num = 0;
  int desc_ids[4] = { 30, 20, 10, 2};
  const char* str_data[4] = {"mnopqr", "ghijkl", "abcdef", "012345" };
  int i = 0;

  AUTHENTICATE();

  for (i = 0; i < 4; i++)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_basic", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_order_by(stmt, "cc_crud_test.crud_basic.id",
                                               SORT_ORDER_DESC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  row_num = 0;
  printf("\n\nRows:");
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t id = 0;
    char buf[256];
    size_t buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &id));

    EXPECT_EQ(id, desc_ids[row_num]);
    printf("\n Row # %d: ", row_num);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    EXPECT_EQ(buflen, 7);
    printf ("[ %d ] [ %s ]", (int)id, buf);
    EXPECT_STREQ(buf, str_data[row_num]);
    ++row_num;
  }
  EXPECT_EQ(row_num, 4);

  printf("\n");
}


TEST_F(xapi, placeholder_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int row_num = 0;
  int i = 0;

  const char *query = "INSERT INTO cc_crud_test.crud_placeholder_test " \
                      "(sint, uint, flv, dbv, strv) VALUES (?,?,?,?,?)";
  int64_t v_sint = -17;
  uint64_t v_uint = 101;
  float v_float = 3.31f;
  double v_double = 1.7E+308;
  const char *v_str = "just some text";

  const char *queries2[] = {
    "DROP TABLE IF EXISTS cc_crud_test.crud_placeholder_test",
    "CREATE TABLE cc_crud_test.crud_placeholder_test " \
      "(sint BIGINT, uint BIGINT UNSIGNED, flv FLOAT, dbv DOUBLE, strv VARCHAR(255))"
  };

  AUTHENTICATE();

  for (i = 0; i < 2; i++)
  {
    exec_sql(queries2[i]);
  }

  RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), query, strlen(query)));
  EXPECT_EQ(0, mysqlx_stmt_bind(stmt, PARAM_SINT(v_sint),
                                  PARAM_UINT(v_uint),
                                  PARAM_FLOAT(v_float),
                                  PARAM_DOUBLE(v_double),
                                  PARAM_STRING(v_str),
                                  PARAM_END));

  RESULT_CHECK(res = mysqlx_execute(stmt));

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_placeholder_test", 1)) != NULL);
  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  row_num = 0;
  printf("\n\nRows:");
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    printf("\n Row # %d: ", row_num);

    int64_t v_sint2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &v_sint2));

    EXPECT_EQ(v_sint, v_sint2);

    uint64_t v_uint2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_uint(row, 1, &v_uint2));
    EXPECT_EQ(v_uint, v_uint2);

    float v_float2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_float(row, 2, &v_float2));
    EXPECT_EQ(v_float, v_float2);

    double v_double2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_double(row, 3, &v_double2));
    EXPECT_EQ(v_double, v_double2);

    char v_str2[256];
    size_t buflen = sizeof(v_str2);

    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 4, 0, v_str2, &buflen));
    EXPECT_EQ(buflen, strlen(v_str) + 1);
    EXPECT_STREQ(v_str2, v_str);
    ++row_num;
  }
  EXPECT_EQ(row_num, 1);

  printf("\n");
}


TEST_F(xapi, insert_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int row_num = 0;
  int i = 0;

  int64_t v_sint[2] = { -17, 34 };
  uint64_t v_uint[2] = { 101, 23234 };
  float v_float[2] = { 3.31f, 12.27f };
  double v_double[2] = { 1.7E+308, 2.8E-100 };
  const char *v_str[2] = { "just some text", "more text" };

  const char *init_queries[] = {
    "DROP TABLE IF EXISTS cc_crud_test.crud_insert_test",
    "CREATE TABLE cc_crud_test.crud_insert_test " \
      "(sint BIGINT, uint BIGINT UNSIGNED, flv FLOAT, dbv DOUBLE, strv VARCHAR(255))"
  };

  AUTHENTICATE();

  for (i = 0; i < 2; i++)
  {
    printf("\nExecuting query:\n  %s ... ", init_queries[i]);
    RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), init_queries[i], strlen(init_queries[i])));
    CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_insert_test", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_insert_new(table));

  // Give columns in different order than is defined in the table
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_columns(stmt, "strv", "sint", "dbv",
                                                 "uint", "flv", PARAM_END));
  for (i = 0; i < 2; ++i)
  {
    EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt,
                                    PARAM_STRING(v_str[i]),
                                    PARAM_SINT(v_sint[i]),
                                    PARAM_DOUBLE(v_double[i]),
                                    PARAM_UINT(v_uint[i]),
                                    PARAM_FLOAT(v_float[i]),
                                    PARAM_END));
  }

  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  row_num = 0;
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t v_sint2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &v_sint2));

    EXPECT_EQ(v_sint[row_num], v_sint2);

    uint64_t v_uint2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_uint(row, 1, &v_uint2));
    EXPECT_EQ(v_uint[row_num], v_uint2);

    float v_float2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_float(row, 2, &v_float2));
    EXPECT_EQ(v_float[row_num], v_float2);

    double v_double2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_double(row, 3, &v_double2));
    EXPECT_EQ(v_double[row_num], v_double2);

    char v_str2[256];
    size_t buflen = sizeof(v_str2);

    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 4, 0, v_str2, &buflen));
    EXPECT_EQ(buflen, strlen(v_str[row_num]) + 1);
    EXPECT_STREQ(v_str2, v_str[row_num]);
    ++row_num;
  }
  EXPECT_EQ(row_num, 2);

}

/*
TEST_F(xapi, find_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_doc_t *doc;
  int i = 0;

  const char *init_queries[4] = {
     "DROP DATABASE IF EXISTS cc_crud_test",
     "CREATE DATABASE cc_crud_test",
     "CREATE TABLE cc_crud_test.crud_collection (id int auto_increment primary key, doc JSON)",
     "INSERT INTO cc_crud_test.crud_collection (doc) VALUES " \
       "('{ \"mykey\" : 1, \"myvalue\" : \"hello world\",       \"dval\" : 3.89E-8 }')," \
       "('{ \"mykey\" : 2, \"myvalue\" : \"how are you world\", \"dval\" : 4.3212392E+3 }')," \
       "('{ \"mykey\" : 3, \"myvalue\" : \"bye world\",         \"dval\" : 3.8900001 }')," \
       "('{ \"mykey\" : 4, \"myvalue\" : \"hello again world\", \"dval\" : 7.00092E-3 }')," \
       "('{ \"mykey\" : 5, \"myvalue\" : \"so long world\",     \"dval\" : 8.888E+20 }')"
  };

  AUTHENTICATE();

  for (i = 0; i < 4; i++)
  {
    printf("\nExecuting query:\n  %s ... ", init_queries[i]);
    RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), init_queries[i], strlen(init_queries[i])));
    CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  }

  RESULT_CHECK(stmt = mysqlx_collection_find_new(get_session(), "cc_crud_test", "crud_collection"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_criteria(stmt, "mykey > 1"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_limit_and_offset(stmt, 2, 1));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  while ((doc = mysqlx_doc_fetch_one(res)) != NULL)
  {
    int64_t mykey = 0;
    char myval[256];
    size_t buflen = sizeof(myval);

    EXPECT_EQ(RESULT_OK, mysqlx_doc_get_sint(doc, "mykey", &mykey));
    printf("\n[mykey: %ld]", mykey);
    if (mysqlx_doc_get_bytes(doc, "myvalue", 0, myval, &buflen) == RESULT_ERROR)
    {
      printf("[Expected error: %s]", mysqlx_stmt_error_message(stmt));
    }

    buflen = sizeof(myval);
    EXPECT_EQ(RESULT_OK, mysqlx_doc_get_str(doc, "myvalue", 0, myval, &buflen));
    printf("[myval: %s]", myval);
  }
}
*/


TEST_F(xapi, ddl_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_session_t *sess;
  mysqlx_schema_t *schema;

  AUTHENTICATE();

  sess = get_session();

  EXPECT_EQ(RESULT_OK, mysqlx_schema_drop(sess, "cc_ddl_test"));
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(sess, "cc_ddl_test"));
  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_ddl_test", 1)) != NULL);

  exec_sql("CREATE TABLE cc_ddl_test.ddl_table (id int)");
  exec_sql("CREATE VIEW cc_ddl_test.ddl_view AS SELECT * FROM cc_ddl_test.ddl_table");
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "ddl_collection"));

  // Check that the collection is created
  exec_sql("SELECT * FROM cc_ddl_test.ddl_collection");

  // Drop an existing collection. Expect OK
  EXPECT_EQ(RESULT_OK, mysqlx_collection_drop(schema, "ddl_collection"));

  // Check that the collection is dropped
  exec_sql_error("SELECT * FROM cc_ddl_test.ddl_collection");

  // Drop a non-existing collection. Expect OK
  EXPECT_EQ(RESULT_OK, mysqlx_collection_drop(schema, "ddl_collection"));

  // Try creating schema with the same name, expect OK
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(sess, "cc_ddl_test"));

  // Try dropping schema, expect OK
  EXPECT_EQ(RESULT_OK, mysqlx_schema_drop(sess, "cc_ddl_test2"));

  // The schema with this name should not exist at this stage, expect OK
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(sess, "cc_ddl_test2"));

  // Check that the schema is created
  exec_sql("CREATE TABLE cc_ddl_test2.wrong_table (id INT)");

  // Dropping an existing schema, expect OK
  EXPECT_EQ(RESULT_OK, mysqlx_schema_drop(sess, "cc_ddl_test2"));

  // Check that the schema is dropped
  exec_sql_error("CREATE TABLE cc_ddl_test2.wrong_table (id INT)");

  // Check that the view exists
  exec_sql("SELECT * FROM cc_ddl_test.ddl_view");

  // Check that the table exists
  exec_sql("SELECT * FROM cc_ddl_test.ddl_table");

  // Drop the test schema
  EXPECT_EQ(RESULT_OK, mysqlx_schema_drop(get_session(), "cc_ddl_test"));
}


TEST_F(xapi, json_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  const char * json_string = NULL;
  int i = 0;
  size_t json_len = 0;
  char insert_buf[1024];

  AUTHENTICATE();
  exec_sql("DROP DATABASE IF EXISTS cc_crud_test");
  exec_sql("CREATE DATABASE cc_crud_test");

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));

  // Insert first document with known length.

  for (i = 0; i < 5; i++)
  {
    sprintf(insert_buf, "INSERT INTO cc_crud_test.crud_collection (doc) VALUES " \
                        "('%s')", json_row[i]);
    RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), insert_buf, strlen(insert_buf)));
    CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  }

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "crud_collection", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_collection_find_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_criteria(stmt, "a_key > 1"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_limit_and_offset(stmt, 2, 1));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  i = 2; // It is expected the rows will be returned starting from 2
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    EXPECT_STREQ(json_row[i], json_string);

    /*
      Note: json_len contains total number of bytes in the returned string,
      includeing the '\0' terminator.
    */

    EXPECT_EQ(strlen(json_string) + 1, json_len);
    ++i;
  }
}


TEST_F(xapi, null_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  int i = 0;

  const char *init_queries[4] = {
     "DROP DATABASE IF EXISTS cc_crud_test",
     "CREATE DATABASE cc_crud_test",
     "CREATE TABLE cc_crud_test.crud_null (id int primary key, " \
       "sint BIGINT, uint BIGINT UNSIGNED, flv FLOAT, dbv DOUBLE, " \
       "strv VARCHAR(255))",
     "INSERT INTO cc_crud_test.crud_null (id) VALUES (1) "
  };

  AUTHENTICATE();

  for (i = 0; i < 4; i++)
  {
    exec_sql(init_queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_null", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t v_sint = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &v_sint));
    EXPECT_EQ(v_sint, 1);

    int64_t v_sint2 = 0;
    EXPECT_EQ(RESULT_NULL, mysqlx_get_sint(row, 1, &v_sint2));

    uint64_t v_uint2 = 0;
    EXPECT_EQ(RESULT_NULL, mysqlx_get_uint(row, 2, &v_uint2));

    float v_float2 = 0;
    EXPECT_EQ(RESULT_NULL, mysqlx_get_float(row, 3, &v_float2));

    double v_double2 = 0;
    EXPECT_EQ(RESULT_NULL, mysqlx_get_double(row, 4, &v_double2));

    char v_str2[256];
    size_t buflen = sizeof(v_str2);

    EXPECT_EQ(RESULT_NULL, mysqlx_get_bytes(row, 5, 0, v_str2, &buflen));
  }
}


TEST_F(xapi, param_safety_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  mysqlx_collection_t *collection;
  mysqlx_session_t *session;
  mysqlx_session_options_t *opt;
  int i = 0;
  char buf[255];

  const char *init_queries[4] = {
    "DROP DATABASE IF EXISTS cc_crud_test",
    "CREATE DATABASE cc_crud_test",
    "CREATE TABLE cc_crud_test.crud_test (a int)",
    "INSERT INTO cc_crud_test.crud_test (a) VALUES (1) "
  };

  AUTHENTICATE();

  for (i = 0; i < 4; i++)
  {
    exec_sql(init_queries[i]);
  }

  /* Schema creating */
  EXPECT_EQ(RESULT_ERROR, mysqlx_schema_create(NULL, "new_schema"));
  EXPECT_EQ(RESULT_ERROR, mysqlx_schema_create(get_session(), NULL));
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));
  EXPECT_EQ(RESULT_ERROR, mysqlx_schema_create(get_session(), ""));
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));

  /* Schema dropping */
  EXPECT_EQ(RESULT_ERROR, mysqlx_schema_drop(NULL, "new_schema"));
  EXPECT_EQ(RESULT_ERROR, mysqlx_schema_drop(get_session(), NULL));
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));
  EXPECT_EQ(RESULT_ERROR, mysqlx_schema_drop(get_session(), ""));
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));

  /* Schema getting */
  EXPECT_TRUE(mysqlx_get_schema(NULL, "cc_crud_test", 1) == NULL);
  EXPECT_TRUE(mysqlx_get_schema(get_session(), NULL, 1) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));
  EXPECT_TRUE(mysqlx_get_schema(get_session(), "", 1) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));
  EXPECT_TRUE(mysqlx_get_schema(get_session(), "nonexisting_schema", 1) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));
  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);

  /* Table getting */
  EXPECT_TRUE(mysqlx_get_table(NULL, "crud_test", 1) == NULL);
  EXPECT_TRUE(mysqlx_get_table(schema, NULL, 1) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(schema));
  EXPECT_TRUE(mysqlx_get_table(schema, "", 1) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(schema));
  EXPECT_TRUE(mysqlx_get_table(schema, "nonexisting_table", 1) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(schema));
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_test", 1)) != NULL);

  /* Collection creating */
  EXPECT_EQ(RESULT_ERROR, mysqlx_collection_create(NULL, "collection_test"));
  EXPECT_EQ(RESULT_ERROR, mysqlx_collection_create(schema, NULL));
  printf("\nExpected error: %s", mysqlx_error_message(schema));
  EXPECT_EQ(RESULT_ERROR, mysqlx_collection_create(schema, ""));
  printf("\nExpected error: %s", mysqlx_error_message(schema));

  /* Collection dropping */
  EXPECT_EQ(RESULT_ERROR, mysqlx_collection_drop(NULL, "collection_test"));
  EXPECT_EQ(RESULT_ERROR, mysqlx_collection_drop(schema, NULL));
  printf("\nExpected error: %s", mysqlx_error_message(schema));
  EXPECT_EQ(RESULT_ERROR, mysqlx_collection_drop(schema, ""));
  printf("\nExpected error: %s", mysqlx_error_message(schema));


  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "collection_test"));

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "collection_test", 1)) != NULL);


  /* Collection FIND, ADD, MODIFY and REMOVE one-call ops */
  EXPECT_TRUE((res = mysqlx_collection_find(NULL, NULL)) == NULL);
  EXPECT_TRUE((res = mysqlx_collection_find(collection, NULL)) != NULL);
  EXPECT_TRUE((res = mysqlx_collection_add(NULL, PARAM_END)) == NULL);
  EXPECT_TRUE((res = mysqlx_collection_add(collection, PARAM_END)) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(collection));
  EXPECT_TRUE((res = mysqlx_collection_modify_set(collection, NULL, PARAM_END)) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(collection));
  EXPECT_TRUE((res = mysqlx_collection_modify_unset(collection, NULL, PARAM_END)) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(collection));
  EXPECT_TRUE((res = mysqlx_collection_remove(collection, NULL)) != NULL);

  /* Table INSERT */
  EXPECT_TRUE((res = mysqlx_table_insert(NULL, PARAM_END)) == NULL);
  EXPECT_TRUE((res = mysqlx_table_insert(table, PARAM_END)) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(table));

  /* Table DELETE */
  EXPECT_TRUE((res = mysqlx_table_delete(NULL, NULL)) == NULL);
  RESULT_CHECK(res = mysqlx_table_delete(table, NULL));

  /* Table UPDATE */
  EXPECT_TRUE((res = mysqlx_table_update(NULL, NULL, PARAM_END)) == NULL);
  EXPECT_TRUE((res = mysqlx_table_update(table, NULL, PARAM_END)) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(table));

  /* Insert sample data in the table */
  RESULT_CHECK(res = mysqlx_table_insert(table, "a", PARAM_SINT(10), PARAM_END));

  /* Table SELECT */
  EXPECT_TRUE((res = mysqlx_table_select(NULL, NULL)) == NULL);
  EXPECT_TRUE((row = mysqlx_row_fetch_one(NULL)) == NULL);

  RESULT_CHECK(res = mysqlx_table_select(table, NULL));

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_sint(row, 0, NULL));
    printf("\nExpected error: %s", mysqlx_error_message(row));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_uint(row, 0, NULL));
    printf("\nExpected error: %s", mysqlx_error_message(row));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_float(row, 0, NULL));
    printf("\nExpected error: %s", mysqlx_error_message(row));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_double(row, 0, NULL));
    printf("\nExpected error: %s", mysqlx_error_message(row));
    EXPECT_EQ(RESULT_ERROR, mysqlx_get_bytes(row, 0, 0, NULL, NULL));
    printf("\nExpected error: %s", mysqlx_error_message(row));
  }

  /* We don't know for sure if it will connect, but it should not crash*/
  session = mysqlx_get_session(NULL, 0, NULL, NULL, NULL, NULL);
  mysqlx_session_close(session);
  session = mysqlx_get_session(NULL, 0, NULL, NULL, NULL, NULL);
  mysqlx_session_close(session);
  session = mysqlx_get_session_from_url(NULL, NULL);
  mysqlx_session_close(session);
  session = mysqlx_get_session_from_url(NULL, NULL);
  mysqlx_session_close(session);
  session = mysqlx_get_session_from_options(NULL, NULL);
  mysqlx_session_close(session);
  session = mysqlx_get_session_from_options(NULL, NULL);
  mysqlx_session_close(session);

  stmt = mysqlx_collection_add_new(collection);
  EXPECT_EQ(RESULT_ERROR, mysqlx_set_add_document(stmt, NULL));
  printf("\nExpected error: %s", mysqlx_error_message(stmt));
  EXPECT_TRUE( mysqlx_execute(stmt) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(stmt));

  stmt = mysqlx_collection_find_new(collection);
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_projection(stmt, NULL));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_criteria(stmt, NULL));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, PARAM_END));
  RESULT_CHECK(res = mysqlx_execute(stmt));
  EXPECT_EQ(RESULT_OK, mysqlx_store_result(res, NULL));

  stmt = mysqlx_table_insert_new(table);
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_columns(stmt, NULL));
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_UINT(120), PARAM_END));
  RESULT_CHECK(res = mysqlx_execute(stmt));

  stmt = mysqlx_table_update_new(table);
  EXPECT_EQ(RESULT_ERROR, mysqlx_set_update_values(stmt, PARAM_END));
  printf("\nExpected error: %s", mysqlx_error_message(stmt));
  EXPECT_TRUE(mysqlx_execute(stmt) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(stmt));

  buf[0] = 0;
  opt = mysqlx_session_options_new();
  // option not set yet
  EXPECT_EQ(RESULT_ERROR, mysqlx_session_option_get(opt, MYSQLX_OPT_HOST, buf));
  printf("\nExpected error: %s", mysqlx_error_message(opt));
  mysqlx_session_option_set(opt, MYSQLX_OPT_HOST, "localhost", PARAM_END);
  EXPECT_EQ(RESULT_ERROR, mysqlx_session_option_get(opt, MYSQLX_OPT_HOST, NULL));
  printf("\nExpected error: %s", mysqlx_error_message(opt));
  EXPECT_EQ(RESULT_OK, mysqlx_session_option_get(opt, MYSQLX_OPT_HOST, buf));
  EXPECT_STRCASEEQ("localhost", buf);

  EXPECT_TRUE(mysqlx_sql(get_session(), NULL, MYSQLX_NULL_TERMINATED) == NULL);
  printf("\nExpected error: %s", mysqlx_error_message(get_session()));

  stmt = mysqlx_sql_new(get_session(), "SHOW DATABASES LIKE ?", MYSQLX_NULL_TERMINATED);
  EXPECT_EQ(RESULT_ERROR, mysqlx_stmt_bind(stmt, PARAM_END));
  printf("\nExpected error: %s", mysqlx_error_message(stmt));
  EXPECT_TRUE(mysqlx_execute(stmt) == NULL);

  mysqlx_free(opt);
}


TEST_F(xapi, long_data_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  uint32_t col_num = 0;
  const char *col_name;
  mysqlx_data_type_t col_type;
  char *data_buf;
  size_t buf_len = 2000000;
  int i = 0;

  // A long piece of data 1M
  const char *query = "SELECT BINARY REPEAT('z', 1000000) as longdata";

  AUTHENTICATE();

  RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), query, strlen(query)));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  col_num = mysqlx_column_get_count(res);
  EXPECT_EQ(col_num, 1);
  col_name = mysqlx_column_get_name(res, 0);
  EXPECT_STREQ(col_name, "longdata");
  col_type = (mysqlx_data_type_t)mysqlx_column_get_type(res, 0);
  EXPECT_EQ(MYSQLX_TYPE_BYTES, col_type);

  data_buf = (char*)malloc(buf_len);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    memset(data_buf, 1, buf_len);
    // Give the buffer with the size of 2M, but expect to get only 1M
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, data_buf, &buf_len));
    /*
      Take into account that data was converted from string with the trailing
      '\0' byte at the end
    */
    EXPECT_EQ(1000001, buf_len);

    // All bytes of the result must be set to the same value 'z'
    for(i = 0; i < 1000000; ++i)
    {
      EXPECT_EQ('z', data_buf[i]);
      if ('z' != data_buf[i])
        break; // Don't flood the log with millions of error messages
    }

    EXPECT_EQ(0, data_buf[1000000]);

    // All remaining bytes have to remain 1
    for(i = 1000001; i < 2000000; ++i)
    {
      EXPECT_EQ(1, data_buf[i]);
      if (1 != data_buf[i])
        break; // Don't flood the log with millions of error messages
    }
  }
  free(data_buf);
}


TEST_F(xapi, projections_tab)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  mysqlx_table_t *table_coll;

  int row_num = 0, col_num = 0;
  int i = 0;

  AUTHENTICATE();

  // Skip drop/create database
  for (i = 0; i < 4; i++)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_basic", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_items(stmt, "id", "id*2 AS id2", "800", "vctext", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_where(stmt, "id = 10"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  col_num = mysqlx_column_get_count(res);
  EXPECT_EQ(col_num, 4);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t id = 0, id2 = 0, int800 = 0;

    char buf[256];
    size_t buflen = sizeof(buf);
    printf("\n Row # %d: ", row_num);
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &id));
    EXPECT_EQ(10, id);
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 1, &id2));
    EXPECT_EQ(20, id2);
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 2, &int800));
    EXPECT_EQ(800, int800);

    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 3, 0, buf, &buflen));
    EXPECT_EQ(6 + 1, buflen);
    printf ("[ %d ] [ %d ] [ %d ] [ %s ]\n", (int)id, (int)id2 , (int)int800, buf);
    EXPECT_STREQ("abcdef", buf);
    ++row_num;
  }

  EXPECT_EQ(row_num, 1); // we expect only one row

  // Checking projection that involves document paths.

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));

  for (i = 0; i < 5; i++)
  {
    const char *insert = "INSERT INTO cc_crud_test.crud_collection (doc) VALUES (?)";
    RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), insert, strlen(insert)));
    EXPECT_EQ(RESULT_OK, mysqlx_stmt_bind(stmt, PARAM_STRING(json_row[i]), PARAM_END));
    CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  }

  // Do not check if this is a table because it is a collection
  EXPECT_TRUE((table_coll = mysqlx_get_table(schema, "crud_collection", 0)) != NULL);
  RESULT_CHECK(stmt = mysqlx_table_select_new(table_coll));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_items(stmt, "doc->$.b_key AS msg", PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    char buf[256];
    size_t buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, buf, &buflen));
    printf("\n Row # %d: [ %s ]\n", row_num, buf);
    ++row_num;
  }
}


TEST_F(xapi, projections_doc)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  const char * json_string = NULL;
  int i = 0;
  size_t json_len = 0;
  char insert_buf[1024];

  const char *json_res[2][2] = {
    { "\"key2\": 6", "\"b_key\": \"bye world\"" },
    { "\"key2\": 8", "\"b_key\": \"hello again world\"" }
  };

  AUTHENTICATE();
  exec_sql("DROP DATABASE IF EXISTS cc_crud_test");
  exec_sql("CREATE DATABASE cc_crud_test");

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));

  for (i = 0; i < 5; i++)
  {
    sprintf(insert_buf, "INSERT INTO cc_crud_test.crud_collection (doc) VALUES " \
                        "('%s')", json_row[i]);
    exec_sql(insert_buf);
  }

  mysqlx_collection_t *collection;
  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "crud_collection", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_collection_find_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_criteria(stmt, "a_key > 1"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_projection(stmt, "{key2: a_key*2, b_key: b_key}"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, "key2", SORT_ORDER_ASC, PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_limit_and_offset(stmt, 2, 1));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  i = 0; // It is expected the rows will be returned starting from 2
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    EXPECT_TRUE(strstr(json_string, json_res[i][0]) != NULL);
    EXPECT_TRUE(strstr(json_string, json_res[i][1]) != NULL);
    ++i;
  }
}


TEST_F(xapi, add_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  const char * json_string = NULL;
  int i = 0, j = 0;
  size_t json_len = 0;
  char json_buf[1024];

  const char *json_add[2][4] = {
    { "a_key", "32768", "b_key", "Text value" },
    { "a_key", "32777", "b_key", "Another text value" }
  };

  AUTHENTICATE();

  for (i = 0; i < 2; ++i)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "crud_collection", 1)) != NULL);
  RESULT_CHECK(stmt = mysqlx_collection_add_new(collection));
  for (i = 0; i < 2; ++i)
  {
    sprintf(json_buf, "{\"%s\": \"%s\", \"%s\": \"%s\"}", json_add[i][0], json_add[i][1],
                                          json_add[i][2], json_add[i][3]);
    EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json_buf));
    printf("\nJSON FOR ADD %d [ %s ]", i + 1, json_buf);
  }
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_find_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, "a_key", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  i = 0;
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    for (j = 0; j < 4; ++j)
      EXPECT_TRUE(strstr(json_string, json_add[i][j]) != NULL);
    ++i;
  }
}


TEST_F(xapi, collection_param_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;

  const char * json_string = NULL;
  int i = 0;
  size_t json_len = 0;
  char json_buf[1024];

  const char *json_add[2][4] = {
    { "a_key", "32768", "b_key", "Text value" },
    { "a_key", "32777", "b_key", "Another text value" }
  };

  AUTHENTICATE();

  for (i = 0; i < 2; ++i)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "crud_collection", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_collection_add_new(collection));
  for (i = 0; i < 2; ++i)
  {
    sprintf(json_buf, "{\"%s\": \"%s\", \"%s\": \"%s\"}", json_add[i][0], json_add[i][1],
                                          json_add[i][2], json_add[i][3]);
    EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json_buf));
    printf("\nJSON FOR ADD %d [ %s ]", i + 1, json_buf);
  }
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_modify_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_criteria(stmt, "a_key = :numv"));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_set(stmt,
                       "b_key", PARAM_STRING("New text value"),
                       "a_key", PARAM_EXPR("a_key - 2*:numv2"),
                       PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_stmt_bind(stmt, "numv", PARAM_STRING("32768"),
                                              "numv2", PARAM_UINT(500),
                                              PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_find_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_criteria(stmt, "a_key = :numv"));
  EXPECT_EQ(RESULT_OK, mysqlx_stmt_bind(stmt, "numv", PARAM_UINT(31768), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, "a_key", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  i = 0;
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

   EXPECT_TRUE(strstr(json_string, "a_key") != NULL);
   EXPECT_TRUE(strstr(json_string, "31768") != NULL);
   EXPECT_TRUE(strstr(json_string, "b_key") != NULL);
   EXPECT_TRUE(strstr(json_string, "New text value") != NULL);
    ++i;
  }
}


TEST_F(xapi, update_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  int i = 0, row_num;

  int64_t v_sint[2] = { -17, 34 };
  uint64_t v_uint[2] = { 101, 23234 };
  float v_float[2] = { 3.31f, 12.27f };
  double v_double[2] = { 1.7E+3, 2.8E-100 };
  const char *v_str[2] = { "just some text", "more text" };
  const char *v_doc[2] = { "{ \"key\": 1, \"val\": \"one\" }",
                           "{ \"key\": 2, \"val\": \"twoo\" }" };

  const char *init_queries[] = {
    "DROP TABLE IF EXISTS cc_crud_test.crud_update_test",
    "CREATE TABLE cc_crud_test.crud_update_test ("
      "sint BIGINT,"
      "uint BIGINT UNSIGNED,"
      "flv FLOAT,"
      "dbv DOUBLE,"
      "strv VARCHAR(255),"
      "docv JSON"
    ")"
  };

  AUTHENTICATE();

  for (i = 0; i < 2; i++)
  {
    exec_sql(init_queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_update_test", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_insert_new(table));

  for (i = 0; i < 2; ++i)
  {
    EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt,
                                    PARAM_SINT(v_sint[i]),
                                    PARAM_UINT(v_uint[i]),
                                    PARAM_FLOAT(v_float[i]),
                                    PARAM_DOUBLE(v_double[i]),
                                    PARAM_STRING(v_str[i]),
                                    PARAM_STRING(v_doc[i]),
                                    PARAM_END));
  }
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_table_update_new(table));

  EXPECT_EQ(RESULT_OK, mysqlx_set_update_values(stmt,
                                    "sint", PARAM_SINT((int64_t)55),
                                    "uint", PARAM_EXPR("(uint*200)+5"),
                                    "flv",  PARAM_FLOAT((float)77),
                                    "dbv",  PARAM_EXPR("(:param1-dbv)*2"),
                                    "strv", PARAM_STRING("text 99"),
                                    "docv->$.key", PARAM_SINT(7),
                                    "docv->$.val", PARAM_STRING("foo"),
                                    PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_update_where(stmt, "uint < :param2"));
  EXPECT_EQ(RESULT_OK, mysqlx_stmt_bind(stmt, "param1", PARAM_UINT(88),
                                              "param2", PARAM_UINT(1000), PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_order_by(stmt, "uint", SORT_ORDER_ASC, PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_where(stmt, "docv->$.val like :paramstr"));
  EXPECT_EQ(RESULT_OK, mysqlx_stmt_bind(stmt, "paramstr", PARAM_STRING("%oo"), PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  /*
    Set the expected values to the same ones as in the
    mysqlx_set_update_values() call
    TODO: Checking result of updates inside docv.
  */
  v_sint[0] = 55;
  v_uint[0] = (v_uint[0]*200) + 5;
  v_double[0] = (double)(88 - v_double[0])*2;
  v_float[0] = (float)77;
  v_str[0] = "text 99";

  row_num = 0;
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t v_sint2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &v_sint2));

    EXPECT_EQ(v_sint[row_num], v_sint2);

    uint64_t v_uint2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_uint(row, 1, &v_uint2));
    EXPECT_EQ(v_uint[row_num], v_uint2);

    float v_float2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_float(row, 2, &v_float2));
    EXPECT_EQ(v_float[row_num], v_float2);

    double v_double2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_double(row, 3, &v_double2));
    EXPECT_EQ(v_double[row_num], v_double2);

    char v_str2[256];
    size_t buflen = sizeof(v_str2);

    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 4, 0, v_str2, &buflen));
    EXPECT_EQ(buflen, strlen(v_str[row_num]) + 1);
    EXPECT_STREQ(v_str2, v_str[row_num]);
    ++row_num;
  }
  EXPECT_EQ(row_num, 2);
}


TEST_F(xapi, modify_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;

  const char * json_string = NULL;
  int i = 0;
  size_t json_len = 0;
  char json_buf[1024];
  double new_double_val = 9.87654321E3;

  const char *json_add[2][6] = {
    { "a_key", "32768", "b_key", "Text value", "c_key", "[11, 22, 33]" },
    { "a_key", "32777", "b_key", "Another text value",  "c_key", "[77, 88, 99]"}
  };

  AUTHENTICATE();

  for (i = 0; i < 2; ++i)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "crud_collection", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_collection_add_new(collection));
  for (i = 0; i < 2; ++i)
  {
    sprintf(json_buf, "{\"%s\": %s, \"%s\": \"%s\", \"%s\": %s}",
                        json_add[i][0], json_add[i][1], json_add[i][2],
                        json_add[i][3], json_add[i][4], json_add[i][5]);
    EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json_buf));
    printf("\nJSON FOR ADD %d [ %s ]", i + 1, json_buf);
  }
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_modify_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_set(stmt,
                       "b_key", PARAM_STRING("New text value"),
                       "a_key", PARAM_EXPR("a_key-1000"),
                       "d_key", PARAM_DOUBLE(new_double_val), // This will be the new key-value in document
                       PARAM_END));

  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_array_insert(stmt,
                       "c_key[1]", PARAM_SINT(199),
                       "c_key[3]", PARAM_SINT(399),
                       PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_criteria(stmt, "a_key=32768"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_modify_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_unset(stmt, "b_key", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_array_delete(stmt, "c_key[1]", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_array_append(stmt, "c_key", PARAM_SINT(-100), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_criteria(stmt, "a_key=32777"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_find_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, "a_key", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  i = 0;
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    if (i == 0)
    {
      // Check setting value using expression
      EXPECT_TRUE(strstr(json_string, "31768") != NULL);
      // Check setting string value
      EXPECT_TRUE(strstr(json_string, "New text value") != NULL);
      EXPECT_TRUE(strstr(json_string, "c_key") != NULL);
      // Check array insert
      EXPECT_TRUE(strstr(json_string, "[11, 199, 22, 399, 33]") != NULL);
      // Check that the new value was created
      EXPECT_TRUE(strstr(json_string, "d_key") != NULL);
      EXPECT_TRUE(strstr(json_string, "9876.54321") != NULL);
    }
    else
    {
      // this value is not changed
      EXPECT_TRUE(strstr(json_string, "32777") != NULL);
      // this was unset
      EXPECT_TRUE(strstr(json_string, "b_key") == NULL);
      // this key is present
      EXPECT_TRUE(strstr(json_string, "c_key") != NULL);
      // the array has one element [1] deleted and one element appended
      EXPECT_TRUE(strstr(json_string, "[77, 99, -100]") != NULL);
    }
    ++i;
  }
}


TEST_F(xapi, remove_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;

  const char * json_string = NULL;
  int i = 0;
  size_t json_len = 0;
  char json_buf[1024];

  const char *json_add[5][2] = {
    { "my_key", "111" },
    { "my_key", "222" },
    { "my_key", "333" },
    { "my_key", "444" },
    { "my_key", "555" }
  };

  AUTHENTICATE();

  for (i = 0; i < 2; ++i)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "crud_collection", 1)) != NULL);
  RESULT_CHECK(stmt = mysqlx_collection_add_new(collection));
  for (i = 0; i < 5; ++i)
  {
    sprintf(json_buf, "{\"%s\": %s}", json_add[i][0], json_add[i][1]);
    EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json_buf));
    printf("\nJSON FOR ADD %d [ %s ]", i + 1, json_buf);
  }
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_remove_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_limit_and_offset(stmt, 2, 0));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_order_by(stmt, "my_key", SORT_ORDER_DESC, PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_where(stmt, "my_key > 111"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_collection_find_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, "my_key", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  i = 0;
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    switch(i)
    {
      case 0: EXPECT_TRUE(strstr(json_string, "111") != NULL); break;
      case 1: EXPECT_TRUE(strstr(json_string, "222") != NULL); break;
      case 2: EXPECT_TRUE(strstr(json_string, "333") != NULL); break;
      default: // There should be no more documents
        FAIL();
        continue;
    }
    ++i;
  }
}


TEST_F(xapi_bugs, myc_288_param_bytes)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int row_num = 0;
  int i = 0;

  int64_t v_sint[2] = { 100, 200 };
  const char *v_str[2] = { "just some text", "more text" };
  const char *init_queries[] = {
    "DROP TABLE IF EXISTS cc_crud_test.crud_myc_288",
    "CREATE TABLE cc_crud_test.crud_myc_288 (id int, strv VARCHAR(255))"
  };
  const char *insert_query = "INSERT INTO cc_crud_test.crud_myc_288 " \
                             "(id, strv) VALUES (? , ?)";
  AUTHENTICATE();

  for (i = 0; i < 2; i++)
  {
    printf("\nExecuting query:\n  %s ... ", init_queries[i]);
    exec_sql(init_queries[i]);
  }

  /* Test plain SQL */
  RESULT_CHECK(stmt = mysqlx_sql_new(get_session(), insert_query,
                                       strlen(insert_query)));
  EXPECT_EQ(0, mysqlx_stmt_bind(stmt, PARAM_SINT(v_sint[0]),
                                PARAM_BYTES(v_str[0], strlen(v_str[0])),
                                PARAM_END));
  RESULT_CHECK(res = mysqlx_execute(stmt));

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_myc_288", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_insert_new(table));

  /* Test with statement Insert */
  EXPECT_EQ(RESULT_OK, mysqlx_set_insert_row(stmt, PARAM_SINT(v_sint[1]),
                                  PARAM_BYTES(v_str[1], strlen(v_str[1])),
                                  PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_order_by(stmt, "id", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  row_num = 0;
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t v_sint2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &v_sint2));

    EXPECT_EQ(v_sint[row_num], v_sint2);

    char v_str2[256];
    size_t buflen = sizeof(v_str2);

    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, v_str2, &buflen));
    EXPECT_EQ(buflen, strlen(v_str[row_num]) + 1);
    EXPECT_STREQ(v_str2, v_str[row_num]);
    ++row_num;
  }
  EXPECT_EQ(row_num, 2);
}


TEST_F(xapi_bugs, myc_293_double_free)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int i = 0;

  AUTHENTICATE();

  // Skip drop/create database
  for (i = 0; i < 4; i++)
  {
    exec_sql(queries[i]);
  }
  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_basic", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_items(stmt, "id", "id*2", "800", "vctext", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_where(stmt, "id = 10"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {}

  mysqlx_free(res);
  mysqlx_free(stmt);
}


TEST_F(xapi_bugs, myc_338_update_null)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  int i = 0;
  int64_t intval = 0;
  char buf[30];
  size_t len = sizeof(buf);

  AUTHENTICATE();

  // Skip drop/create database
  for (i = 0; i < 4; i++)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_basic", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_update_new(table));

  EXPECT_EQ(RESULT_OK, mysqlx_set_update_values(stmt,
                                    "vctext", PARAM_NULL(), PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_update_where(stmt, "id = 30"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_where(stmt, "id = 30"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &intval));
    EXPECT_EQ(30, intval);
    EXPECT_EQ(RESULT_NULL, mysqlx_get_bytes(row, 1, 0, buf, &len));
  }

  mysqlx_free(res);
  mysqlx_free(stmt);
}


TEST_F(xapi_bugs, myc_297_col_types)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  int col_num = 0;
  int i = 0;

  const char *init_queries[] = {
    "DROP TABLE IF EXISTS cc_crud_test.crud_myc_297",
    "CREATE TABLE cc_crud_test.crud_myc_297(c1 BIGINT, c2 BIGINT UNSIGNED, " \
    "c3 INT, c4 INT UNSIGNED, c5 CHAR(100), c6 DOUBLE, c7 BINARY(100), " \
    "c8 FLOAT, c9 DOUBLE, c10 JSON, c11 BOOL, c12 DATETIME, c13 TIME, " \
    "c14 DECIMAL(10,5), c15 BIT(64), c16 ENUM('a', 'b', 'c')," \
    "c17 SET('a', 'b', 'c'), c18 GEOMETRY)"
  };
  AUTHENTICATE();

  for (i = 0; i < 2; i++)
  {
    printf("\nExecuting query:\n  %s ... ", init_queries[i]);
    exec_sql(init_queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_myc_297", 1)) != NULL);
  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  col_num = mysqlx_column_get_count(res);
  EXPECT_EQ(col_num, 18);

  // TODO: Uncomment when CDK supports GEOMETRY
  EXPECT_EQ(MYSQLX_TYPE_SINT, (mysqlx_data_type_t)mysqlx_column_get_type(res, 0));
  EXPECT_EQ(MYSQLX_TYPE_UINT, (mysqlx_data_type_t)mysqlx_column_get_type(res, 1));
  EXPECT_EQ(MYSQLX_TYPE_SINT, (mysqlx_data_type_t)mysqlx_column_get_type(res, 2));
  EXPECT_EQ(MYSQLX_TYPE_UINT, (mysqlx_data_type_t)mysqlx_column_get_type(res, 3));
  EXPECT_EQ(MYSQLX_TYPE_STRING, (mysqlx_data_type_t)mysqlx_column_get_type(res, 4));
  EXPECT_EQ(MYSQLX_TYPE_DOUBLE, (mysqlx_data_type_t)mysqlx_column_get_type(res, 5));
  EXPECT_EQ(MYSQLX_TYPE_BYTES, (mysqlx_data_type_t)mysqlx_column_get_type(res, 6));
  EXPECT_EQ(MYSQLX_TYPE_FLOAT, (mysqlx_data_type_t)mysqlx_column_get_type(res, 7));
  EXPECT_EQ(MYSQLX_TYPE_DOUBLE, (mysqlx_data_type_t)mysqlx_column_get_type(res, 8));
  EXPECT_EQ(MYSQLX_TYPE_JSON, (mysqlx_data_type_t)mysqlx_column_get_type(res, 9));
  EXPECT_EQ(MYSQLX_TYPE_BOOL, (mysqlx_data_type_t)mysqlx_column_get_type(res, 10));
  EXPECT_EQ(MYSQLX_TYPE_DATETIME, (mysqlx_data_type_t)mysqlx_column_get_type(res, 11));
  EXPECT_EQ(MYSQLX_TYPE_TIME, (mysqlx_data_type_t)mysqlx_column_get_type(res, 12));
  EXPECT_EQ(MYSQLX_TYPE_DECIMAL, (mysqlx_data_type_t)mysqlx_column_get_type(res, 13));
  EXPECT_EQ(MYSQLX_TYPE_BYTES, (mysqlx_data_type_t)mysqlx_column_get_type(res, 14));
  EXPECT_EQ(MYSQLX_TYPE_ENUM, (mysqlx_data_type_t)mysqlx_column_get_type(res, 15));
  EXPECT_EQ(MYSQLX_TYPE_SET, (mysqlx_data_type_t)mysqlx_column_get_type(res, 16));
  EXPECT_EQ(MYSQLX_TYPE_GEOMETRY, (mysqlx_data_type_t)mysqlx_column_get_type(res, 17));
}


TEST_F(xapi_bugs, update_collection_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  mysqlx_table_t *table;

  int i = 0;
  char json_buf[1024];
  size_t buflen;

  const char *json_add[4] ={ "my_key", "111", "my_key2", "\"abcde\"" };

  AUTHENTICATE();

  for (i = 0; i < 2; ++i)
  {
    exec_sql(queries[i]);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "crud_collection"));
  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "crud_collection", 1)) != NULL);
  RESULT_CHECK(stmt = mysqlx_collection_add_new(collection));

  sprintf(json_buf, "{\"%s\": %s, \"%s\": %s}",
    json_add[0], json_add[1], json_add[2], json_add[3]);
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, json_buf));
  printf("\nJSON FOR ADD [ %s ]", json_buf);
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  // Do not check if this is a table because it is a collection
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_collection", 0)) != NULL);

  RESULT_CHECK(stmt = mysqlx_table_update_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_update_values(stmt,
                                    "doc->$.my_key", PARAM_SINT(222),
                                    "doc->$.my_key2", PARAM_STRING("qwertyui"),
                                    PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  RESULT_CHECK(stmt = mysqlx_table_select_new(table));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_items(stmt, "doc->$.my_key as my_key", "doc->$.my_key2 as my_key2", PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  EXPECT_EQ(2, mysqlx_column_get_count(res));

  EXPECT_EQ(MYSQLX_TYPE_JSON, mysqlx_column_get_type(res, 0));
  EXPECT_EQ(MYSQLX_TYPE_JSON, mysqlx_column_get_type(res, 1));

  EXPECT_STREQ(json_add[0], mysqlx_column_get_name(res, 0));
  EXPECT_STREQ(json_add[2], mysqlx_column_get_name(res, 1));

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
      buflen = sizeof(json_buf);
      EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, json_buf, &buflen));
      EXPECT_STREQ("222", json_buf);
      EXPECT_EQ(4, buflen);

      buflen = sizeof(json_buf);
      EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, json_buf, &buflen));
      EXPECT_STREQ("\"qwertyui\"", json_buf);
      EXPECT_EQ(11, buflen);
  }
}


TEST_F(xapi_bugs, one_call_functions_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;

  int i = 0;
  size_t buflen;
  char buf[1024];
  const char *str_val[4] = { "sample text", "another sample", "foo", "bar" };
  uint64_t uval[4] = { 18, 88, 40, 99 };


  AUTHENTICATE();

  for (i = 0; i < 3; ++i)
  {
    // Drop/create database and table using one-shot mysqlx_sql()
    SESS_CHECK( res = mysqlx_sql(get_session(), queries[i], MYSQLX_NULL_TERMINATED));
  }

  // Send the wron query
  EXPECT_EQ(NULL, mysqlx_sql(get_session(), "wrong query", MYSQLX_NULL_TERMINATED));
  printf("\n Expected error: %s", mysqlx_error_message(mysqlx_error(get_session())));

  SESS_CHECK( res = mysqlx_sql_param(get_session(),
    "INSERT INTO cc_crud_test.crud_basic (id, vctext) VALUES (?, ?), (?, ?)",
    MYSQLX_NULL_TERMINATED,
    PARAM_UINT(uval[0]),
    PARAM_STRING(str_val[0]),
    PARAM_UINT(uval[1]),
    PARAM_STRING(str_val[1]),
    PARAM_END));

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "crud_basic", 1)) != NULL);

  SESS_CHECK( res = mysqlx_table_select(table,
                    "(id > 50) AND (vctext LIKE '%sample')"));

  while((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t val = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &val));
    EXPECT_EQ(uval[1], val);
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    EXPECT_STREQ(str_val[1], buf);
  }

  SESS_CHECK( res = mysqlx_table_select_limit(table,
                    NULL, 100, 0, "id", SORT_ORDER_DESC, PARAM_END));

  i = 0;
  int rows = 2;
  while((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t val = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &val));
    EXPECT_EQ(uval[rows-i-1], val);
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    EXPECT_STREQ(str_val[rows-i-1], buf);
    ++i;
  }

  SESS_CHECK( res = mysqlx_table_insert(table,
    "vctext", PARAM_STRING(str_val[2]),
    "id", PARAM_UINT(uval[2]), PARAM_END));

  SESS_CHECK( res = mysqlx_table_select(table, "id = 40"));

  while((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t val = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &val));
    EXPECT_EQ(uval[2], val);
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    EXPECT_STREQ(str_val[2], buf);
  }

  SESS_CHECK( res = mysqlx_table_update(table, "id = 40",
    "vctext", PARAM_STRING("aaa"),
    "id", PARAM_UINT(111), PARAM_END));

  SESS_CHECK( res = mysqlx_table_select(table, "id = 111"));

  while((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    int64_t val = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_sint(row, 0, &val));
    EXPECT_EQ(111, val);
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    EXPECT_STREQ("aaa", buf);
  }

  SESS_CHECK( res = mysqlx_table_delete(table, "id = 111"));

  SESS_CHECK( res = mysqlx_table_select(table, "id = 111"));
  EXPECT_EQ(NULL, mysqlx_row_fetch_one(res));
}


TEST_F(xapi_bugs, list_functions)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_row_t *row;
  mysqlx_schema_t *schema;
  char buf[256];
  size_t buflen, rownum = 0;
  int col_num = 0;
  int i = 0;

  AUTHENTICATE();

  for (i = 0; i < 2; i++)
  {
    exec_sql(queries[i]);
  }

  SESS_CHECK( res = mysqlx_get_schemas(get_session(), NULL));
  SESS_CHECK( res = mysqlx_get_schemas(get_session(), "cc_crud_te%"));
  col_num = mysqlx_column_get_count(res);
  EXPECT_EQ(col_num, 1);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, buf, &buflen));
    EXPECT_STREQ("cc_crud_test", buf);
  }

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "collection_1"));
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "collection_2"));
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "collection_3"));

  exec_sql("CREATE TABLE cc_crud_test.tab_1 (id int)");
  exec_sql("CREATE TABLE cc_crud_test.tab_2 (id int)");
  exec_sql("CREATE TABLE cc_crud_test.tab_3 (id int)");

  exec_sql("CREATE VIEW cc_crud_test.view_1 AS SELECT * FROM cc_crud_test.tab_1");
  exec_sql("CREATE VIEW cc_crud_test.view_2 AS SELECT * FROM cc_crud_test.tab_2");
  exec_sql("CREATE VIEW cc_crud_test.view_3 AS SELECT * FROM cc_crud_test.tab_3");

  // Get tables only
  SESS_CHECK( res = mysqlx_get_tables(schema, "%", 0));

  EXPECT_EQ(RESULT_OK, mysqlx_store_result(res, &rownum));

  col_num = mysqlx_column_get_count(res);
  EXPECT_EQ(col_num, 2);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, buf, &buflen));
    EXPECT_TRUE(strstr(buf, "tab_") != NULL);
    EXPECT_EQ(NULL, strstr(buf, "view_"));
    printf("\n [%s]", buf);
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    printf(" [%s]", buf);
  }

  // Get tables and views (NULL pattern is the same as "%")
  SESS_CHECK( res = mysqlx_get_tables(schema, NULL, 1));

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, buf, &buflen));
    EXPECT_TRUE(strstr(buf, "tab_") != NULL || strstr(buf, "view_") != NULL);
    printf("\n [%s]", buf);
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    printf(" [%s]", buf);
  }

  // Get collections
  SESS_CHECK( res = mysqlx_get_collections(schema, NULL));
  SESS_CHECK( res = mysqlx_get_collections(schema, "col%"));

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, buf, &buflen));
    EXPECT_TRUE(strstr(buf, "collection_") != NULL);
    printf("\n [%s]", buf);
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 1, 0, buf, &buflen));
    printf(" [%s]", buf);
  }
}


TEST_F(xapi_bugs, schemas_list_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_row_t *row;
  char buf[256];
  size_t buflen;
  int col_num = 0;
  int i = 0;

  AUTHENTICATE();
  for (i = 0; i < 2; i++)
  {
    exec_sql(queries[i]);
  }

  authenticate(NULL, NULL, NULL);
  if (!get_session())
    FAIL();

  SESS_CHECK(res = mysqlx_get_schemas(get_session(), "cc_crud_te%"));
  col_num = mysqlx_column_get_count(res);
  EXPECT_EQ(col_num, 1);

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    buflen = sizeof(buf);
    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, buf, &buflen));
    EXPECT_STREQ("cc_crud_test", buf);
  }
}

TEST_F(xapi_bugs, one_call_collection_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;

  const char * json_string = NULL;
  int i = 0, j = 0;
  size_t json_len = 0;
  char json_buf[2][1024] = { "", "" };
  const char *new_str_val = "New string value";

  const char *json_add[2][4] = {
    { "a_key", "327", "b_key", "Text value" },
    { "a_key", "320", "b_key", "Another text value" }
  };

  AUTHENTICATE();

  mysqlx_schema_drop(get_session(), "cc_crud_test");
  mysqlx_schema_create(get_session(), "cc_crud_test");

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "collection_exec"));

  for (i = 0; i < 2; ++i)
    sprintf(json_buf[i], "{\"%s\": %s, \"%s\": \"%s\"}", json_add[i][0],
            json_add[i][1], json_add[i][2], json_add[i][3]);

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "collection_exec", 1)) != NULL);

  CRUD_CHECK(
    res = mysqlx_collection_add(collection, json_buf[0], json_buf[1], PARAM_END),
    collection
  );

  CRUD_CHECK(
    res = mysqlx_collection_find(collection, "a_key = 327"),
    collection
  );

  i = 0;
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    for (j = 0; j < 4; ++j)
      EXPECT_TRUE(strstr(json_string, json_add[0][j]) != NULL);
    ++i;
  }

  EXPECT_EQ(1, i);

  CRUD_CHECK(
    res = mysqlx_collection_modify_set(
      collection, "a_key = 327",
      "c_key", PARAM_EXPR("a_key + 100"),
      "b_key", PARAM_STRING(new_str_val),
      PARAM_END
    ),
    collection
  );

  CRUD_CHECK(
    res = mysqlx_collection_modify_unset(
      collection, "a_key = 327", "a_key", PARAM_END
    ),
    collection
  );

  CRUD_CHECK(
    res = mysqlx_collection_find(collection, "c_key = 427"),
    collection
  );

  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    EXPECT_TRUE(strstr(json_string, "c_key") != NULL);
    EXPECT_TRUE(strstr(json_string, "427") != NULL);
    EXPECT_TRUE(strstr(json_string, new_str_val) != NULL);
    EXPECT_TRUE(strstr(json_string, "a_key") == NULL); // it is unset
    ++i;
  }

  // remove one document
  CRUD_CHECK(
    res = mysqlx_collection_remove(collection, "a_key = 320"),
    collection
  );

  CRUD_CHECK(
    res = mysqlx_collection_find(collection, "a_key = 320"),
    collection
  );
  EXPECT_EQ(NULL, mysqlx_json_fetch_one(res, &json_len));

  CRUD_CHECK(res = mysqlx_collection_remove(collection, "true"), collection);

  CRUD_CHECK(res = mysqlx_collection_find(collection, ""), collection);
  EXPECT_EQ(NULL, mysqlx_json_fetch_one(res, &json_len));

}


TEST_F(xapi_bugs, collection_null_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_stmt_t *stmt;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;

  const char * json_string = NULL;
  size_t json_len = 0;

  AUTHENTICATE();

  mysqlx_schema_create(get_session(), "cc_crud_test");

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "collection_null"));
  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "collection_null", 1)) != NULL);

  SESS_CHECK(res = mysqlx_collection_add(collection, "{\"a\" : \"abc\"}", PARAM_END));

  RESULT_CHECK(stmt = mysqlx_collection_modify_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_modify_set(stmt,
                       "a", PARAM_NULL(), PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  SESS_CHECK(res = mysqlx_collection_find(collection, NULL));

  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);
    EXPECT_TRUE(strstr(json_string, "null") != NULL); // it is unset
  }
}

TEST_F(xapi_bugs, collection_id_test)
{
  SKIP_IF_NO_XPLUGIN

    mysqlx_result_t *res;
  mysqlx_stmt_t *stmt;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;

  const char * json_string = NULL;
  size_t json_len = 0;
  int i = 0;
  const char *id;
  char id_buf[3][128];


  AUTHENTICATE();

  mysqlx_schema_create(get_session(), "cc_crud_test");

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "cc_crud_test", 1)) != NULL);
  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "collection_id"));
  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "collection_id", 1)) != NULL);

  RESULT_CHECK(stmt = mysqlx_collection_add_new(collection));

  // empty document
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{}"));

  // Normal document with auto-generated _id
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"a_key\" : 100}"));

  // Document with _id specified by user
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"a_key\" : 200, \"_id\" : \"111222333\"}"));

  // Document with invalid _id specified by user, expect error when the add
  // operation is executed, but not when document is appended to the operation
  EXPECT_EQ(RESULT_OK, mysqlx_set_add_document(stmt, "{\"a_key\" : 300, \"_id\" : \"000000000000000000000000000000000011122223333\"}"));
  EXPECT_EQ(NULL, res = mysqlx_execute(stmt));
  printf("\nExpected error: %s\n", mysqlx_error_message(stmt));

  while ((id = mysqlx_fetch_generated_id(res)) != NULL)
  {
    strcpy(id_buf[i], id);
    ++i;
  }

  RESULT_CHECK(stmt = mysqlx_collection_find_new(collection));
  EXPECT_EQ(RESULT_OK, mysqlx_set_find_order_by(stmt, "a_key", SORT_ORDER_ASC, PARAM_END));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);

  i = 0;
  while ((json_string = mysqlx_json_fetch_one(res, &json_len)) != NULL)
  {
    if (json_string)
      printf("\n[json: %s]", json_string);

    EXPECT_TRUE(strstr(json_string, id_buf[i]) != NULL);

    switch (i)
    {
    case 0: // just _id in the JSON
      EXPECT_TRUE(strstr(json_string, "a_key") == NULL);
      break;
    case 1: // { "a_key" : 100}
      EXPECT_TRUE(strstr(json_string, "\"a_key\": 100") != NULL);
      break;
    case 2: // { "a_key" : 200, "_id" : "111222333"}
      EXPECT_TRUE(strstr(json_string, "\"a_key\": 200") != NULL);
      break;
    default: // no more documents in the result
      FAIL();
    }

    ++i;
  }
}

TEST_F(xapi_bugs, myc_352_stored_proc_err)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  const char *schema_name = "cc_crud_test";
  const char *errmsg = NULL;

  AUTHENTICATE();

  mysqlx_schema_drop(get_session(), schema_name);
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(get_session(), schema_name));

  res = mysqlx_sql(get_session(),
    "CREATE PROCEDURE cc_crud_test.myc_352(d INT)" \
    " BEGIN" \
    "   select 1, 2, 3;" \
    " IF d = 0 THEN " \
    " BEGIN " \
    " select point(1, 0) / point(1, 2); " \
    " END; " \
    " END IF; " \
    " select 'abc', 1.0; " \
    " END", MYSQLX_NULL_TERMINATED);

  res = mysqlx_sql(get_session(), "CALL cc_crud_test.myc_352(0)",
    MYSQLX_NULL_TERMINATED);

  EXPECT_EQ(RESULT_OK, mysqlx_store_result(res, NULL));
  EXPECT_EQ(RESULT_ERROR, mysqlx_next_result(res));
  EXPECT_TRUE((errmsg = mysqlx_error_message(res)) != NULL);
  printf("\nExpected error: %s\n", errmsg);
}


TEST_F(xapi, more_data_test)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_row_t *row;
  char data_buf[1200];
  size_t buf_len = 1200;

  // A piece of data 3K
  const char *query = "SELECT BINARY REPEAT('z', 3000) as longdata";

  AUTHENTICATE();

  res = mysqlx_sql(get_session(), query, strlen(query));

  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    EXPECT_EQ(RESULT_MORE_DATA, mysqlx_get_bytes(row, 0, 0, data_buf, &buf_len));
    EXPECT_EQ(1200, buf_len);

    EXPECT_EQ(RESULT_MORE_DATA, mysqlx_get_bytes(row, 0, 1200, data_buf, &buf_len));
    EXPECT_EQ(1200, buf_len);

    EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 2400, data_buf, &buf_len));
    EXPECT_EQ(601, buf_len);
  }
}


TEST_F(xapi, test_decimal_type)
{
  SKIP_IF_NO_XPLUGIN

    mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_table_t *table;
  mysqlx_row_t *row;
  int row_num = 1;

  AUTHENTICATE();

  mysqlx_schema_drop(get_session(), "xapi_dec_test");
  EXPECT_EQ(RESULT_OK, mysqlx_schema_create(get_session(), "xapi_dec_test"));
  res = mysqlx_sql(get_session(), "CREATE TABLE xapi_dec_test.dec_test" \
                   "(id int primary key, dcol DECIMAL(30, 10), dcol2 DECIMAL(65, 1))",
                   MYSQLX_NULL_TERMINATED);
  EXPECT_TRUE(res != NULL);
  res = mysqlx_sql(get_session(), "INSERT INTO xapi_dec_test.dec_test" \
                   "(id, dcol, dcol2) VALUES (1, -786.9876543219, 0),"\
                   "(2, 10.000001234, 0),"\
                   "(3, 999999999999999.5555, 0),"\
                   "(4, -1.1, 0),"\
                   "(5, 0, 9876543210987654321000000000000000000000000000000000000000000000.1)",
                   MYSQLX_NULL_TERMINATED);
  EXPECT_TRUE(res != NULL);
  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "xapi_dec_test", 1)) != NULL);
  EXPECT_TRUE((table = mysqlx_get_table(schema, "dec_test", 1)) != NULL);

  res = mysqlx_table_select(table, NULL);
  EXPECT_TRUE(res != NULL);
  while ((row = mysqlx_row_fetch_one(res)) != NULL)
  {
    float f = 0, f2 = 0;
    double d = 0, d2 = 0;
    EXPECT_EQ(RESULT_OK, mysqlx_get_float(row, 1, &f));
    EXPECT_EQ(RESULT_OK, mysqlx_get_double(row, 1, &d));

    if (row_num < 5)
      EXPECT_EQ(RESULT_OK, mysqlx_get_float(row, 2, &f2));
    else
      EXPECT_EQ(RESULT_ERROR, mysqlx_get_float(row, 2, &f2));

    EXPECT_EQ(RESULT_OK, mysqlx_get_double(row, 2, &d2));
    switch (row_num)
    {
    case 1:
      EXPECT_TRUE(f == -786.9876543219F);
      EXPECT_TRUE(d > -786.987654322L && d < -786.987654321L);
      break;
    case 2:
      EXPECT_TRUE(f == 10.000001234F);
      EXPECT_TRUE(d > 10.000001230L && d < 10.000001240L);
      break;
    case 3:
      EXPECT_TRUE(f == 999999999999999.5F);
      EXPECT_TRUE(d > 999999999999999.4L && d < 999999999999999.6L);
      break;
    case 4:
      EXPECT_TRUE(f == -1.1F);
      EXPECT_TRUE(d > -1.11L && d < -1.09);
      break;
    case 5:
      // Work around non-exact values
      EXPECT_TRUE(d2 > 9.87654321098765E+64 && d2 < 9.87654321098766E+64);
      break;
    default:
      FAIL();
    }
    ++row_num;
  }

  mysqlx_schema_drop(get_session(), "xapi_dec_test");
}


TEST_F(xapi, expr_in_expr)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  mysqlx_table_t *table;
  mysqlx_stmt_t *stmt;
  mysqlx_row_t* row;
  const char* json_string;
  size_t json_length;
  char buf[256];
  size_t buflen;

  AUTHENTICATE();

  SKIP_IF_SERVER_VERSION_LESS(8,0,2);

  mysqlx_schema_drop(get_session(), "expr_in_expt");

  mysqlx_schema_create(get_session(), "expr_in_expt");

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "expr_in_expt", 1)) != NULL);

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "c1"));

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "c1", 1)) != NULL);

  const char *foo   = "{ \"name\": \"foo\", \"age\": 1 }";
  const char *baz   = "{ \"name\": \"baz\", \"age\": 3, \"birth\": { \"day\": 20, \"month\": \"Apr\" } }";
  const char *bar   = "{ \"name\": \"bar\", \"age\": 2, \"food\": [\"Milk\", \"Soup\"] }";
  const char *foo_7 = "{ \"_id\": \"myuuid-1\", \"name\": \"foo\", \"age\": 7 }";
  const char *buz   = "{ \"name\": \"buz\", \"age\": 17 }";

  SESS_CHECK(res = mysqlx_collection_add(collection,foo, baz, bar, foo_7, buz, PARAM_END));

  SESS_CHECK(res = mysqlx_collection_find(collection,"{\"name\":\"baz\"} in $"));
  json_string = mysqlx_json_fetch_one(res, &json_length);
  EXPECT_TRUE(strstr(json_string, "\"name\": \"baz\"") != NULL);
  EXPECT_EQ(NULL, row = mysqlx_row_fetch_one(res));

  SESS_CHECK(res = mysqlx_collection_find(collection,"'bar' in $.name"));
  json_string = mysqlx_json_fetch_one(res, &json_length);
  EXPECT_TRUE(strstr(json_string, "\"name\": \"bar\"") != NULL);
  EXPECT_EQ(NULL, row = mysqlx_row_fetch_one(res));

  SESS_CHECK(res = mysqlx_collection_find(collection,"{ \"day\": 20, \"month\": \"Apr\" } in $.birth"));
  json_string = mysqlx_json_fetch_one(res, &json_length);
  EXPECT_TRUE(strstr(json_string, "\"name\": \"baz\"") != NULL);
  EXPECT_EQ(NULL, row = mysqlx_row_fetch_one(res));

  SESS_CHECK(res = mysqlx_collection_find(collection,"JSON_TYPE($.food) = 'ARRAY' AND 'Milk' IN $.food "));
  json_string = mysqlx_json_fetch_one(res, &json_length);
  EXPECT_TRUE(strstr(json_string, "\"name\": \"bar\"") != NULL);
  EXPECT_EQ(NULL, row = mysqlx_row_fetch_one(res));

  //using tables
  EXPECT_TRUE((table = mysqlx_get_table(schema, "c1", false)) != NULL);
  stmt = mysqlx_table_select_new(table);
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_items(stmt, "JSON_EXTRACT(doc,'$.name') as name", PARAM_END));
  EXPECT_EQ(RESULT_OK, mysqlx_set_select_where(stmt, "{\"name\":\"baz\"} in doc->$"));
  CRUD_CHECK(res = mysqlx_execute(stmt), stmt);
  EXPECT_TRUE((row = mysqlx_row_fetch_one(res)) != NULL);
  buflen = sizeof(buf);
  EXPECT_EQ(RESULT_OK, mysqlx_get_bytes(row, 0, 0, buf, &buflen));
  EXPECT_EQ(string(buf), string("\"baz\""));
  EXPECT_EQ(NULL, row = mysqlx_row_fetch_one(res));

}

TEST_F(xapi, schema_validation)
{
  SKIP_IF_NO_XPLUGIN;

  mysqlx_schema_t *schema;
  mysqlx_collection_options_t *opts;
  mysqlx_collection_t *coll;
  mysqlx_stmt_t *stmt;
  mysqlx_result_t *res;

  AUTHENTICATE();

  SKIP_IF_SERVER_VERSION_LESS(8, 0, 20);

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session() , "test", 1)) != nullptr);

  EXPECT_EQ(RESULT_OK,
  mysqlx_collection_drop(schema, "places"));

  EXPECT_TRUE((opts = mysqlx_collection_options_new()) != nullptr);

  const char *validation_schema =
  R"(
  {
    "id": "http://json-schema.org/geo",
    "$schema": "http://json-schema.org/draft-06/schema#",
    "description": "A geographical coordinate",
    "type": "object",
    "properties": {
      "latitude": {
        "type": "number"
      },
      "longitude": {
        "type": "number"
    }
  },
  "required": ["latitude", "longitude"]
  })";

  EXPECT_EQ(RESULT_ERROR,
  mysqlx_collection_options_set(opts,
                                OPT_COLLECTION_REUSE(false),
                                OPT_COLLECTION_VALIDATION_LEVEL(VALIDATION_STRICT),
                                OPT_COLLECTION_VALIDATION_SCHEMA(validation_schema),
                                100,3,
                                PARAM_END));

  std::cout << "EXPECTED: " << mysqlx_error_message(opts) << std::endl;

  EXPECT_EQ(RESULT_OK,
  mysqlx_collection_options_set(opts,
                                OPT_COLLECTION_REUSE(false),
                                OPT_COLLECTION_VALIDATION_LEVEL(VALIDATION_STRICT),
                                OPT_COLLECTION_VALIDATION_SCHEMA(validation_schema),
                                PARAM_END));

  EXPECT_EQ(RESULT_ERROR,
  mysqlx_collection_options_set(opts,
                                OPT_COLLECTION_VALIDATION_LEVEL(VALIDATION_STRICT),
                                OPT_COLLECTION_VALIDATION_SCHEMA(validation_schema),
                                PARAM_END));

  EXPECT_EQ(RESULT_OK,
            mysqlx_collection_create_with_options(schema, "places", opts));

  EXPECT_EQ(RESULT_ERROR,
            mysqlx_collection_create_with_options(schema, "places", opts));

  //With reuseExisting=true will work
  mysqlx_collection_create_with_json_options(schema, "places",
                                             R"({
                                             "reuseExisting": true,
                                             "validation": {
                                             "level": "Strict",
                                             "schema":
                                             {
                                             "id": "http://json-schema.org/geo",
                                             "$schema": "http://json-schema.org/draft-06/schema#",
                                             "description": "A geographical coordinate",
                                             "type": "object",
                                             "properties": {
                                             "latitude": {
                                             "type": "number"
                                             },
                                             "longitude": {
                                             "type": "number"
                                             }
                                             },
                                             "required": ["latitude", "longitude"]
                                             }
                                             }
                                             })");

  std::cout << "EXPECTED: " << mysqlx_error_message(schema) << std::endl;

  EXPECT_TRUE(
  (coll = mysqlx_get_collection(schema, "places",1)) != nullptr);

  EXPECT_TRUE(
  (stmt = mysqlx_collection_add_new(coll)) != nullptr);

  EXPECT_EQ(RESULT_OK,
  mysqlx_set_add_document(stmt, R"({"location":"Lisbon", "latitude":38.722321, "longitude": -9.139336})"));

  CRUD_CHECK((res = mysqlx_execute(stmt)), stmt);

  EXPECT_EQ(RESULT_OK,
  mysqlx_set_add_document(stmt, R"({"location":"Lisbon"})"));

  //Expected error
  EXPECT_TRUE(
   (res = mysqlx_execute(stmt)) == nullptr);

  std::cout << "EXPECTED: " << mysqlx_error_message(stmt) << std::endl;

  mysqlx_free(opts);

  opts = mysqlx_collection_options_new();

  EXPECT_EQ(RESULT_OK,
  mysqlx_collection_options_set(opts,
                                OPT_COLLECTION_VALIDATION(
                                  "{"
                                    "\"level\": \"Off\","
                                    "\"schema\":"
                                    "{"
                                      "\"id\": \"http://json-schema.org/geo\","
                                      "\"$schema\": \"http://json-schema.org/draft-06/schema#\","
                                      "\"description\": \"A geographical coordinate\","
                                      "\"type\": \"object\","
                                      "\"properties\": {"
                                        "\"latitude\": {"
                                          "\"type\": \"number\""
                                        "},"
                                        "\"longitude\": {"
                                           "\"type\": \"number\""
                                         "}"
                                       "},"
                                       "\"required\": [\"latitude\", \"longitude\"]"
                                    "}"
                                  "}"
                                  ),
                                PARAM_END));

  EXPECT_EQ(RESULT_OK,
            mysqlx_collection_modify_with_options(schema, "places", opts));

  EXPECT_TRUE(
  (stmt = mysqlx_collection_add_new(coll)) != nullptr);

  EXPECT_EQ(RESULT_OK,
  mysqlx_set_add_document(stmt, R"({"location":"Lisbon"})"));

  CRUD_CHECK((res = mysqlx_execute(stmt)), stmt);

  mysqlx_free(opts);

}



TEST_F(xapi_bugs, session_invalid_password_deadlock)
{
  SKIP_IF_NO_XPLUGIN

  auto sess = mysqlx_get_session(m_xplugin_host,
                              m_port,
                              m_xplugin_usr,
                              "bal_xplugin_pwd",
                              NULL,
                              NULL);

  EXPECT_EQ(NULL, sess);
}

TEST_F(xapi_bugs, crash_empty_reply)
{
  SKIP_IF_NO_XPLUGIN

  mysqlx_result_t *res;
  mysqlx_schema_t *schema;
  mysqlx_collection_t *collection;
  mysqlx_stmt_t *stmt;

  AUTHENTICATE();

  SKIP_IF_SERVER_VERSION_LESS(8,0,2);

  mysqlx_schema_drop(get_session(), "crash_empty_reply");

  mysqlx_schema_create(get_session(), "crash_empty_reply");

  EXPECT_TRUE((schema = mysqlx_get_schema(get_session(), "crash_empty_reply", 1)) != NULL);

  EXPECT_EQ(RESULT_OK, mysqlx_collection_create(schema, "c1"));

  EXPECT_TRUE((collection = mysqlx_get_collection(schema, "c1", 1)) != NULL);

  stmt = mysqlx_collection_find_new(collection);
  res = mysqlx_execute(stmt);

  stmt = mysqlx_collection_modify_new(collection);
  res = mysqlx_execute(stmt);
}

