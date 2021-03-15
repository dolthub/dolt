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
#include <list>

using std::cout;
using std::endl;
using namespace mysqlx;


class Ddl : public mysqlx::test::Xplugin
{};


TEST_F(Ddl, create_drop)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.ddl..." << endl;

  // Cleanup

  const string schema_name_1 = "schema_to_drop_1";
  const string schema_name_2 = "schema_to_drop_2";

  try {
    get_sess().dropSchema(schema_name_1);
  } catch (...) {};
  try {
    get_sess().dropSchema(schema_name_2);
  } catch (...) {};


  // Create 2 schemas

  get_sess().createSchema(schema_name_1);
  get_sess().createSchema(schema_name_2);


  // Catch error because schema is already created

  EXPECT_THROW(get_sess().createSchema(schema_name_1),mysqlx::Error);

  // Reuse created schema

  Schema schema = get_sess().createSchema(schema_name_1, true);

  //Tables Test

  {
    sql("USE schema_to_drop_1");
    sql("CREATE TABLE tb1 (`name` varchar(20), `age` int)");
    sql("CREATE TABLE tb2 (`name` varchar(20), `age` int)");
    sql("CREATE VIEW  view1 AS SELECT `name`, `age` FROM tb1");

    std::list<Table> tables_list = schema.getTables();

    EXPECT_EQ(3, tables_list.size());

    for (auto tb : tables_list)
    {
      if (tb.getName().find(u"view") != std::string::npos)
      {
        EXPECT_TRUE(tb.isView());

        //check using getTable() passing check_existence = true
        EXPECT_TRUE(schema.getTable(tb.getName(), true).isView());

        //check using getTable() on isView()
        EXPECT_TRUE(schema.getTable(tb.getName()).isView());

      }
    }
  }

  //Collection tests

  {

    const string collection_name_1 = "collection_1";
    const string collection_name_2 = "collection_2";
    // Create Collections
    schema.createCollection(collection_name_1);

    schema.createCollection(collection_name_2);

    // Get Collections

    std::list<Collection> list_coll = schema.getCollections();

    EXPECT_EQ(2, list_coll.size());

    for (Collection col : list_coll)
    {
      col.add("{\"name\": \"New Guy!\"}").execute();
    }

    // Drop Collections

    EXPECT_NO_THROW(schema.getCollection(collection_name_1, true));

    std::list<string> list_coll_name = schema.getCollectionNames();
    for (auto name : list_coll_name)
    {
      schema.dropCollection(name);
    }

    //Doesn't throw even if don't exist
    for (auto name : list_coll_name)
    {
      schema.dropCollection(name);
    }

    //Test Drop Collection
    EXPECT_THROW(schema.getCollection(collection_name_1, true), mysqlx::Error);
    EXPECT_THROW(schema.getCollection(collection_name_2, true), mysqlx::Error);
  }


  // Get Schemas

  std::list<Schema> schemas = get_sess().getSchemas();

  // Drop Schemas

  for (auto schema_ : schemas)
  {
    if (schema_.getName() == schema_name_1 ||
        schema_.getName() == schema_name_2)
      get_sess().dropSchema(schema_.getName());
  }

  // Drop Schemas doesn't throw if it doesnt exist
  for (auto schema_ : schemas)
  {
    if (schema_.getName() == schema_name_1 ||
        schema_.getName() == schema_name_2)
      EXPECT_NO_THROW(get_sess().dropSchema(schema_.getName()));
  }

  EXPECT_THROW(get_sess().getSchema(schema_name_1, true), mysqlx::Error);
  EXPECT_THROW(get_sess().getSchema(schema_name_2, true), mysqlx::Error);


  cout << "Done!" << endl;
}


TEST_F(Ddl, create_index)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Creating collection..." << endl;

  Schema sch = getSchema("test");
  Collection coll = sch.createCollection("c1", true);
  coll.remove("true").execute();

  cout << "Inserting documents..." << endl;

  {
    Result add;

    add = coll
      .add(R"({ "zip": "34239", "zcount": "10", "some_text": "just some text" })")
      .add(R"({ "zip": "30001", "zcount": "20", "some_text": "some more text" })")
      .execute();
    output_id_list(add);
    EXPECT_EQ(2U, add.getAffectedItemsCount());

  }

  /* Create a multi value index */
  cout << "Plain index..." << endl;

  coll.createIndex("custom_idx1", R"-({
    "Fields": [
      { "field": "$.zip", "required" : true , "TyPe" : "TEXT(10)" },
      { "FIELD": "$.zcount", "type" : "INT unsigned" }
    ]
  })-");

  coll.dropIndex("custom_idx1");
  coll.remove("true").execute();

  /**
    First we create a spatial index, then we insert the document.
    Otherwise the server-side reports error:

    "Collection contains document missing required field"
    Looks like it is an issue in xplugin.

    Also, the server 5.7 doesn't seem to handle spatial indexes
  */
  SKIP_IF_SERVER_VERSION_LESS(8, 0, 4);

  cout << "Spatial index..." << endl;

  coll.createIndex("geo_idx1", R"-({
    "type" : "SPATIAL",
    "fields": [{
      "field": "$.coords",
      "type" : "GEOJSON",
      "required" : true,
      "options": 2,
      "srid": 4326
    }]
  })-");

  {
    Result add;

    add = coll.add(R"({
      "zip": "34239",
      "coords": { "type": "Point", "coordinates": [102.0, 0.0] }
    })")
    .execute();

    output_id_list(add);
    EXPECT_EQ(1U, add.getAffectedItemsCount());

  }

#if 0

  // Spatial index with implicit "required" attribute.
  // TODO: FIXME

  coll.remove("true").execute();

  EXPECT_NO_THROW(coll.createIndex("geo_idx1", R"-({
    "type" : "SPATIAL",
    "fields": [{
      "field": "$.coords",
      "type" : "GEOJSON",
    }]
  })-"));

#endif

  cout << "Drop non exsiting index..." << endl;

  EXPECT_NO_THROW(coll.dropIndex("non exsiting"));

  cout << "Negative tests" << endl;

  cout << "- index already exists" << endl;

  EXPECT_ERR(coll.createIndex("geo_idx1",
    R"({ "fields": [{ "field": "$.zcount", "type": "int" }] })"));

  coll.dropIndex("geo_idx1");

  cout << "- empty index name" << endl;

  EXPECT_ERR(coll.dropIndex(""));
  EXPECT_ERR(coll.createIndex("",
    R"({ "fields": [{ "field": "$.zcount", "type": "int" }] })"));

  cout << "- no index fields" << endl;

  EXPECT_ERR(coll.createIndex("bad_idx", R"({ "type": "INDEX" })"));
  EXPECT_ERR(coll.createIndex("bad_idx", R"({ })"));
  EXPECT_ERR(coll.createIndex("bad_idx", R"({ "fields": [] })"));

  cout << "- invalid index definition" << endl;

  EXPECT_ERR(coll.createIndex("bad_idx", "{ this is not valid )"));
  EXPECT_ERR(coll.createIndex("bad_idx", R"({ "foo": 123 })"));
  EXPECT_ERR(coll.createIndex("bad_idx",
    R"({ "fields": [{ "field": "$.zcount", "type": "int" }], "foo": 7 })"));
  EXPECT_ERR(coll.createIndex("bad_idx",
    R"({ "fields": [{ "field": "$.zcount", "type": "int", "foo": 7 }] })"));
  EXPECT_ERR(coll.createIndex("bad_idx",
    R"({ "fields": { "field": "$.zcount", "type": "int" } })"));

  cout << "- bad index type" << endl;

  EXPECT_ERR(coll.createIndex("bad_idx",
   R"({ "type": "foo", "fields": [{ "field": "$.zcount", "type": "int" }] })"));

  cout << "- bad index field type" << endl;

  EXPECT_ERR(coll.createIndex("bad_idx",
    R"({ "fields": [{ "field": "$.zcount", "type": "foo" }] })"));

  cout << "- options for non-spatial index" << endl;

  EXPECT_ERR(coll.createIndex("bad_idx",
    R"({ "fields": [{ "field": "$.zcount", "type": "int", "options": 123 }] })"));

  cout << "- bad spatial index" << endl;

  EXPECT_ERR(coll.createIndex("geo_idx2",
    R"({ "type" : "SPATIAL", "fields": [{ "field": "$.coords", "type" : "GEOJSON", "required" : false }] })")
             );

  SKIP_IF_SERVER_VERSION_LESS(8, 0, 17);

  cout << "Array index" << endl;
  coll.createIndex("arr_idx",
    R"-({ "fields": [{ "field": "$.zip", "type" : "CHAR(5)", "array" : true }] })-"
  );
  coll.dropIndex("arr_idx");

  cout << "Done!" << endl;
}

TEST_F(Ddl, schema_validation)
{
  SKIP_IF_NO_XPLUGIN;

  SKIP_IF_SERVER_VERSION_LESS(8, 0, 20);

  auto schema = get_sess().createSchema("test", true);

  schema.dropCollection("places");

  CollectionOptions opts_reuse_strict(
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

  CollectionOptions opts_strict(
        R"({
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

  CollectionValidation validation_off(
          CollectionValidation::LEVEL,
          CollectionValidation::OFF,
          CollectionValidation::SCHEMA,
          R"(
          {
            "id": "http://json-schema.org/geo",
            "$schema": "http://json-schema.org/draft-06/schema#",
            "description": "A geographical coordinate",
            "type": "object",
            "properties":
            {
              "latitude":
              {
                "type": "number"
              },
              "longitude":
              {
                "type": "number"
              }
            },
            "required": ["latitude", "longitude"]
          })"
          );

  auto places = schema.createCollection(
                  "places",
                  opts_reuse_strict
                  );

  places.add(R"({"location":"Almeirim", "latitude":39.2092349, "longitude": -8.6290121})").execute();
  EXPECT_THROW(places.add(R"({"location":"Santarem"})").execute(), Error);

  //Has Reuse... throw error
  EXPECT_THROW(
  schema.modifyCollection("places",
                          opts_reuse_strict
                          ),
        Error);


  schema.modifyCollection("places",
                          CollectionValidation(
                          CollectionValidation::LEVEL, CollectionValidation::OFF,
                          CollectionValidation::SCHEMA,
                          R"(
                          {
                            "id": "http://json-schema.org/geo",
                            "$schema": "http://json-schema.org/draft-06/schema#",
                            "description": "A geographical coordinate",
                            "type": "object",
                            "properties":
                            {
                              "latitude":
                              {
                                "type": "number"
                              },
                              "longitude":
                              {
                                "type": "number"
                              }
                            },
                            "required": ["latitude", "longitude"]
                          })")
                          );

  auto res = places.add(R"({"location":"Santarem"})").execute();


  EXPECT_THROW(
  opts_reuse_strict.set(CollectionValidation::LEVEL, CollectionValidation::STRICT), Error);

  EXPECT_THROW(
  opts_reuse_strict.set(CollectionValidation::SCHEMA, "{}"), Error);

  EXPECT_THROW(
  opts_reuse_strict.set(CollectionOptions::REUSE, true), Error);

  EXPECT_THROW(
  opts_reuse_strict.set(CollectionOptions::VALIDATION, validation_off), Error);


  EXPECT_THROW(
  schema.modifyCollection("places",
                          opts_strict
                          ), Error);

  //Removing the document that doesn't respect the validation, it should now work

  places.remove(R"({"_ix"::id})").bind("id", *res.getGeneratedIds().begin()).execute();

  schema.modifyCollection("places",
                          opts_strict
                          );

  EXPECT_THROW(
        places.add(R"({"location":"Lisbon"})").execute(),
        Error);

  places.add(R"({"location":"Lisbon", "latitude":38.722321, "longitude": -9.139336})")
      .execute();



  {
    auto places2 = schema.createCollection(
                     "places2",
                     CollectionValidation::LEVEL, CollectionValidation::STRICT,
                     CollectionOptions::REUSE, true,
                     CollectionValidation::SCHEMA,
                     R"(
                     {
                     "id": "http://json-schema.org/geo",
                     "$schema": "http://json-schema.org/draft-06/schema#",
                     "description": "A geographical coordinate",
                     "type": "object",
                     "properties":
                     {
                     "latitude":
                     {
                     "type": "number"
                     },
                     "longitude":
                     {
                     "type": "number"
                     }
                     },
                     "required": ["latitude", "longitude"]
                     })"
                     );

    places2.add(R"({"location":"Lisbon", "latitude":38.722321, "longitude": -9.139336})")
        .execute();

    EXPECT_THROW(
          places2.add(R"({"location":"Lisbon"})").execute(),
          Error);
  }

  //Check REUSE

  EXPECT_THROW(
        places = schema.createCollection(
                   "places",
                   opts_strict
                   ),
        Error);

  places = schema.createCollection(
             "places",
             opts_reuse_strict
             );

  schema.modifyCollection("places", opts_strict);

  EXPECT_THROW(
        places.add(R"({"location":"Lisbon"})").execute(),
        Error);

  schema.modifyCollection(
        "places",
        CollectionOptions::VALIDATION, validation_off);

  places.add(R"({"location":"Lisbon"})").execute();
}

TEST_F(Ddl, bugs)
{

  SKIP_IF_NO_XPLUGIN;

  {
    /*
      Having Result before dropView() triggered error, because cursor was closed
      without informing Result, so that Result could cache and then close Cursor
      and Reply
    */

    get_sess().dropSchema("bugs");
    get_sess().createSchema("bugs", false);

    Schema schema = get_sess().getSchema("bugs");
    sql(
          "CREATE TABLE bugs.bugs_table("
          "c0 JSON,"
          "c1 INT"
          ")");

    Table tbl = schema.getTable("bugs_table");
    Collection coll = schema.createCollection("coll");

    // TODO

    //schema.createView("newView").algorithm(Algorithm::TEMPTABLE)
    //    .security(SQLSecurity ::INVOKER).definedAs(tbl.select()).execute();

    RowResult result = get_sess().sql("show create table bugs.bugs_table").execute();
    Row r = result.fetchOne();

    schema.dropCollection("coll");
    //schema.dropView("newView");

    //schema.createView("newView").algorithm(Algorithm::TEMPTABLE)
    //    .security(SQLSecurity ::INVOKER).definedAs(tbl.select()).execute();

    r = result.fetchOne();
  }
}

