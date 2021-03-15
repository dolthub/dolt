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

#include <iostream>
#include <array>
#include <cmath>  // for fabs()
#include <vector>

#include <test.h>

using std::cout;
using std::wcout;
using std::endl;
using std::array;
using std::fabs;
using std::vector;
using namespace mysqlx;


class Types : public mysqlx::test::Xplugin
{
};


TEST_F(Types, numeric)
{
  {
    Value val = 7U;
    EXPECT_EQ(Value::UINT64, val.getType());

    int v0;
    EXPECT_NO_THROW(v0 = val);
    EXPECT_EQ(7, v0);

    unsigned v1;
    EXPECT_NO_THROW(v1 = val);
    EXPECT_EQ(7, v1);

    float v2;
    EXPECT_NO_THROW(v2 = val);
    EXPECT_EQ(7, v2);

    double v3;
    EXPECT_NO_THROW(v3 = val);
    EXPECT_EQ(7, v3);

    bool v4;
    EXPECT_NO_THROW(v4 = (bool)val);
    EXPECT_TRUE(v4);
  }

  {
    Value val = -7;
    EXPECT_EQ(Value::INT64, val.getType());

    int v0;
    EXPECT_NO_THROW(v0 = val);
    EXPECT_EQ(-7, v0);

    unsigned v1;
    EXPECT_THROW(v1 = val, Error);

    float v2;
    EXPECT_NO_THROW(v2 = val);
    EXPECT_EQ(-7, v2);

    double v3;
    EXPECT_NO_THROW(v3 = val);
    EXPECT_EQ(-7, v3);

    bool v4;
    EXPECT_NO_THROW(v4 = (bool)val);
    EXPECT_TRUE(v4);
  }

  {
    unsigned max_uint = std::numeric_limits<unsigned>::max();
    Value val = max_uint;
    EXPECT_EQ(Value::UINT64, val.getType());

    int v0;
    EXPECT_THROW(v0 = val, Error);

    unsigned v1;
    EXPECT_NO_THROW(v1 = val);
    EXPECT_EQ(max_uint, v1);

    float v2;
    EXPECT_NO_THROW(v2 = val);
    // Note: allow small rounding errors
    EXPECT_LE(fabs(v2/max_uint - 1), 1e-7);

    double v3;
    EXPECT_NO_THROW(v3 = val);
    EXPECT_EQ(max_uint, v3);

    bool v4;
    EXPECT_NO_THROW(v4 = (bool)val);
    EXPECT_TRUE(v4);
  }

  {
    Value val = 7.0F;
    EXPECT_EQ(Value::FLOAT, val.getType());

    int v0;
    EXPECT_THROW(v0 = val, Error);

    unsigned v1;
    EXPECT_THROW(v1 = val, Error);

    float v2;
    EXPECT_NO_THROW(v2 = val);
    EXPECT_EQ(7.0, v2);

    double v3;
    EXPECT_NO_THROW(v3 = val);
    EXPECT_EQ(7.0, v3);

    bool v4;
    EXPECT_THROW(v4 = (bool)val, Error);
  }

  {
    Value val = 7.0;
    EXPECT_EQ(Value::DOUBLE, val.getType());

    int v0;
    EXPECT_THROW(v0 = val, Error);

    unsigned v1;
    EXPECT_THROW(v1 = val, Error);

    float v2;
    EXPECT_THROW(v2 = val, Error);

    double v3;
    EXPECT_NO_THROW(v3 = val);
    EXPECT_EQ(7.0, v3);

    bool v4;
    EXPECT_THROW(v4 = (bool)val, Error);
  }

  {
    Value val = true;
    EXPECT_EQ(Value::BOOL, val.getType());

    int v0;
    EXPECT_NO_THROW(v0 = val);
    EXPECT_EQ(1, v0);

    unsigned v1;
    EXPECT_NO_THROW(v1 = val);
    EXPECT_EQ(1, v1);

    float v2;
    EXPECT_THROW(v2 = val, Error);

    double v3;
    EXPECT_THROW(v3 = val, Error);

    bool v4;
    EXPECT_NO_THROW(v4 = (bool)val);
    EXPECT_TRUE(v4);
  }

  {
    Value val(nullptr);

    EXPECT_TRUE(val.isNull());

    val = mysqlx::nullvalue;

    EXPECT_TRUE(val.isNull());

    val = 0;

    EXPECT_FALSE(val.isNull());
  }
}


TEST_F(Types, basic)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.types..." << endl;

  sql("DROP TABLE IF EXISTS test.types");
  sql(
    "CREATE TABLE test.types("
    "  c0 INT,"
    "  c1 DECIMAL(4,2),"
    "  c2 FLOAT,"
    "  c3 DOUBLE,"
    "  c4 VARCHAR(32)"
    ")");

  Table types = getSchema("test").getTable("types");

  int data_int[]        = { 7, -7 };
  double data_decimal[] = { 3.14, -2.71 };
  float  data_float[]   = { 3.1415F, -2.7182F };
  double data_double[]  = { 3.141592, -2.718281 };
  string data_string[]  = { "First row", "Second row" };

  Row row(data_int[0], data_decimal[0], data_float[0], data_double[0],
          data_string[0]);

  types.insert()
    .values(row)
    .values(data_int[1], data_decimal[1], data_float[1], data_double[1],
            data_string[1])
    .execute();

  cout << "Table prepared, querying it..." << endl;

  RowResult res = types.select().execute();
  const Columns &cc = res.getColumns();

  cout << "Query sent, reading rows..." << endl;
  cout << "There are " << res.getColumnCount() << " columns in the result" << endl;

  //const Columns &cc = res.getColumns();

  EXPECT_EQ(string("c0"), cc[0].getColumnName());
  EXPECT_EQ(Type::INT, cc[0].getType());
  EXPECT_TRUE(cc[0].isNumberSigned());
  EXPECT_EQ(0, cc[0].getFractionalDigits());

  EXPECT_EQ(string("c1"), cc[1].getColumnName());
  EXPECT_EQ(Type::DECIMAL, cc[1].getType());
  cout << "column " << cc[1] << " precision: "
       << cc[1].getFractionalDigits() << endl;

  EXPECT_EQ(string("c2"), cc[2].getColumnName());
  EXPECT_EQ(Type::FLOAT, cc[2].getType());
  cout << "column " << cc[2] << " precision: "
    << cc[2].getFractionalDigits() << endl;

  const Column &c3 = res.getColumns()[3];
  EXPECT_EQ(string("c3"), c3.getColumnName());
  EXPECT_EQ(Type::DOUBLE, c3.getType());
  cout << "column " << c3 << " precision: "
    << c3.getFractionalDigits() << endl;

  const Column &c4 = res.getColumn(4);
  EXPECT_EQ(string("c4"), c4.getColumnName());
  EXPECT_EQ(Type::STRING, c4.getType());
  cout << "column " << res.getColumn(4) << " length: "
    << c4.getLength();
  cout << ", collation: " << c4.getCollationName() << endl;

  for (unsigned i = 0; (row = res.fetchOne()); ++i)
  {
    cout << "== next row ==" << endl;
    for (unsigned j = 0; j < res.getColumnCount(); ++j)
    {
      cout << "col " << res.getColumn(j) << ": " << row[j] << endl;
    }

    // Note: DECIMAL values are converted to double.

    EXPECT_EQ(Value::INT64,  row[0].getType());
    EXPECT_EQ(Value::DOUBLE, row[1].getType());
    EXPECT_EQ(Value::FLOAT,  row[2].getType());
    EXPECT_EQ(Value::DOUBLE, row[3].getType());
    EXPECT_EQ(Value::STRING, row[4].getType());

    EXPECT_EQ(data_int[i], (int)row[0]);
    EXPECT_EQ(data_decimal[i], (double)row[1]);
    EXPECT_EQ(data_float[i], (float)row[2]);
    EXPECT_EQ(data_double[i], (double)row[3]);
    EXPECT_EQ(data_string[i], (string)row[4]);

    EXPECT_GT(row[1].getRawBytes().size(), 1);
    EXPECT_EQ(data_string[i].length(), string(row[4]).length());
  }

  cout << "Testing Boolean value" << endl;

  types.remove().execute();

  Value bv(false);
  types.insert("c0").values(bv).execute();

  res = types.select().execute();

  row = res.fetchOne();
  EXPECT_TRUE(row);

  cout << "value: " << row[0] << endl;
  EXPECT_FALSE((bool)row[0]);

  cout << "Testing null value" << endl;

  types.update().set("c0", nullvalue).set("c1", nullptr).execute();
  res = types.select("c0","c1").execute();
  row = res.fetchOne();

  EXPECT_TRUE(row);
  EXPECT_TRUE(row[0].isNull());
  EXPECT_TRUE(row[1].isNull());

  cout << "Done!" << endl;
}


TEST_F(Types, integer)
{
  // Note: this part of the test does not require a running server

  {
    Value v1(-7);
    EXPECT_EQ(Value::INT64, v1.getType());
    EXPECT_EQ(-7, (int64_t)v1);

    Value v2(-7L);
    EXPECT_EQ(Value::INT64, v1.getType());
    EXPECT_EQ(-7, (int64_t)v1);

    Value v3(-7LL);
    EXPECT_EQ(Value::INT64, v1.getType());
    EXPECT_EQ(-7, (int64_t)v1);
  }

  {
    Value v1(7U);
    EXPECT_EQ(Value::UINT64, v1.getType());
    EXPECT_EQ(7, (uint64_t)v1);

    Value v2(7UL);
    EXPECT_EQ(Value::UINT64, v1.getType());
    EXPECT_EQ(7, (uint64_t)v1);

    Value v3(7ULL);
    EXPECT_EQ(Value::UINT64, v1.getType());
    EXPECT_EQ(7, (uint64_t)v1);
  }

  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.int_types..." << endl;

  sql("DROP TABLE IF EXISTS test.int_types");
  sql(
    "CREATE TABLE test.int_types("
    "  c0 INT,"
    "  c1 INT UNSIGNED"
    ")");

  Table types = getSchema("test").getTable("int_types");

  types.insert().values(-7, 7).execute();

  cout << "Table prepared, querying it..." << endl;

  RowResult res = types.select().execute();

  cout << "Query sent, reading rows..." << endl;
  cout << "There are " << res.getColumnCount() << " columns in the result" << endl;

  const Columns &cc = res.getColumns();

  EXPECT_EQ(Type::INT, cc[0].getType());
  EXPECT_TRUE(cc[0].isNumberSigned());

  EXPECT_EQ(Type::INT, cc[1].getType());
  EXPECT_FALSE(cc[1].isNumberSigned());
}


TEST_F(Types, string)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.types..." << endl;

  sql("DROP TABLE IF EXISTS test.types");
  sql(
    "CREATE TABLE test.types("
    "  c0 VARCHAR(10) COLLATE latin2_general_ci,"
    "  c1 VARCHAR(32) COLLATE utf8_swedish_ci,"
    "  c2 VARCHAR(32) CHARACTER SET latin2,"
    "  c3 VARCHAR(32) CHARACTER SET utf8mb4,"
    "  c4 VARCHAR(32)"  // use default collation
    ")"
  );

  Table types = getSchema("test").getTable("types");

  string str0(u"Foobar");
  string str1(u"Mog\u0119 je\u015B\u0107 szk\u0142o");

  types.insert().values(str0, str1, str1, str1, str1).execute();

  cout << "Table prepared, querying it..." << endl;

  RowResult res = getSchema("test").getTable("types").select().execute();

  /*
    FIXME: Reported result meta-data differs between 8.0.14 and earlier
    versions. For that reason the meta-data checks are disabled below.
  */

  const Column &c0 = res.getColumn(0);
  EXPECT_EQ(Type::STRING, c0.getType());
  cout << "column #0 length: " << c0.getLength() << endl;
  cout << "column #0 charset: " << c0.getCharacterSetName() << endl;
  cout << "column #0 collation: " << c0.getCollationName() << endl;

  EXPECT_EQ(10, c0.getLength());
  //EXPECT_EQ(CharacterSet::latin2, c0.getCharacterSet());
  //EXPECT_EQ(Collation<CharacterSet::latin2>::general_ci, c0.getCollation());

  const Column &c1 = res.getColumn(1);
  EXPECT_EQ(Type::STRING, c1.getType());
  cout << "column #1 length: " << c1.getLength() << endl;
  cout << "column #1 charset: " << c1.getCharacterSetName() << endl;
  cout << "column #1 collation: " << c1.getCollationName() << endl;

  /*
    FIXME: getLength() returns length in bytes, but for strings it should
    be length in characters (check X protocol specs).
  */

  //EXPECT_EQ(32, c0.getLength());
  //EXPECT_EQ(CharacterSet::utf8, c1.getCharacterSet());
  //EXPECT_EQ(Collation<CharacterSet::utf8>::swedish_ci, c1.getCollation());

  const Column &c2 = res.getColumn(2);
  EXPECT_EQ(Type::STRING, c2.getType());
  cout << "column #2 length: " << c2.getLength() << endl;
  cout << "column #2 charset: " << c2.getCharacterSetName() << endl;
  cout << "column #2 collation: " << c2.getCollationName() << endl;

  //EXPECT_EQ(CharacterSet::latin2, c2.getCharacterSet());

  const Column &c3 = res.getColumn(3);
  EXPECT_EQ(Type::STRING, c3.getType());
  cout << "column #3 length: " << c3.getLength() << endl;
  cout << "column #3 charset: " << c3.getCharacterSetName() << endl;
  cout << "column #3 collation: " << c3.getCollationName() << endl;

  //EXPECT_EQ(CharacterSet::utf8mb4, c3.getCharacterSet());

  const Column &c4 = res.getColumn(4);
  EXPECT_EQ(Type::STRING, c4.getType());
  cout << "column #4 length: " << c4.getLength() << endl;
  cout << "column #4 charset: " << c4.getCharacterSetName() << endl;
  cout << "column #4 collation: " << c4.getCollationName() << endl;

  Row row = res.fetchOne();

  EXPECT_EQ(str0, (string)row[0]);
  EXPECT_EQ(str1, (string)row[1]);
  EXPECT_EQ(str1, (string)row[3]);
  EXPECT_EQ(str1, (string)row[4]);

  /*
    FIXME: the third colum contains non-utf8 string which uses non-ascii
    characters. Currently we do not handle such strings and an error is
    thrown on an attempt of converting it to a C++ string.

    Replace with EXPECT_EQ() once we handle all MySQL charsets.
  */

  //EXPECT_THROW((string)row[2], Error);
}


inline
const CollationInfo* get_collation(unsigned id)
{
#define COLL_FIND(COL) COLLATIONS_##COL(COLL_FIND1)
#define COLL_FIND1(CS,ID,COLL,CASE) \
  case ID: return &Collation<CharacterSet::CS>::COLL_CONST_NAME(COLL,CASE);

  switch (id)
  {
    CDK_CS_LIST(COLL_FIND)
  default:
    return nullptr;
  }
}


TEST_F(Types, collations)
{
  SKIP_IF_NO_XPLUGIN;

  using col_data = std::pair<unsigned, string>;
  std::vector<col_data> unknown;

  Table t = getSchema("information_schema").getTable("collations");

  for (Row r : t.select("id", "collation_name").execute())
  {
    col_data col = { r[0], r[1] };

    const CollationInfo *info = get_collation(col.first);

    if (!info)
      unknown.push_back(col);
    else
    {
      EXPECT_EQ(std::string{ col.second }, std::string{ info->getName() })
        << "bad collation name";
    }
  }

  if (!unknown.empty())
  {
    cout << "Unknown collations:" << endl;
    for (col_data col : unknown)
    {
      cout << " -" << col.first << ": " << col.second << endl;
    }
    FAIL() << "There are unknown collations";
  }
}


TEST_F(Types, blob)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.types..." << endl;

  sql("DROP TABLE IF EXISTS test.types");
  sql(
    "CREATE TABLE test.types("
    "  c0 BLOB"
    ")"
  );

  Table types = getSchema("test").getTable("types");

  bytes data((byte*)"foo\0bar",7);

  types.insert().values(data).execute();

  cout << "Table prepared, querying it..." << endl;

  RowResult res = types.select().execute();

  const Column &c0 = res.getColumn(0);
  EXPECT_EQ(Type::BYTES, c0.getType());
  cout << "BLOB column length: " << c0.getLength() << endl;

  Row row = res.fetchOne();

  cout << "Got a row, checking data..." << endl;


  Value f0 = row[0];

  EXPECT_EQ(Value::RAW, f0.getType());

  const bytes &dd = f0.getRawBytes();

  cout << "Data length: " << dd.size() << endl;
  EXPECT_EQ(data.size(), dd.size());

  for (const byte *ptr = data.begin(); ptr < data.end(); ++ptr)
    EXPECT_EQ(*ptr, dd.begin()[ptr- data.begin()]);

  cout << "Data matches!" << endl;
}


TEST_F(Types, json)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.types..." << endl;

  sql("DROP TABLE IF EXISTS test.types");
  sql(
    "CREATE TABLE test.types("
    "  c0 JSON"
    ")"
    );

  Table types = getSchema("test").getTable("types");

  const char *json = "{"
    "\"foo\": 7,"
    "\"arr\": [1, 2, \"string\"],"
    "\"sub\": { \"day\": 20, \"month\": \"Apr\" }"
  "}";

  types.insert().values(json).execute();

  DbDoc doc(json);
  types.insert().values(doc).execute();

  cout << "Table prepared, querying it..." << endl;

  RowResult res = types.select().execute();

  cout << "Got results, checking data..." << endl;

  const Column &c0 = res.getColumn(0);
  EXPECT_EQ(Type::JSON, c0.getType());

  Row row;
  for (unsigned i = 0; (row = res.fetchOne()); ++i)
  {
    EXPECT_EQ(Value::DOCUMENT, row[0].getType());

    doc = row[0];
    cout << "- document: " << row[0] << endl;

    EXPECT_TRUE(doc.hasField("foo"));
    EXPECT_TRUE(doc.hasField("arr"));
    EXPECT_TRUE(doc.hasField("sub"));

    EXPECT_EQ(Value::UINT64, doc["foo"].getType());
    EXPECT_EQ(Value::ARRAY, doc["arr"].getType());
    EXPECT_EQ(Value::DOCUMENT, doc["sub"].getType());

    EXPECT_EQ(7, (int)doc["foo"]);
    EXPECT_EQ(3, doc["arr"].elementCount());
    EXPECT_TRUE(doc["sub"].hasField("day"));
    EXPECT_TRUE(doc["sub"].hasField("month"));
  }

  cout << endl << "Checking extracted JSON fields" << endl;

  {
    res = types.select("c0->$.foo","c0->$.sub.month","c0->$.no_such_field")
               .execute();
    EXPECT_EQ(Type::JSON, res.getColumn(0).getType());
    EXPECT_EQ(Type::JSON, res.getColumn(1).getType());
    EXPECT_EQ(Type::JSON, res.getColumn(2).getType());
    row = res.fetchOne();
    EXPECT_TRUE(row);

    EXPECT_EQ(Value::UINT64, row[0].getType());
    int c0 = row[0];
    cout << "c0 (int): " << c0 << endl;

    EXPECT_EQ(Value::STRING, row[1].getType());
    string c1 = row[1];
    cout << "c1 (string): " << c1 << endl;

    EXPECT_EQ(Value::VNULL, row[2].getType());
  }

  cout << endl << "Checking JSON array..." << endl;

  {
    types.remove().execute();
    doc = DbDoc(json);
    Value arr = { 1, "a", doc };

    types.insert().values(arr).values("[1, \"a\"]").execute();

    cout << "Arrays inserted, querying data..." << endl;

    res = types.select().execute();

    for (unsigned i = 0; (row = res.fetchOne()); ++i)
    {
      EXPECT_EQ(Value::ARRAY, row[0].getType());
      cout << endl << "next row" << endl;
      for (Value el : row[0])
        cout << " el: " << el << endl;

      EXPECT_EQ(1, (int)row[0][0]);
      EXPECT_EQ(string("a"), (string)row[0][1]);

      if (0 == i)
        EXPECT_EQ(Value::DOCUMENT, row[0][2].getType());
    }
  }

  //JSON Error reporting
  {
    {
      const string not_ending_double_quote[]=
      {{R"({"This is a wrong:"JSON Key"})"},
       {R"({"This is a wrong":"Value string})"},
       {R"({"This is a wrong":{"document":1})"},
      };

      for (auto &json : not_ending_double_quote)
      {
        try{
          DbDoc doc(json);
          for (auto field: doc)
          {
            std::cout << field << std::endl;
          }
        } catch(Error &e)
        {
          std::cout << e.what() << std::endl;
        }
      }
    }

    //stack overflow test
    {
      const int deep = 1000;
      std::string stack_overflow("{");
      for (int i=0; i < deep; ++i)
      {
        if (i == 0)
        {
          stack_overflow+=R"("overflow_doc":{ "overflow_arr":)";
          stack_overflow.append(deep, '[');
          stack_overflow+="1";
          stack_overflow.append(deep, ']');
          stack_overflow+=",";
        }
        else
        {
          stack_overflow+=R"("overflow_doc":{)";
        }
      }
      stack_overflow.append(deep, '}');
      stack_overflow += "}";
      try{
        DbDoc doc(stack_overflow);
        doc.begin();
        for (auto field: doc)
        {
          std::cout << field << std::endl;
        }
      } catch(Error &e)
      {
        FAIL() << e.what();
      }
    }
  }
}


TEST_F(Types, datetime)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.types..." << endl;

  sql("DROP TABLE IF EXISTS test.types");
  sql(
    "CREATE TABLE test.types("
    "  c0 DATE,"
    "  c1 TIME,"
    "  c2 DATETIME,"
    "  c3 TIMESTAMP,"
    "  c4 DATETIME"
    ")"
    );

  Table types = getSchema("test").getTable("types");

  Row data;

  data[0] = "2014-05-11";
  data[1] = "10:40:23.456";
  data[2] = "2014-05-11 10:40";
  data[3] = "2014-05-11 11:35:00.000";
  data[4] = Value();

  types.insert().values(data).execute();

  cout << "Table prepared, querying it..." << endl;

  RowResult res = types.select().execute();

  const Column &c0 = res.getColumn(0);
  cout << "column #0 type: " << c0.getType() << endl;
  EXPECT_EQ(Type::DATE, c0.getType());

  const Column &c1 = res.getColumn(1);
  cout << "column #1 type: " << c1.getType() << endl;
  EXPECT_EQ(Type::TIME, c1.getType());

  const Column &c2 = res.getColumn(2);
  cout << "column #2 type: " << c2.getType() << endl;
  EXPECT_EQ(Type::DATETIME, c2.getType());

  const Column &c3 = res.getColumn(3);
  cout << "column #3 type: " << c3.getType() << endl;
  EXPECT_EQ(Type::TIMESTAMP, c3.getType());

  const Column &c4 = res.getColumn(4);
  cout << "column #4 type: " << c4.getType() << endl;
  EXPECT_EQ(Type::DATETIME, c4.getType());


  Row row = res.fetchOne();

  EXPECT_TRUE(row);

  cout << "Got a row, checking data..." << endl;

  for (unsigned j = 0; j < res.getColumnCount(); ++j)
  {
    cout << "- col#" << j << ": " << row[j] << endl;
    if(j==4)
    {
      EXPECT_TRUE(row[j].isNull());
      break;
    }
    EXPECT_EQ(Value::RAW, row[j].getType());
    switch(res.getColumn(j).getType())
    {
    case Type::DATE:
    case Type::TIME:
      EXPECT_EQ(4, row[j].getRawBytes().size());
      break;
    case Type::DATETIME:
    case Type::TIMESTAMP:
      EXPECT_EQ(6, row[j].getRawBytes().size());
      break;
    default:
      FAIL() << "Unexpected type! Update UT";
    }
  }
}


TEST_F(Types, set_enum)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.types..." << endl;

  sql("DROP TABLE IF EXISTS test.types");
  sql(
    "CREATE TABLE test.types("
    "  c0 SET('a','b','c'),"
    "  c1 ENUM('a','b','c')"
    ")"
    );

  Table types = getSchema("test").getTable("types");

  Row data[2];

  data[0][0] = "a,b,c";
  data[0][1] = "a";

  data[1][0] = ""; // empty set
  data[1][1] = Value(); // NULL value

  TableInsert insert = types.insert();
  for (Row r : data)
    insert.values(r);
  insert.execute();

  cout << "Table prepared, querying it..." << endl;

  RowResult res = types.select().execute();

  cout << "Got result, checking data..." << endl;

  const Column &c0 = res.getColumn(0);
  cout << "column #0 type: " << c0.getType() << endl;
  EXPECT_EQ(Type::SET, c0.getType());
  cout << "- column #0 collation: " << c0.getCollationName() << endl;

  const Column &c1 = res.getColumn(1);
  cout << "column #1 type: " << c1.getType() << endl;
  EXPECT_EQ(Type::ENUM, c1.getType());
  cout << "- column #1 collation: " << c1.getCollationName() << endl;

  Row row;
  for (unsigned i = 0; (row = res.fetchOne()); ++i)
  {
    cout << "== next row ==" << endl;
    for (unsigned j = 0; j < res.getColumnCount(); ++j)
    {
      cout << "- col#" << j << ": " << row[j] << endl;
      if (Value::VNULL == data[i][j].getType())
        EXPECT_EQ(Value::VNULL, row[j].getType());
      else
        EXPECT_EQ(j == 0 ? Value::RAW : Value::STRING, row[j].getType());
    }
  }
}


TEST_F(Types, geometry)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.types_geom..." << endl;

  sql("DROP TABLE IF EXISTS test.types_geom");
  sql(
    "CREATE TABLE test.types_geom("
    "  c0 GEOMETRY,"
    "  c1 POINT,"
    "  c2 LINESTRING,"
    "  c3 POLYGON,"
    "  c4 MULTIPOINT,"
    "  c5 MULTILINESTRING,"
    "  c6 MULTIPOLYGON,"
    "  c7 GEOMETRYCOLLECTION"
    ")"
    );

  Table types = getSchema("test").getTable("types_geom");

  cout << "Table prepared, querying it..." << endl;

  {
    RowResult res = types.select().execute();

    const Columns &cc = res.getColumns();

    for (auto &c : cc)
    {
      EXPECT_EQ(Type::GEOMETRY, c.getType());
    }
  }

  {
    RowResult res = types.select("ST_AsBinary(c0)").execute();
    EXPECT_EQ(Type::BYTES, res.getColumn(0).getType());
  }

  {
    RowResult res = types.select("ST_AsText(c0)").execute();
    EXPECT_EQ(Type::STRING, res.getColumn(0).getType());
  }
}


TEST_F(Types, int64_conversion)
{
  SKIP_IF_NO_XPLUGIN;

  cout << "Preparing test.int64_conversion..." << endl;

  Value value(std::numeric_limits<int64_t>::max());

  int64_t int64_v = value;

  EXPECT_EQ(std::numeric_limits<int64_t>::max(), int64_v);

  //should overflow
  int int_v;
  EXPECT_ANY_THROW(int_v = value);

  //now with min value
  value = std::numeric_limits<int64_t>::min();

  int64_v = value;

  EXPECT_EQ(std::numeric_limits<int64_t>::min(), int64_v);

  EXPECT_ANY_THROW(int_v = value);

  //Now using the uint64_t max.
  value = std::numeric_limits<uint64_t>::max();

  uint64_t uint64_v = value;

  EXPECT_EQ(std::numeric_limits<uint64_t>::max(), uint64_v);

  EXPECT_ANY_THROW(int64_v = value);

  EXPECT_ANY_THROW(int_v = value);

}
