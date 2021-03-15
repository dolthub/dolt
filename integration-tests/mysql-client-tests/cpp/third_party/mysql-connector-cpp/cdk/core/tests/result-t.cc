/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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
 * which is part of <MySQL Product>, is also subject to the
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

#include "test.h"
#include "session_test.h"

#include <iostream>
#include <mysql/cdk.h>


using ::std::cout;
using ::std::endl;
using namespace ::cdk;

class Result
    : public cdk::test::Core_test
    , public cdk::test::Row_processor<cdk::Meta_data>
{
public:

  scoped_ptr<Session> m_sess;

  virtual void SetUp()
  {
    Core_test::SetUp();

    if (!has_xplugin())
      return;

    m_sess.reset(new Session(this));
    if (!m_sess->is_valid())
      FAIL() << "could not create session";
  }

  Session& get_sess()
  {
    return *m_sess;
  }

  void do_sql(const string &query)
  {
    Reply r;
    r = m_sess->sql(query);
    r.wait();
    if (0 < r.entry_count())
      r.get_error().rethrow ();
  }
};


using ::std::cout;
using ::std::endl;
using namespace ::cdk;
using namespace ::cdk::test;


class Result_cs : public Result
{
public:

  Table_ref t;

  Result_cs()
    : t("t1", "test")
  {}

  string expected_string;

  void process_field_val(col_count_t pos, bytes data,
                         const cdk::string &val)
  {
    std::cout << val;
    if (!expected_string.empty())
      EXPECT_EQ(expected_string, val);
  }

  /*
    Check that we correctly retreive non-ascii strings if stored using
    utf8 encoding.
  */

  void check1(const char *cs, const string &val)
  {
    cout << "Testing " << cs << " string stored as utf8" << endl;
    create_table("utf8");
    insert_string(val);
    Reply select(get_sess().table_select(t));
    Cursor c(select);
    set_meta_data(c);
    expected_string = val;
    c.get_rows(*this);
    c.wait();
  }

  /*
    Check that non-utf8 strings which use non-ascii characters trigger
    string conversion errors (as, at the moment, we do not support encodings
    other than utf8)
  */

  void check2(const char *cs, const string &val)
  {
    cout << "Testing " << cs << " string (expected conversion error)" << endl;
    create_table(cs);
    insert_string(val);
    Reply select(get_sess().table_select(t));
    Cursor c(select);
    set_meta_data(c);
    EXPECT_THROW({ c.get_rows(*this); c.wait(); }, cdk::Error);
    cout << endl;
  }

  /*
    Check that non-utf8 strings which consist only of ascii characters are
    handled correctly.
  */

  void check3(const char *cs)
  {
    cout << "Testing ascii string stored as " << cs << endl;
    expected_string = "I can eat glass{}, [].!";
    create_table(cs);
    insert_string(expected_string);
    Reply select(get_sess().table_select(t));
    Cursor c(select);
    set_meta_data(c);
    c.get_rows(*this);
    c.wait();
  }

  void create_table(const char *cs)
  {
    create_table(t, cs);
  }

  void create_table(const Table_ref &t, const char *cs)
  {
    do_sql(
      string(L"drop table if exists ")
      + L"`" + t.schema()->name() + L"`.`" + t.name() + L"`");
    do_sql(
      string(L"create table ")
      + L"`" + t.schema()->name() + L"`.`" + t.name() + L"`"
      + L"(c text character set " + string(cs) + L")"
    );
  }

  void insert_string(const string &val)
  {
    insert_string(t, val);
  }

  void insert_string(const Table_ref &t, const string &val)
  {
    do_sql(
      string(L"insert into ")
      + L"`" + t.schema()->name() + L"`.`" + t.name() + L"`"
      + L" values ('" + val + L"')"
    );
  }

};


// Note: samples taken from foundation codec_t test.

#define SAMPLES(X) \
  X (polish, "latin2", L"Mog\u0119 je\u015B\u0107 szk\u0142o", \
     "\x4D\x6F\x67\xC4\x99\x20\x6A\x65\xC5\x9B\xC4\x87\x20\x73\x7A\x6B\xC5\x82\x6F") \
  X (japaneese, "ujis", L"\u79C1\u306F\u30AC\u30E9\u30B9\u3092\u98DF\u3079\u3089\u308C\u307E\u3059\u3002\u305D\u308C\u306F\u79C1\u3092\u50B7\u3064\u3051\u307E\u305B\u3093\u3002", \
     "\xE7\xA7\x81\xE3\x81\xAF\xE3\x82\xAC\xE3\x83\xA9\xE3\x82\xB9\xE3\x82\x92\xE9\xA3\x9F\xE3\x81\xB9\xE3\x82\x89\xE3\x82\x8C\xE3\x81\xBE\xE3\x81\x99\xE3\x80\x82\xE3\x81\x9D\xE3\x82\x8C\xE3\x81\xAF\xE7\xA7\x81\xE3\x82\x92\xE5\x82\xB7\xE3\x81\xA4\xE3\x81\x91\xE3\x81\xBE\xE3\x81\x9B\xE3\x82\x93\xE3\x80\x82") \
  X (ukrainian, "koi8u", L"\u042F \u043C\u043E\u0436\u0443 \u0457\u0441\u0442\u0438 \u0441\u043A\u043B\u043E, \u0456 \u0432\u043E\u043D\u043E \u043C\u0435\u043D\u0456 \u043D\u0435 \u0437\u0430\u0448\u043A\u043E\u0434\u0438\u0442\u044C", \
     "\xD0\xAF\x20\xD0\xBC\xD0\xBE\xD0\xB6\xD1\x83\x20\xD1\x97\xD1\x81\xD1\x82\xD0\xB8\x20\xD1\x81\xD0\xBA\xD0\xBB\xD0\xBE\x2C\x20\xD1\x96\x20\xD0\xB2\xD0\xBE\xD0\xBD\xD0\xBE\x20\xD0\xBC\xD0\xB5\xD0\xBD\xD1\x96\x20\xD0\xBD\xD0\xB5\x20\xD0\xB7\xD0\xB0\xD1\x88\xD0\xBA\xD0\xBE\xD0\xB4\xD0\xB8\xD1\x82\xD1\x8C") \
  X (portuguese, "latin1", L"Posso comer vidro, n\u00E3o me faz mal", \
     "\x50\x6F\x73\x73\x6F\x20\x63\x6F\x6D\x65\x72\x20\x76\x69\x64\x72\x6F\x2C\x20\x6E\xC3\xA3\x6F\x20\x6D\x65\x20\x66\x61\x7A\x20\x6D\x61\x6C")


TEST_F(Result_cs, strings)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {
    cout << "== CHECK 1 ==" << endl;

#define CHECK1(NAME,CS,WIDE,UTF) check1(CS,WIDE);

    SAMPLES(CHECK1)

    cout <<endl << "== CHECK 2 ==" << endl;

#define CHECK2(NAME,CS,WIDE,UTF) check2(CS,WIDE);

    SAMPLES(CHECK2)

    cout <<endl << "== CHECK 3 ==" << endl;

#define CHECK3(NAME,CS,WIDE,UTF) check3(CS);

    SAMPLES(CHECK3)

  }
  CATCH_TEST_GENERIC
}

