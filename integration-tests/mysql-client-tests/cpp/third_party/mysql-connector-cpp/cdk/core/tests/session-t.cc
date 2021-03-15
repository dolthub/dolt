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

class Session_core
    : public cdk::test::Core_test
    , public cdk::test::Row_processor<cdk::Meta_data>
{
public:

  static void do_sql(Session &sess, const string &query)
  {
    Reply r;
    r = sess.sql(query);
    r.wait();
    if (0 < r.entry_count())
      r.get_error().rethrow();
  }
};



/*
  Basic test that connects to the test server, sends a message and
  reads server's reply.

  Note: Test server should be started before running this test.
*/

using ::std::cout;
using ::std::endl;
using namespace ::cdk;
using namespace ::cdk::test;


TEST_F(Session_core, basic)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {

    ds::TCPIP ds("localhost", m_port);
    ds::TCPIP::Options options("root");
    cdk::Session s1(ds, options);


    if (!s1.is_valid())
      FAIL() << "Invalid Session created";

    if (!s1.check_valid())
      FAIL() << "Invalid Session created";

    s1.close();

    if (s1.is_valid())
      FAIL() << "Invalid Session after close()";

    if (s1.check_valid())
      FAIL() << "Invalid Session after close()";

  }
  catch (Error &e)
  {
    FAIL() << "Connection error: " << e << endl;
  }

}


TEST_F(Session_core, default_schema)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {

    ds::TCPIP ds("localhost", m_port);
    ds::TCPIP::Options options("root");
    options.set_database("test");

    cdk::Session s(ds, options);


    if (!s.is_valid())
      FAIL() << "Invalid Session created";

    if (!s.check_valid())
      FAIL() << "Invalid Session created";

    Reply r(s.sql(L"SELECT DATABASE()"));
    r.wait();

    Cursor c(r);

    struct : cdk::Row_processor
    {
      // Row_processor callbacks

      virtual bool row_begin(row_count_t row)
      {
        return true;
      }
      virtual void row_end(row_count_t row)
      {}

      virtual void field_null(col_count_t pos)
      {}

      virtual size_t field_begin(col_count_t pos, size_t)
      {
        return  SIZE_MAX;
      }

      size_t field_data(col_count_t pos, bytes data)
      {
        EXPECT_EQ(0, pos);

        // We expect string with current schema name

        cdk::foundation::Codec<cdk::foundation::Type::STRING> codec;
        cdk::string db;

        // Trim trailing \0
        bytes d1(data.begin(), data.end() - 1);
        codec.from_bytes(d1, db);

        cout << "current schema: " << db << endl;
        EXPECT_EQ(string("test"),db);

        return 0;
      }

      virtual void field_end(col_count_t /*pos*/)
      {}

      virtual void end_of_data()
      {}
    }
    prc;

    set_meta_data(c);
    c.get_rows(prc);
    c.wait();

  }
  catch (Error &e)
  {
    FAIL() << "CDK error: " << e << endl;
  }

}


TEST_F(Session_core, sql_basic)
{
  try {
    SKIP_IF_NO_XPLUGIN;

  Session s(this);

    // No results
    {
      Reply rp;
      rp = s.sql(L"select * from mysql.user where invalid query :) ;");
      EXPECT_FALSE(rp.has_results());
    }

    for (int i = 0; i >=0 ;i++)
    {
      Reply rp;
      rp = s.sql(L"select * from mysql.user;");

      switch (i)
      {

      case 0:
        cout <<endl <<"== Case 0 ==" <<endl;
        {
          /*
             Normal results treatment
          */

          EXPECT_TRUE(rp.has_results());
          Cursor cursor(rp);

          set_meta_data(cursor);
          cursor.get_rows(*this);
          cursor.wait();

          EXPECT_FALSE(rp.has_results());

        }
        break;

      case 1:
        cout <<endl <<"== Case 1 ==" <<endl;
        {
          /*
             Second attempt to read cursor without results
          */

          EXPECT_TRUE(rp.has_results());
          Cursor cursor(rp);

          EXPECT_FALSE(rp.has_results());
          cursor.close();

          try
          {
            Cursor cr2(rp);
            FAIL() << "Should throw exception because there are no results now";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }

        }
        break;

      case 2:
        cout <<endl <<"== Case 2 ==" <<endl;
        {
          /*
             Skip Result
          */

          rp.skip_result();

          EXPECT_FALSE(rp.has_results());

          try
          {
            Cursor c2(rp);
            FAIL() << "No exception thrown";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }

        }
        break;

      case 3:
        cout <<endl <<"== Case 3 ==" <<endl;
        {
          /*
             Skip Result cannot be done when cursor exists
          */

          Cursor cursor(rp);

          try
          {
            rp.skip_result();

            FAIL() << "No exception thrown";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }

        }
        break;


      case 4:
        cout <<endl <<"== Case 4 ==" <<endl;
        {
          /*
             Discard Result
          */

          rp.discard();

          EXPECT_FALSE(rp.has_results());
          try
          {
            Cursor c2(rp);
            FAIL() << "No exception thrown";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }
        }
        break;

      case 5:
        cout <<endl <<"== Case 5 ==" <<endl;
        {
          /*
             Discard Result cannot be done when cursor exists
          */

          Cursor cursor(rp);

          try
          {
            rp.discard();

            FAIL() << "No exception thrown";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }

        }
        break;

      case 6:
        cout <<endl <<"== Case 6 ==" <<endl;
        {
          rp.skip_result();

          EXPECT_EQ(0U, rp.affected_rows());
        }
        break;

      case 7:
        cout <<endl <<"== Case 7 ==" <<endl;
        {
          Cursor cursor(rp);

          try {

            rp.affected_rows();
            FAIL() << "Affected Rows before end of cursor";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }

        }
        break;


      default:
        //break;
        i = -2;
        break;
      }
    }

    cout <<endl <<"== Diagnostics ==" <<endl;
    //TODO: Where should erros on queries stay?
    //      Since processors are all on

    for (Session::Diagnostics::Iterator &it= s.get_entries();
         it.next();)
    {
      FAIL() << it.entry().description();
    }

    cout <<"Done!" <<endl;

  }
  CATCH_TEST_GENERIC
}



TEST_F(Session_core, sql_args)
{
  try {
  SKIP_IF_NO_XPLUGIN;

  Session s(this);

  if (!s.is_valid())
    FAIL() << "Invalid Session!";

  struct Args
    : public Any_list
  {
    void process(Processor &prc) const
    {
      Safe_prc<Processor> sprc(prc);
      sprc->list_begin();
      sprc->list_el()->scalar()->num((uint64_t)7);
      sprc->list_el()->scalar()->num((int64_t)-7);
      sprc->list_el()->scalar()->num((double)3.141592);
      sprc->list_el()->scalar()->str("Hello World!");
      sprc->list_end();
    }
  }
  args;

  {
    Reply rp;
    rp = s.sql(L"select ? as A, ? as B, ? as C, ? as D;", &args);

    EXPECT_TRUE(rp.has_results());
    Cursor cursor(rp);

    set_meta_data(cursor);
    cursor.get_rows(*this);
    cursor.wait();

    EXPECT_FALSE(rp.has_results());
  }

  cout <<"Done!" <<endl;

}
  CATCH_TEST_GENERIC
}


TEST_F(Session_core, meta_data)
{
  // TODO: More extensive tests checking that meta-data reported
  // by CDK is as expected.

  try {
  SKIP_IF_NO_XPLUGIN;

  Session s(this);

  if (!s.is_valid())
    FAIL() << "Invalid Session!";

  struct Tables
  {
    Session &m_sess;

    Tables(Session &sess) : m_sess(sess)
    {
      drop_tables();
      do_sql(m_sess, L"USE test");
      do_sql(m_sess, L"CREATE TABLE meta_data1 (col1 JSON)");
      do_sql(m_sess, L"CREATE TABLE meta_data2 (col1 CHAR(16))");
    }

    ~Tables()
    {
      drop_tables();
    }

    void drop_tables()
    {
      do_sql(m_sess, L"USE test");
      do_sql(m_sess, L"DROP TABLE IF EXISTS meta_data1");
      do_sql(m_sess, L"DROP TABLE IF EXISTS meta_data2");
    }
  }
  create_tables(s);

  {
    Reply rp;
    rp = s.sql(L"SELECT col1 FROM meta_data1");

    EXPECT_TRUE(rp.has_results());
    Cursor cursor(rp);

    EXPECT_EQ(TYPE_DOCUMENT, cursor.type(0));
  }

  {
    Reply rp;
    rp = s.sql(L"SELECT col1 FROM meta_data2");

    EXPECT_TRUE(rp.has_results());
    Cursor cursor(rp);

    EXPECT_EQ(TYPE_STRING, cursor.type(0));
  }

  cout <<"Done!" <<endl;

}
  CATCH_TEST_GENERIC
}


TEST_F(Session_core, affected)
{
  try {
    SKIP_IF_NO_XPLUGIN;

    Session s(this);

    if (!s.is_valid())
      FAIL() << "Invalid Session!";

    do_sql(s, L"USE test");

    cout << "Current schema: " << s.current_schema() << endl;

    do_sql(s, L"DROP TABLE IF EXISTS affected");
    do_sql(s, L"CREATE TABLE affected (c0 INT)");

    Table_ref tbl("affected", "test");

    struct : public Row_source
    {
      int m_val;

      void process(Processor &prc) const
      {
        Safe_prc<Processor> sprc(prc);

        prc.list_begin();
        sprc->list_el()->scalar()->val()->num((int64_t)m_val);
        prc.list_end();
      }

      bool next()
      {
        if (m_val <= 0)
          return false;
        m_val--;
        return true;
      }
    }
    data;
    data.m_val = 7;

    {
      cout << "inserting data into table" << endl;

      Reply rp(s.table_insert(tbl, data, NULL, NULL));
      rp.wait();
      cout << "affected rows: " << rp.affected_rows() << endl;
      EXPECT_EQ(7, rp.affected_rows());

      rp.discard();

      /*
        After discarding reply, affected rows count is no
        longer available.
      */

      EXPECT_THROW(rp.affected_rows(), Error);
    }

    {
      cout << "fetching data from table" << endl;

      Reply rp(s.table_select(tbl, NULL));
      rp.wait();

      /*
        Affected rows count is available only for statements
        that do not produce results.
      */

      EXPECT_THROW(rp.affected_rows(), Error);

      rp.discard();
    }

    cout << "Done!" << endl;

  }
  CATCH_TEST_GENERIC
}


/*
  Test handling of multi-result-sets
*/

TEST_F(Session_core, sql_multi_rset)
{
  try {
    SKIP_IF_NO_XPLUGIN;

    Session s(this);

    if (!s.is_valid())
      FAIL() << "Invalid Session!";

    do_sql(s, L"DROP PROCEDURE IF EXISTS test.test");
    do_sql(s, L"CREATE PROCEDURE test.test() BEGIN SELECT 1; SELECT 'foo', 2; END");

    {
      Reply rp;
      rp = s.sql(L"CALL test.test()");

      EXPECT_TRUE(rp.has_results());
      {
        cout << "-- next result-set" << endl;
        Cursor cursor(rp);
        set_meta_data(cursor);
        cursor.get_rows(*this);
        cursor.wait();
        EXPECT_EQ(cdk::TYPE_INTEGER, cursor.type(0));
      }

      EXPECT_TRUE(rp.has_results());
      {
        cout << "-- next result-set" << endl;
        Cursor cursor(rp);
        set_meta_data(cursor);
        cursor.get_rows(*this);
        cursor.wait();
        EXPECT_EQ(cdk::TYPE_STRING, cursor.type(0));
      }

      EXPECT_FALSE(rp.has_results());
    }

    cout << "Test discarding of multi-result-set reply" << endl;

    {
      Reply rp;
      rp = s.sql(L"CALL test.test()");

      EXPECT_TRUE(rp.has_results());
      {
        Cursor cursor(rp);
      }

      EXPECT_TRUE(rp.has_results());
    }

    cout << "reply discarded" << endl;

    {
      Reply rp;
      rp = s.sql(L"CALL test.test()");

      EXPECT_TRUE(rp.has_results());
    }

    cout << "reply discarded" << endl;

    // TODO: Test output parameters when xplugin supports it.
    //do_sql(s, L"CREATE PROCEDURE test.test(OUT x INT) BEGIN SELECT 1; SET x = 2; END");
    //rp = s.sql(L"CALL test.test(@ret)");

    cout << "Done!" << endl;

  }
  CATCH_TEST_GENERIC
}


TEST_F(Session_core, trx)
{
  try {
    SKIP_IF_NO_XPLUGIN;

    Session s(this);

    if (!s.is_valid())
      FAIL() << "Invalid Session!";

    do_sql(s, L"DROP TABLE IF EXISTS t");
    do_sql(s, L"CREATE TABLE t (a INT)");

    // These commands should be no-op without any transaction.

    s.commit();
    s.rollback();

    try {

      s.begin();
      do_sql(s, L"INSERT INTO t VALUES (1)");
      do_sql(s, L"INSERT INTO t VALUES (2)");
      s.commit();

      s.begin();
      do_sql(s, L"INSERT INTO t VALUES (3)");
      do_sql(s, L"INSERT INTO t VALUES (4)");
      s.rollback();
    }
    catch (...)
    {
      s.rollback();
      throw;
    }

    /*
      A processor used to process and chek result of SELECT a FROM t.
    */

    struct Prc : cdk::Row_processor
    {
      // integer encoding format used by the result
      const Format_info *m_fi;
      std::vector<int> m_vals;
      size_t m_row_cnt;

      Prc() : m_row_cnt(0) {}

      void reset(Meta_data &md)
      {
        EXPECT_EQ(cdk::TYPE_INTEGER, md.type(0));
        m_row_cnt = 0;
        m_fi = &md.format(0);
      }

      void add(int x)
      {
        m_vals.push_back(x);
      }

      void clear()
      {
        m_vals.clear();
      }

      bool row_begin(row_count_t pos)
      {
        cout << "- row#" << pos << ": ";
        return true;
      }
      void row_end(row_count_t pos)
      {
        cout << endl;
        m_row_cnt++;
      }

      size_t field_begin(col_count_t pos, size_t data_len)
      {
        return 1024;
      }
      void field_end(col_count_t pos) {}

      void field_null(col_count_t pos)
      {
        FAIL() << "Unexpected NULL value in reply";
      }

      size_t field_data(col_count_t pos, bytes data)
      {
        // only one column expected
        EXPECT_EQ(0, pos);

        assert(m_fi);
        Codec<cdk::TYPE_INTEGER> codec(*m_fi);
        int val;
        codec.from_bytes(data, val);
        cout << val;
        EXPECT_EQ(m_vals.at(m_row_cnt), val);
        return 0;
      }

      void end_of_data()
      {
        EXPECT_EQ(m_vals.size(), m_row_cnt);
      }
    };

    Prc prc;
    prc.add(1);
    prc.add(2);

    Reply r;

    {
      r = s.sql(L"SELECT a FROM t");
      Cursor c(r);
      prc.reset(c);
      cout << "== processing rows ==" << endl;
      c.get_rows(prc);
      c.wait();
      cout << "== end of data ==" << endl;
    }

    {
      Session s1(this);
      s1.begin();
      do_sql(s1, "DELETE FROM t WHERE a = 2");
      // when session is destroyed, opened transaction should
      // be rolled back.
    }

    {
      r = s.sql(L"SELECT a FROM t");
      Cursor c(r);
      prc.reset(c);
      cout << "== processing rows ==" << endl;
      c.get_rows(prc);
      c.wait();
      cout << "== end of data ==" << endl;
    }

    //With SavePoints

    s.begin();
    do_sql(s, L"INSERT INTO t VALUES (3)");
    s.savepoint_set("P3");
    do_sql(s, L"INSERT INTO t VALUES (4)");
    s.savepoint_set("P4");
    do_sql(s, L"INSERT INTO t VALUES (5)");
    s.savepoint_set("P5");
    do_sql(s, L"INSERT INTO t VALUES (5)");
    s.savepoint_set("P6");
    s.savepoint_remove("P5");

    //Removing/Setting empty savepoint
    EXPECT_THROW(s.savepoint_set(""), Error);
    EXPECT_THROW(s.savepoint_remove(""), Error);

    //removing already removed savepoint
    EXPECT_THROW(s.savepoint_remove("P5"), Error);

    // Rollback to removed savepoint
    EXPECT_THROW(s.rollback("P5"), Error);

    s.rollback("P4");
    s.savepoint_remove("P3");

    //Savepoint should have been removed, since was not used on the rollback!
    EXPECT_THROW(s.savepoint_remove("P6"), Error);

    s.commit();

    prc.add(3);
    prc.add(4);

    {
      r = s.sql(L"SELECT a FROM t");
      Cursor c(r);
      prc.reset(c);
      cout << "== processing rows ==" << endl;
      c.get_rows(prc);
      c.wait();
      cout << "== end of data ==" << endl;
    }

    // Negative tests

    s.begin();

    try {
      s.begin();
    }
    catch (const Error &e)
    {
      cout << "Expected error: " << e << endl;
      EXPECT_EQ(cdkerrc::in_transaction, e.code());
    }

    cout << "Done!" << endl;

  }
  CATCH_TEST_GENERIC
}


#if 0

parser::JSON_parser m_parser;

  Doc(const string &json)
    : m_parser(json)
  {
    reset(m_parser);
  }

  using Doc_converter::process;
};


struct Doc_list_base
{
  virtual Doc& get_doc(uint32_t) const =0;
};


template <uint32_t N>
struct Doc_list
  : public Doc_list_base
  , public Expr_list
  , public Expression
{

  const wchar_t **m_list;

  Doc_list(const wchar_t *list[N]) : m_list(list)
  {}

  uint32_t count() const { return N; }

  boost::scoped_ptr<Doc> m_doc;

  Doc& get_doc(uint32_t pos) const
  {
    Doc_list *self= const_cast<Doc_list*>(this);
    self->m_doc.reset(new Doc(m_list[pos]));
    return *m_doc;
  }

  uint32_t m_pos;

  const Expression& get(uint32_t pos) const
  {
    Doc_list *self= const_cast<Doc_list*>(this);
    self->m_pos= pos;
    return *this; //static_cast<Expression*>(self);
  }

  void process(Expression::Processor &prc) const
  {
    prc.doc(get_doc(m_pos));
  }
};



TEST_F(Session_core, docs)
{
  // TODO: Share this between different tests
  static const wchar_t *docs[] =
  {
    L"{'_id': 'uuid-1', 'str': 'foo', 'num': 123, 'bool': true}",
    L"{'_id': 'uuid-2', 'str': 'bar', 'doc': {'str': 'foo', 'num': 123, 'bool': true}}",
  };

  try {

    SKIP_IF_NO_XPLUGIN;

    Session &s= get_session();

    Object_ref coll("my_coll", "test");
    Reply r;

    {
      cout <<"== Creating collection" <<endl;
      r= s.admin("create_collection", coll);
      // note: ignoring "collection already exists" error
    }

    {
      cout <<"== Inserting documents" <<endl;
      Doc_list<sizeof(docs)/sizeof(wchar_t*)> list(docs);
      r= s.coll_add(coll, list);
      r.wait();
      if (0 < r.entry_count())
        r.get_error().rethrow();
    }

    {
      cout <<"== Reading results" <<endl;
      r= s.coll_find(coll, NULL);
      Cursor c(r);
      set_meta_data(c);
      c.get_rows(*this);
      c.wait();
    }

    //s.coll_add(coll, docs);
    cout <<"== Done!" <<endl;
  }
  CATCH_TEST_GENERIC
}

#endif


#ifdef WITH_SSL

TEST_F(Session_core, tls_options)
{
  SKIP_IF_NO_XPLUGIN;

  struct row_processor_variable
      : cdk::Row_processor
  {
    row_processor_variable(std::string &variable)
      : m_variable(variable)
    {}

    virtual bool row_begin(row_count_t pos)
    {
      return true;
    }

    virtual void row_end(row_count_t pos) {}

    virtual size_t field_begin(col_count_t pos, size_t data_len)
    {
      return data_len;
    }

    virtual void field_end(col_count_t pos) {}

    virtual void field_null(col_count_t pos) {}

    virtual size_t field_data(col_count_t pos, bytes data)
    {
      if (pos == 1)
        m_variable.assign(data.begin(), data.end()-1);

      return data.size();
    }

    virtual void end_of_data() {}

    std::string& m_variable;

  };

  try
  {

    ds::TCPIP ds("localhost", m_port);
    ds::TCPIP::Options options("root");
    connection::TLS::Options tls_options;

    std::string ssl_ca;
    std::string datadir;

    {

      cdk::Session s_tmp(ds, options);

      Reply ssl_var(s_tmp.sql("show global variables like 'ssl_ca';"));

      if (ssl_var.has_results())
      {
        Cursor cur(ssl_var);

        row_processor_variable m_row_ca(ssl_ca);

        cur.get_row(m_row_ca);

        cout << "Server CA: " << ssl_ca << endl;
      }

      // CA path is same as data dir
      Reply ssl_var_path(s_tmp.sql("show global variables like 'datadir';"));
      if (ssl_var_path.has_results())
      {
        Cursor cur(ssl_var_path);

        row_processor_variable m_row_ca_path(datadir);

        cur.get_row(m_row_ca_path);

        cout << "Server data dir: " << datadir << endl;
      }
    }

    if (ssl_ca.find('\\') == string::npos && ssl_ca.find('/') == string::npos)
    { //not full path
      ssl_ca = datadir + ssl_ca;
    }

    cout << "Setting CA to: " << ssl_ca << endl;

    tls_options.set_ca(ssl_ca);

    options.set_tls(tls_options);

    cdk::Session s1(ds, options);


    if (!s1.is_valid())
      FAIL() << "Invalid Session created";

    ssl_ca.erase(ssl_ca.size()-1);

    cout << "Setting CA to: " << ssl_ca << endl;

    tls_options.set_ca(ssl_ca);

    options.set_tls(tls_options);

    EXPECT_THROW(cdk::Session s1(ds, options), Error);

  }
  catch (Error &e)
  {
    FAIL() << "Connection error: " << e << endl;
  }

}

#endif

TEST_F(Session_core, failover_add)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {
    unsigned short highest_priority = 100;
    ds::TCPIP ds("localhost", m_port);
    ds::TCPIP::Options options("root");
    ds::Multi_source ms;

    ms.add(ds, options, highest_priority);
    try
    {
      /* Try adding non-prioritized item to a prioritized list */
      ms.add(ds, options, 0);
      FAIL() << "Mixing data sources with and without priority";
    }
    catch (cdk::Error &err)
    {
      if (err.code() == cdk::cdkerrc::generic_error)
        cout << "Expected error: " << err << endl;
      else
        FAIL() << "Unexpected error: " << err;
    }

    ms.clear();

    ms.add(ds, options, 0);
    try
    {
      /* Try adding prioritized item to a non-prioritized list */
      ms.add(ds, options, highest_priority);
      FAIL() << "Mixing data sources with and without priority";
    }
    catch (cdk::Error &err)
    {
      if (err.code() == cdk::cdkerrc::generic_error)
        cout << "Expected error: " << err << endl;
      else
        FAIL() << "Unexpected error: " << err;
    }
  }
  catch (Error &e)
  {
    FAIL() << "CDK error: " << e << endl;
  }

}


TEST_F(Session_core, failover_error)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {
    unsigned short highest_priority = 100;
    ds::TCPIP ds("localhost", m_port);
    ds::TCPIP::Options options("root");

    ds::TCPIP ds_error("localhost", m_port + 1);
    std::string bad_pwd = "bad_password";
    ds::TCPIP::Options options_error("non_existing_user", &bad_pwd);

    ds::Multi_source ms;

    ms.add(ds_error, options, highest_priority);
    ms.add(ds_error, options, highest_priority);
    ms.add(ds_error, options, highest_priority - 1);
    ms.add(ds_error, options, highest_priority - 1);

    /*
    Testing how Multi_source walks thrhough all added
    data sources without being able to connect.
    The exception must be thrown when the end of the list
    is reached
    */
    try
    {
      cdk::Session s(ms);
      FAIL() << "Exception is expected";
    }
    catch (cdk::Error &err)
    {
      if (err.code() == cdk::cdkerrc::generic_error)
        cout << "Expected error: " << err << endl;
      else
        FAIL() << "Unexpected error: " << err;
    }

    ms.clear();
    /* This will not connect, but no critical error */
    ms.add(ds_error, options, highest_priority);

    /* This will give the auth error */
    ms.add(ds, options_error, highest_priority - 1);

    /* This is able to connect, but should never be reached */
    ms.add(ds, options, highest_priority - 2);

    /*
    Testing how Multi_source encounters authentication
    error. No more connecting attempts should be made.
    The last correct data source must not be tried.
    */
    cdk::Session s(ms);
    if (s.is_valid())
      FAIL() << "Session is supposed to be invalid";
    else
    {
      cout << "Expected error: " << s.get_error() << endl;
    }
  }
  catch (Error &e)
  {
    FAIL() << "CDK error: " << e << endl;
  }

}


TEST_F(Session_core, failover)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {

    Session sess(this);

    {
      Reply r(sess.sql("CREATE SCHEMA IF NOT EXISTS failover_test_1"));
      r.wait();
      if (r.entry_count()) FAIL() << "Error creating schema";
    }

    {
      Reply r(sess.sql("CREATE SCHEMA IF NOT EXISTS failover_test_2"));
      r.wait();
      if (r.entry_count()) FAIL() << "Error creating schema";
    }

    {
      Reply r(sess.sql("CREATE SCHEMA IF NOT EXISTS failover_test_3"));
      r.wait();
      if (r.entry_count()) FAIL() << "Error creating schema";
    }

    unsigned short highest_priority = 100;
    ds::TCPIP ds_correct("localhost", m_port);
    ds::TCPIP ds_error("localhost", m_port + 1);

    ds::TCPIP::Options options("root");
    options.set_database("test");

    ds::TCPIP::Options options_db1("root");
    options_db1.set_database("failover_test_1");

    ds::TCPIP::Options options_db2("root");
    options_db2.set_database("failover_test_2");

    ds::TCPIP::Options options_db3("root");
    options_db3.set_database("failover_test_3");

    ds::Multi_source ms;
    /* Add a failing source, just for fun */
    ms.add(ds_error, options, highest_priority);

    /* Add sources with different databases to test random pick */
    ms.add(ds_correct, options, highest_priority - 1);
    ms.add(ds_correct, options_db1, highest_priority - 1);
    ms.add(ds_correct, options_db2, highest_priority - 1);
    ms.add(ds_correct, options_db3, highest_priority - 1);

#ifndef _WIN32
    ds::Unix_socket ds_correct_unix("/tmp/varxpl/tmp/mysqlx.1.sock");
    ds::Unix_socket::Options options_unix_db1("root");
    options_db1.set_database("failover_test_unix_1");

    ds::Unix_socket::Options options_unix_db2("root");
    options_db2.set_database("failover_test_unix_2");

    ds::Unix_socket::Options options_unix_db3("root");
    options_db3.set_database("failover_test_unix_3");

    ms.add(ds_correct_unix, options_unix_db1, highest_priority - 1);
    ms.add(ds_correct_unix, options_unix_db2, highest_priority - 1);
    ms.add(ds_correct_unix, options_unix_db3, highest_priority - 1);

#endif

    struct : cdk::Row_processor
    {
      // Row_processor callbacks

      std::string m_db_name;

      virtual bool row_begin(row_count_t row)
      {
        return true;
      }
      virtual void row_end(row_count_t row)
      {}

      virtual void field_null(col_count_t pos)
      {}

      virtual size_t field_begin(col_count_t pos, size_t)
      {
        return  SIZE_MAX;
      }

      size_t field_data(col_count_t pos, bytes data)
      {
        EXPECT_EQ(0, pos);

        // We expect string with current schema name

        cdk::foundation::Codec<cdk::foundation::Type::STRING> codec;
        cdk::string db;

        // Trim trailing \0
        bytes d1(data.begin(), data.end() - 1);
        codec.from_bytes(d1, db);

        cout << "current schema: " << db << endl;
        m_db_name = db;

        return 0;
      }

      virtual void field_end(col_count_t)
      {}

      virtual void end_of_data()
      {}

      std::string get_db_name() { return m_db_name; }
    }
    prc;

    std::string cur_db = "";
    int different_source = -1;

    for (int i = 0; i < 10; ++i)
    {
      cdk::Session s(ms);
      Reply r(s.sql("SELECT DATABASE()"));
      r.wait();
      Cursor c(r);
      c.get_rows(prc);
      c.wait();

      if (cur_db.compare(prc.get_db_name()))
        ++different_source;
      cur_db = prc.get_db_name();
    }

    /*
      If database was not changed at least 3 times in 10 connects
      something is surely not right
    */
    if (different_source < 3)
      FAIL() << "Failed to connect to a random data source";

  }
  catch (Error &e)
  {
    FAIL() << "CDK error: " << e << endl;
  }

}


TEST_F(Session_core, auth_method)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {

    Session sess(this);
    {
      Reply r(sess.sql("CREATE SCHEMA IF NOT EXISTS auth_test_db"));
      r.wait();
      if (r.entry_count()) FAIL() << "Error creating schema";
    }

    using cdk::ds::mysqlx::Protocol_options;
    ds::TCPIP ds(m_host, m_port);
    ds::TCPIP::Options options("root");
    options.set_database("auth_test_db");

    struct : cdk::Row_processor
    {
      std::string m_db_name;
      // Row_processor callbacks
      virtual bool row_begin(row_count_t row) { return true; }
      virtual void row_end(row_count_t row) {}
      virtual void field_null(col_count_t pos) {}
      virtual size_t field_begin(col_count_t pos, size_t) { return  SIZE_MAX; }

      size_t field_data(col_count_t pos, bytes data)
      {
        EXPECT_EQ(0, pos);
        cdk::foundation::Codec<cdk::foundation::Type::STRING> codec;
        cdk::string db;
        // Trim trailing \0
        bytes d1(data.begin(), data.end() - 1);
        codec.from_bytes(d1, db);
        cout << "current schema: " << db << endl;
        m_db_name = db;
        return 0;
      }

      virtual void field_end(col_count_t) {}
      virtual void end_of_data() {}
      std::string get_db_name() { return m_db_name; }
    }
    prc;

    for (int i = 0; i < 2; ++i)
    {
      switch (i)
      {
      case 0:
        options.set_auth_method(Protocol_options::MYSQL41);
        break;
      case 1:
        options.set_auth_method(Protocol_options::PLAIN);
        break;
      }
      cdk::Session s(ds, options);
      if (!s.is_valid())
        FAIL() << "Session is not valid";

      Reply r(s.sql("SELECT DATABASE()"));
      r.wait();
      Cursor c(r);
      c.get_rows(prc);
      c.wait();

      if (prc.m_db_name.compare("auth_test_db"))
        FAIL() << "Unexpected database name";
    }

  }
  catch (Error &e)
  {
    FAIL() << "CDK error: " << e << endl;
  }

}

TEST_F(Session_core, external_auth)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {

    Session sess(this);
    {
      Reply r(sess.sql("CREATE SCHEMA IF NOT EXISTS auth_test_db"));
      r.wait();
      if (r.entry_count()) FAIL() << "Error creating schema";
    }

    using cdk::ds::mysqlx::Protocol_options;
    ds::TCPIP ds(m_host, m_port);
    ds::TCPIP::Options options("root");
    options.set_database("auth_test_db");
    options.set_auth_method(Protocol_options::EXTERNAL);

    cdk::Session s(ds, options);
    if (s.is_valid())
      FAIL() << "Session is not supposed to be valid";
  }
  catch (Error &e)
  {
    FAIL() << "CDK error: " << e << endl;
  }

}

