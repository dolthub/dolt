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

/*
  To use getenv() on Windows, which warns that it is not safe
*/

#undef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS

#include <iostream>
#include <mysql/cdk.h>
#include <mysql/cdk/mysqlx.h>

#include "xplugin_test.h"
#include "session_test.h"

using namespace ::std;
using namespace cdk::foundation;


class Session_mysqlx
    : public cdk::test::Xplugin
    , public cdk::test::Row_processor<cdk::mysqlx::Cursor>
{
public:


  template<cdk::Type_info TI, typename T>
  void print_test(cdk::Codec<TI> &codec, bytes data)
  {
    T val = 0;
    size_t sz = codec.from_bytes(data, val);
    char* buffer  = new char[sz];
    bytes data2((byte*)buffer, sz);
    codec.to_bytes(val, data2);
    int rc = memcmp(data.begin(), data2.begin(), sz);
    delete[] buffer;
    if (rc != 0)
    {
      throw "Diff data generated!";
    }
    cout << val;
  }

#if 0

  /*
    TODO: These methods were never calles because virtual methods in
    cdk::test::Row_processor<> have different signatures. Either
    remove them or fix them so that they are really called on result
    data.
  */

  virtual void process_field_val(col_count_t pos, bytes data,
                                 cdk::Type_info type, const cdk::Format_info &fi,
                                 uint64_t val)
  {
    EXPECT_EQ(cdk::TYPE_INTEGER, type);
    cdk::Codec<cdk::TYPE_INTEGER> codec(fi);
    print_test<cdk::TYPE_INTEGER, uint64_t>(codec, data);
  }

  virtual void process_field_val(col_count_t pos, bytes data,
                                 cdk::Type_info type, const cdk::Format_info &fi,
                                 int64_t val)
  {
    EXPECT_EQ(cdk::TYPE_INTEGER, type);
    cdk::Codec<cdk::TYPE_INTEGER> codec(fi);
    print_test<cdk::TYPE_INTEGER, int64_t>(codec, data);
  }

  virtual void process_field_val(col_count_t pos, bytes data,
                                 cdk::Type_info type, const cdk::Format_info &fi,
                                 double val)
  {
    EXPECT_EQ(cdk::TYPE_FLOAT, type);
    cdk::Codec<cdk::TYPE_FLOAT> codec(fi);
    cdk::Format<cdk::TYPE_FLOAT> fmt(fi);

    if (fmt.type() == cdk::Format<cdk::TYPE_FLOAT>::FLOAT)
      print_test<cdk::TYPE_FLOAT, float>(codec, data);
    else
      print_test<cdk::TYPE_FLOAT, double>(codec, data);
  }

#endif

};


/*
  Basic test that connects to the test server, sends a message and
  reads server's reply.

  Note: Test server should be started before running this test.
*/


TEST_F(Session_mysqlx, basic)
{
  SKIP_IF_NO_XPLUGIN;

  try
  {

    cdk::ds::TCPIP::Options options;
    cdk::mysqlx::Session s1(get_conn(), options);


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
  catch (std::exception &e)
  {
    FAIL() << "Connection error: " << e.what() << endl;
  }
  catch (const char *e)
  {
    FAIL() << "EXCEPTION: " << e << endl;
  }

}


TEST_F(Session_mysqlx, error_on_connect)
{
  SKIP_IF_NO_XPLUGIN;

  try {

    cdk::ds::TCPIP ds("localhost", m_port+1);
    cdk::ds::TCPIP::Options options;
    cdk::connection::TCPIP conn(ds.host(), ds.port());
    conn.connect();
    cdk::mysqlx::Session s1(conn, options);

    if (!s1.is_valid())
      FAIL() << "Invalid Session created";

  }
  catch (Error &e)
  {
    cout << "Connection error: " << e << endl;
  }
  catch (...)
  {
    FAIL() << "Invalid Error Thrown";
  }

}


//TODO add test with errors (change mockup to do that - auth error, for example)
TEST_F(Session_mysqlx, auth_error)
{
  SKIP_IF_NO_XPLUGIN;

  try {

    cdk::string user = "bad_user";
    std::string passwd = "bad_password";

    cdk::ds::TCPIP::Options options(user,&passwd);
    cdk::mysqlx::Session s1(get_conn(), options);

    if (s1.is_valid())
      FAIL() << "Session created with invalid auth data" << endl;

    if (s1.entry_count() != 1)
      FAIL() << "Error number different from expected" << endl;

    cout << s1.get_error() << endl;

    for (cdk::Session::Iterator &it = s1.get_entries(); it.next();)
    {
      cout << it.entry() << endl;
    }

  }
  catch (Error &e)
  {
    FAIL() << "Session error: " << e << endl;
  }

}


TEST_F(Session_mysqlx,sql_basic)
{
  try {
    SKIP_IF_NO_XPLUGIN;



    cdk::ds::TCPIP::Options options;
    cdk::mysqlx::Session s(get_conn(), options);


    if (!s.is_valid())
      FAIL() << "Invalid Session!";


    // No results
    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("select * from mysql.user where invalid query :) ;", NULL);
      EXPECT_FALSE(rp.has_results());
    }

    for (int i = 0; i >=0 ;i++)
    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("select * from mysql.user;", NULL);

      switch (i)
      {

      case 0:
        cout <<endl <<"== Case 0 ==" <<endl;
        {
          /*
             Normal results treatment
          */

          EXPECT_TRUE(rp.has_results());

          cdk::mysqlx::Cursor cr(rp);

          set_meta_data(cr);
          cr.get_rows(*this);
          cr.wait();

          EXPECT_FALSE(rp.has_results());

          try
          {
            cr.close();
            cr.get_rows(*this);
            FAIL() << "Expected Cursor Closed";
          }
          catch(...) //const cdk::Error& e)
          {
            cout << "Expected CDK Error: " <<endl; //<< e << std::endl;
          }

        }
        break;

      case 1:
        cout <<endl <<"== Case 1 ==" <<endl;
        {
          /*
             Second attempt to read cursor without results
          */

          EXPECT_TRUE(rp.has_results());

          cdk::mysqlx::Cursor cr(rp);
          cr.close();

          EXPECT_FALSE(rp.has_results());

          try
          {
            cdk::mysqlx::Cursor cr2(rp);
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
            cdk::mysqlx::Cursor c2(rp);
            FAIL() << "No exception thrown";
          }
          catch(...) //const cdk::Error& e)
          {
            cout << "Expected CDK Error: " <<endl; //<< e << std::endl;
          }

        }
        break;

      case 3:
        cout <<endl <<"== Case 3 ==" <<endl;
        {
          /*
             Skip Result cannot be done when cursor exists
          */

          cdk::mysqlx::Cursor cr(rp);

          try
          {
            rp.skip_result();

            FAIL() << "No exception thrown";
          }
          catch(...) //const cdk::Error& e)
          {
            cout << "Expected CDK Error: " <<endl; //<< e << std::endl;
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
            cdk::mysqlx::Cursor c2(rp);
            FAIL() << "No exception thrown";
          }
          catch(...) //const cdk::Error& e)
          {
            cout << "Expected CDK Error: " <<endl; //<< e << std::endl;
          }
        }
        break;

      case 5:
        cout <<endl <<"== Case 5 ==" <<endl;
        {
          /*
             Discard Result cannot be done when cursor exists
          */

          cdk::mysqlx::Cursor cr(rp);

          try
          {
            rp.discard();

            FAIL() << "No exception thrown";
          }
          catch(...) //const cdk::Error& e)
          {
            cout << "Expected CDK Error: " <<endl; //<< e << std::endl;
          }
        }
        break;

      case 6:
        cout <<endl <<"== Case 6 ==" <<endl;

        rp = s.sql("drop database if exists xpto", NULL);
        EXPECT_FALSE(rp.has_results());

        rp = s.sql("create database xpto", NULL);
        rp = s.sql("drop table if exists xpto.table_test", NULL);
        rp = s.sql("create table xpto.table_test (id int unsigned not null)", NULL);

        rp = s.sql("insert into xpto.table_test ( id ) values(1),(2),(3)", NULL);

        EXPECT_FALSE(rp.has_results());
        rp.discard();

        /*
         TODO: Enable when affected_rows info is handled correctly (it is sent
         as notices, not in Ok packet).

        EXPECT_EQ(3U, rp.affected_rows());
        */

        break;

      case 7:
        cout <<endl <<"== Case 7 ==" <<endl;
        {
          cdk::mysqlx::Cursor cr(rp);

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

      case 8:
        cout <<endl <<"== Case 8 ==" <<endl;
        {

          /*
             Using get_rows()
          */
          cdk::mysqlx::Cursor cr(rp);

          set_meta_data(cr);
          cr.get_rows(*this);
          cr.wait();
          EXPECT_FALSE(cr.get_row(*this));

        }
        break;


      case 9:
        cout <<endl <<"== Case 9 ==" <<endl;
        {

          /*
             Using get_row()
          */
          cdk::mysqlx::Cursor cr(rp);

          set_meta_data(cr);
          while(cr.get_row(*this))
          {
          std::cout << "New Line!" << std::endl;
          }
          cr.close();

          try {
            cdk::mysqlx::Cursor cr2(rp);
            FAIL() << "No results to process";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }

        }
        break;

      case 10:
        cout <<endl <<"== Case 10 ==" <<endl;
        {

          /*
             Using get_row()
          */
          cdk::mysqlx::Cursor cr(rp);

          set_meta_data(cr);
          EXPECT_TRUE(cr.get_row(*this));
          std::cout << "New Line!" << std::endl;
          EXPECT_TRUE(cr.get_row(*this));
          std::cout << "New Line!" << std::endl;

          try {
            rp.affected_rows();
            FAIL() << "Affected Rows before end of cursor";
          }
          catch(const cdk::Error& e)
          {
            cout << "Expected CDK Error: " << e << std::endl;
          }


          //The rest is discarded

        }
        break;

      /*
        TODO: fix this test case
      case 11:
        cout <<endl <<"== Case 11 ==" <<endl;
        {
          rp = s.sql(cdk::string(L"SELECT 1.2 as float_dec, 27182818284590452353602872e-25 as test_float, -2718281828 as big_int_neg, CAST(1-2 AS UNSIGNED) as big_uint "));

          cdk::mysqlx::Cursor cr(rp);
          set_cursor(cr);
          cr.get_rows(*this);
          cr.wait();
        }
        break;
      */

      default:
        //break;
        i = -2;
        break;
      }
    }

    cout <<endl <<"== Diagnostics ==" <<endl;
    //TODO: Where should erros on queries stay?
    //      Since processors are all on

    for (cdk::mysqlx::Session::Diagnostics::Iterator &it= s.get_entries();
         it.next();)
    {
      FAIL() << it.entry().description();
    }


    cout <<"Done!" <<endl;


  }
  CATCH_TEST_GENERIC
}


template <typename GetType, typename TestType, cdk::Type_info TI>
class PrintCompareType
    : public cdk::mysqlx::Row_processor

{
public:
  PrintCompareType(TestType val, size_t size, cdk::mysqlx::Cursor &cursor)
    : m_val(val)
    , m_size_t(size)
    , m_cursor(cursor)
    , m_codec(m_cursor.format(0))
  {

  }

  TestType m_val;
  size_t m_size_t;
  cdk::mysqlx::Cursor &m_cursor;
  cdk::Codec<TI> m_codec;

  /*
     cdk::mysqlx::Row_processor
  */

  virtual bool row_begin(row_count_t row)
  {
    std::cout << "Process Row Begin: "
              << row
              << std::endl;
    return true;
  }
  virtual void row_end(row_count_t row)
  {
    std::cout << "Process Row End: "
              << row
              << std::endl;
  }

  virtual void field_null(col_count_t /*pos*/)
  {
    std::cout << "Null";
  }



  virtual size_t field_begin(col_count_t pos, size_t)
  {
    const cdk::mysqlx::Column_ref &column = m_cursor.col_info(pos);
    const cdk::mysqlx::Table_ref* table = column.table();
    if (table)
    {
      const cdk::mysqlx::Schema_ref* schema = table->schema();
      if (schema)
      {
        if (schema->catalog())
        {
          std::cout << schema->catalog()->name()
                    << ".";
        }
        std::cout << schema->name()
                  << ".";
      }
      std::cout << table->name()
                << ".";
    }

    std::cout << column.name();

    std::cout << ": ";

    return  SIZE_MAX;
  }


  size_t field_data(col_count_t /*pos*/, bytes data)
  {
    GetType out;

    m_codec.from_bytes(data, out);

    wcout << m_val << "=" <<out <<endl;

    EXPECT_EQ(out, m_val);

    char* buffer  = new char[m_size_t];
    bytes data2((byte*)buffer, m_size_t);

    m_codec.to_bytes(out, data2);

    EXPECT_EQ(0, memcmp(data.begin(), data2.begin(), data.size()));

    return 0;
  }

  virtual void field_end(col_count_t /*pos*/)
  {
    std::cout << std::endl;
  }

  virtual void end_of_data()
  {
    std::cout << "DONE" << std::endl;
  }
};


TEST_F(Session_mysqlx,sql_type_conv)
{
  try {
    SKIP_IF_NO_XPLUGIN;

    cdk::ds::TCPIP::Options options;
    cdk::mysqlx::Session s(get_conn(), options);

    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("SELECT 27182818284590452353602872e-25 as test_float", NULL);
      cdk::mysqlx::Cursor cr(rp);

      PrintCompareType<double,double,cdk::TYPE_FLOAT> pt(27182818284590452353602872e-25, sizeof(double), cr);

      cr.get_rows(pt);
      cr.wait();
    }

    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("SELECT -2718281828 as big_int_neg", NULL);
      cdk::mysqlx::Cursor cr(rp);

      PrintCompareType<int64_t, int64_t, cdk::TYPE_INTEGER> pt(-2718281828LL,sizeof(uint64_t)*8, cr);

      cr.get_rows(pt);
      cr.wait();
    }

    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("SELECT CAST(-1 AS UNSIGNED) as big_uint ", NULL);
      cdk::mysqlx::Cursor cr(rp);

      PrintCompareType<uint64_t, uint64_t,cdk::TYPE_INTEGER> pt(-1, sizeof(uint64_t)*8, cr);

      cr.get_rows(pt);
      cr.wait();
    }


    /*
     * Overflow Tests
     */

    //double to float overflow
    try
    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("SELECT 27182818284590452353602872e-25 as test_float", NULL);
      cdk::mysqlx::Cursor cr(rp);


      PrintCompareType<float,double,cdk::TYPE_FLOAT> pt(27182818284590452353602872e-25, sizeof(float), cr);

      cr.get_rows(pt);
      cr.wait();

      FAIL() << "Exception expected";
    }
    catch (Error &e)
    {
      cout << e << endl;
      EXPECT_EQ(cdkerrc::conversion_error, e.code());
    }

    //uint32 to uint64 overflow
    try
    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("SELECT CAST(-1 AS UNSIGNED) as big_uint ", NULL);
      cdk::mysqlx::Cursor cr(rp);

      PrintCompareType<uint32_t, uint64_t,cdk::TYPE_INTEGER> pt(-1, sizeof(uint64_t)*8, cr);

      cr.get_rows(pt);
      cr.wait();

      FAIL() << "Exception expected";
    }
    catch (Error &e)
    {
      cout << e << endl;
      EXPECT_EQ(cdkerrc::conversion_error, e.code());
    }

    //destination buffer too small
    try
    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("SELECT 27182818284590452353602872e-25 as test_float", NULL);
      cdk::mysqlx::Cursor cr(rp);


      PrintCompareType<double,double,cdk::TYPE_FLOAT> pt(27182818284590452353602872e-25, sizeof(float), cr);

      cr.get_rows(pt);
      cr.wait();

      FAIL() << "Exception expected";
    }
    catch (Error &e)
    {
      cout << e << endl;
      EXPECT_EQ(cdkerrc::conversion_error, e.code());
    }

    //destination buffer too small
    try
    {
      cdk::mysqlx::Reply rp;
      rp = s.sql("SELECT CAST(-1 AS UNSIGNED) as big_uint ", NULL);
      cdk::mysqlx::Cursor cr(rp);

      PrintCompareType<uint64_t, uint64_t,cdk::TYPE_INTEGER> pt(-1, sizeof(uint64_t), cr);

      cr.get_rows(pt);
      cr.wait();

      FAIL() << "Exception expected";
    }
    catch (Error &e)
    {
      cout << e << endl;
      EXPECT_EQ(cdkerrc::conversion_error, e.code());
    }


  }
  CATCH_TEST_GENERIC
}
