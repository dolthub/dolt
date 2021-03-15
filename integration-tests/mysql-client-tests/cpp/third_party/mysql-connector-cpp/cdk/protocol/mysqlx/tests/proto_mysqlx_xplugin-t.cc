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
  Test mysqlx::Protocol class against xplugin.
*/


#include "test.h"


namespace cdk {
namespace test {


TEST_F(Protocol_mysqlx_xplugin, basic)
{
  SKIP_IF_NO_XPLUGIN;

  using cdk::byte;

  authenticate();
  Protocol &proto= get_proto();

  cout <<"=== Execute query ===" <<endl;
  cdk::string query(L"select -1 as number, \"foo\" as string");
  cout << query <<endl;
  proto.snd_StmtExecute("sql", query, NULL).wait();

  cout <<"Fetch meta-data" <<endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Fetch rows" <<endl;
  Row_handler rh;
  proto.rcv_Rows(rh).wait();

  cout <<"Final OK" <<endl;
  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();

  // Try wrong sequence of receive operations

  cout <<"=== Execute query ===" <<endl;
  cout << query <<endl;
  proto.snd_StmtExecute("sql", query, NULL).wait();

  try {
    proto.rcv_Rows(rh).wait();
    FAIL() << "rcv_Rows() should not work directly after StmtExecute";
  }
  catch (Error &e)
  {
    cout <<"Expected error in rcv_Rows() after StmtExecute: "
         <<e <<endl;
  }
  catch (const char* e)
  {
    cout <<"Expected error in rcv_Rows() after StmtExecute: "
         <<e <<endl;
  }

  // process remaining server reply
  proto.rcv_MetaData(mdh).wait();
  proto.rcv_Rows(rh).wait();
  proto.rcv_StmtReply(sh).wait();

  cout <<"Done!" <<endl;
}


TEST_F(Protocol_mysqlx_xplugin, no_rset)
{
  SKIP_IF_NO_XPLUGIN;

  using cdk::byte;

  authenticate();
  Protocol &proto= get_proto();

  cout <<"=== Execute query ===" <<endl;
  cout <<"set @foo = 1" <<endl;
  proto.snd_StmtExecute("sql", L"set @foo = 1", NULL).wait();

  cout <<"Fetch meta-data" <<endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Final OK" <<endl;
  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();

  cout <<"=== Execute query ===" <<endl;
  cout <<"set @foo = 2" <<endl;
  proto.snd_StmtExecute("sql", L"set @foo = 2", NULL).wait();

  cout <<"Fetch meta-data" <<endl;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Final OK" <<endl;
  proto.rcv_StmtReply(sh).wait();

  cout <<"Done!" <<endl;
}


class Row_handler_interrupt : public Row_handler
{
private:
  int m_rows_to_fetch;
  row_count_t m_rows_read;

public:
  std::vector<string> row_ids;

  Row_handler_interrupt() : m_rows_to_fetch(0), m_rows_read(0)
  {}

  void set_rows_to_fetch(int n)
  { m_rows_to_fetch = n; }

  virtual size_t col_data(col_count_t col, bytes data)
  {
    memcpy(buf+pos, data.begin(), data.size());
    char local_buf[128];
    pos += data.size();

    /* Use this for making a local string */
    memcpy(local_buf, data.begin(), data.size());
    local_buf[data.size()] = '\0';

    if (col == 0)
    {
      string s1 = row_ids[(unsigned)m_row_num];
      string s2(local_buf);
      EXPECT_EQ(s1.compare(s2) , 0);
    }

    return sizeof(buf) - pos;
  }

  row_count_t get_read_rows_number()
  { return m_rows_read; }


  virtual void row_end(row_count_t row)
  {
    cout <<"== end row #" <<row <<endl;
    m_rows_read = row + 1;

    if (row == (unsigned)(m_rows_to_fetch - 1))
    {
      // signal that processor wants to interrupt reading
      m_rows_to_fetch = -1;
    }
  }

  virtual bool message_end()
  {
    if (m_rows_to_fetch == -1)
    {
      // set back to not initialized
      m_rows_to_fetch = 0;
      m_row_num = 0;
      return false;
    }
    return true;
  }
};


TEST_F(Protocol_mysqlx_xplugin, row_fetch_interrupt)
{
  SKIP_IF_NO_XPLUGIN;

  using cdk::byte;

  authenticate();
  Protocol &proto= get_proto();

  proto.snd_StmtExecute("sql",
    L"SELECT BINARY 'aa' UNION SELECT BINARY 'bb' as number UNION SELECT BINARY 'cc' as number UNION SELECT BINARY 'dd' as number UNION SELECT BINARY 'ee' as number",
    NULL).wait();
  cout <<"Metadata" <<endl;
  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  cout <<"Rows" <<endl;
  Row_handler_interrupt rhi;

  rhi.row_ids.push_back("aa");
  rhi.row_ids.push_back("bb");
  rhi.row_ids.push_back("cc");

  rhi.set_rows_to_fetch(3);

  proto.rcv_Rows(rhi).wait(); // this call will read only 3 rows and then stop

  if (rhi.get_read_rows_number() != 3)
    FAIL();

  cout <<"Reading rows interrupted" <<endl;

  /*
    Row numbers will start from 0 again. So, clearing the list
    and adding new values to check
  */
  rhi.row_ids.clear();
  rhi.row_ids.push_back("dd");
  rhi.row_ids.push_back("ee");

  cout <<"Resuming reading remaing rows" <<endl;

  proto.rcv_Rows(rhi).wait(); // this call will read remaining rows

  if (rhi.get_read_rows_number() != 2)
    FAIL();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();
  cout <<"Done" <<endl;
}

class Expect_test : public cdk::protocol::mysqlx::api::Expectations
{
  void process(Processor &prc) const
  {
    prc.list_begin();
    prc.list_el()->set(NO_ERROR);
    prc.list_end();
  }
};


TEST_F(Protocol_mysqlx_xplugin, expectation)
{
  SKIP_IF_NO_XPLUGIN;
  authenticate();
  SKIP_IF_SERVER_VERSION_LESS(8, 0, 2)

  Protocol &proto = get_proto();
  Expect_test expect;

  //Reply
  Row_handler rh;
  Mdata_handler mdh;

  proto.snd_Expect_Open(expect, false).wait();
  cout << "Expect_Open is sent" << endl;

  struct Expect_prc : cdk::protocol::mysqlx::Reply_processor
  {
    bool has_error = false;

    void error(unsigned int code, short int severity,
               cdk::protocol::mysqlx::sql_state_t sql_state, const string &msg)
    {
      cout << "Error: " << msg << endl;
      has_error = true;
    }

    void ok(string s)
    {
      cout << "OK received. Message: " <<
        (s.empty() ? "(EMPTY)" : s) << endl;
      has_error = false;
    }
  } prc;

  proto.rcv_Reply(prc).wait();

  proto.snd_StmtExecute("sql", "SELECT 1", NULL).wait();
  proto.rcv_MetaData(mdh).wait();
  proto.rcv_Rows(rh).wait();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();

  proto.snd_StmtExecute("sql", "ERROR SQL", NULL).wait();
  prc.has_error = false;
  proto.rcv_Reply(prc).wait();
  EXPECT_EQ(true, prc.has_error);

  // Valid statement will fail because of expectation error
  proto.snd_StmtExecute("sql", "SELECT 2", NULL).wait();
  prc.has_error = false;
  proto.rcv_Reply(prc).wait();
  EXPECT_EQ(true, prc.has_error);

  proto.snd_Expect_Close().wait();
  cout << "Expect_Close is sent" << endl;
  // This will report the failed expectation error, but it is expected
  proto.rcv_Reply(prc).wait();

  // Valid statement should succeed after expectation is closed
  proto.snd_StmtExecute("sql", "SELECT 3", NULL).wait();
  proto.rcv_MetaData(mdh).wait();
  proto.rcv_Rows(rh).wait();
  proto.rcv_StmtReply(sh).wait();

}

struct Expect_fied_exists : public cdk::protocol::mysqlx::api::Expectations
{
  std::string field_data;

  void process(Processor &prc) const
  {
    prc.list_begin();
    prc.list_el()->set(FIELD_EXISTS, field_data.data());
    prc.list_end();
  }
};


TEST_F(Protocol_mysqlx_xplugin, expectation_field)
{
  SKIP_IF_NO_XPLUGIN;
  authenticate();
  SKIP_IF_SERVER_VERSION_LESS(8, 0, 3)

  Protocol &proto = get_proto();
  Expect_fied_exists expect;
  /*
    CRUD_FIND = 17;
    optional RowLock locking = 12;
  */
  expect.field_data = "17.12";

  //Reply
  Row_handler rh;
  Mdata_handler mdh;

  proto.snd_Expect_Open(expect, false).wait();
  cout << "Expect_Open is sent" << endl;

  struct Expect_prc : cdk::protocol::mysqlx::Reply_processor
  {
    void error(unsigned int code, short int severity,
               cdk::protocol::mysqlx::sql_state_t sql_state, const string &msg)
    {
      cout << "Error: " << msg << endl;
    }

    void ok(string s)
    {
      cout << "OK received. Message: " <<
        (s.empty() ? "(EMPTY)" : s) << endl;
    }
  } prc;

  proto.rcv_Reply(prc).wait();

  proto.snd_Expect_Close().wait();
  cout << "Expect_Close is sent" << endl;
  proto.rcv_Reply(prc).wait();
}

}}  // cdk::test

