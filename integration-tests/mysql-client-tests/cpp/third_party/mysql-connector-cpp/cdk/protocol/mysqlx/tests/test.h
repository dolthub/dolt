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

#ifndef MYSQL_CDK_PROTOCOL_MYSQLX_TESTS_TEST_H
#define MYSQL_CDK_PROTOCOL_MYSQLX_TESTS_TEST_H

/*
  To use getenv() on Windows, which warns that it is not safe
*/
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <gtest/gtest.h>
#include <xplugin_test.h>

// required by ../protocol.h

#ifdef _WIN32
  #if _WIN32_WINNT < 0x0600
    #undef _WIN32_WINNT
    #define _WIN32_WINNT 0x0600
  #endif
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #include <Windows.h>
#endif

#include <process_launcher.h>
#include <exception.h>
#include <iostream>

#include <mysql/cdk.h>
#include <mysql/cdk/protocol/mysqlx.h>
#include <auth_mysql41.h>


#include "../protocol.h"

/**
  Common infrastructure for mysqlx protocol tests.
*/


namespace cdk {
namespace test {

using namespace ::std;
using namespace cdk::foundation;
using namespace cdk::protocol::mysqlx;

// Disambiguate these types

using cdk::protocol::mysqlx::collation_id_t;
using cdk::protocol::mysqlx::row_count_t;

typedef foundation::Number_codec<Endianess::NATIVE> Number_codec;

/*
  Fixture which creates mysqlx protocol session over a connection
  to xplugin. Uses Xplugin fixture which sets up a connection with
  xplugin using env variables XPLUGIN_PORT.

  FIXME: (MYC-136) This test is disabled because it uses PLAIN
  authentication which requires SSL connection.
*/

class Protocol_mysqlx_xplugin : public Xplugin
{

protected:

  Protocol *m_proto;
  bool      m_sess;

  virtual void SetUp()
  {
    m_sess= false;
    m_proto= NULL;

    Xplugin::SetUp();

    if (has_xplugin())
      m_proto= new Protocol(get_conn());
  }

  virtual void TearDown()
  {
    if (m_proto && m_sess)
      m_proto->snd_Close();
    Xplugin::TearDown();
  }

  void authenticate(const char *usr, const char *pwd);
  void do_query(const string &query);

  Protocol& get_proto()
  {
    if (!m_proto)
      cdk::throw_error("no protocol instance");
    return *m_proto;
  }

  public:

  bool is_server_version_less(int test_upper_version,
                              int test_lower_version,
                              int test_release_version);

};


/*
  Handler template which adds definition of error() callback
  to a base processor interface.
*/

template <class X>
class Handler: public X
{
protected:

  typedef Handler<X> Base;

  const std::string prefix;

  Handler(const std::string &_prefix)
    : prefix(_prefix)
  {}

  void error(unsigned int code, short int /*severity*/,
             sql_state_t,
             const cdk::string &msg)
  {
    FAIL() <<prefix <<": Server error: " <<msg <<" (" <<code <<")";
  }

  void message_received(size_t size)
  {
    cout <<"- message of type " <<(unsigned)Processor_base::m_type
        <<" (" <<size <<" bytes)" <<endl;
  }
};


/*
  Processor used during authentication handshake.
*/

class Auth_handler
  : public Handler<cdk::protocol::mysqlx::Auth_processor>
{
public:

  std::string &m_user;
  std::string *m_pass;
  std::string m_cont_data;

  Auth_handler(std::string &user, std::string *pass)
    : Base("Auth handshake")
    , m_user(user)
    , m_pass(pass)
  {}

  void auth_ok(bytes data)
  {
    cout <<"Session accepted: " <<std::string(data.begin(), data.end()) <<endl;
  }

  void auth_fail(bytes data)
  {
    FAIL() <<"Session refused: " <<std::string(data.begin(), data.end());
  }

  void auth_continue(bytes data)
  {
    std::string pass;
    if (m_pass)
      pass = *m_pass;

    m_cont_data = ::mysqlx::build_mysql41_authentication_response(std::string(data.begin(), data.end()),
                                                                  m_user,
                                                                  pass,
                                                                  "");

  }

};


/*
  Processor for server replies during statement execution.
*/

class Stmt_handler
  : public Handler<cdk::protocol::mysqlx::Stmt_processor>
{
private:

  int m_rows_check_num;

public:

  Stmt_handler() : Base("Stmt reply"), m_rows_check_num(-1)
  {}

  void prepare_ok() { cout <<"Statement prepared OK" <<endl; }
  void execute_ok() { cout <<"Statement executed OK" <<endl; }
  void stmt_close_ok() { cout <<"Statement closed" <<endl; }
  void cursor_close_ok() { cout <<"Cursor closed" <<endl; }

  /*
    TODO: Affected rows and last insert id are now reported
    through notices. Refactor m_rows_check_num test accordingly.
  */

#if 0
  void rows_affected(row_count_t rows)
  {
    if (m_rows_check_num >= 0)
      EXPECT_EQ((row_count_t)m_rows_check_num, rows);

    cout <<"Rows affected: " <<rows <<endl;
  }
  void last_insert_id(row_count_t id) { cout <<"Last insert id: " <<id <<endl; }
#endif

  void error(unsigned int code, short int severity, sql_state_t state,
            const cdk::string &msg)
  {
    // Error 1235 = "Not yet supported"

    if (code == 1235) {
      wcout <<"Expected server error: " <<msg <<endl;
      return;
    }

    Handler::error(code, severity, state, msg);
  }

  void set_rows_check_num(int n) { m_rows_check_num = n; }

};



class Mdata_handler
  : public Handler<cdk::protocol::mysqlx::Mdata_processor>
{
public:

  col_count_t m_col_count;

  Mdata_handler()
    : Base("Meta-data")
    , m_col_count(0)
  {}

  virtual void col_count(col_count_t cnt)
  {
    m_col_count = cnt;
    cout <<"Column count: " <<cnt <<endl;
  }

  virtual void col_type(col_count_t pos, unsigned short type)
  { cout <<"Column #" <<pos <<" type: " <<type <<endl; }

  virtual void col_name(col_count_t pos,
                        const cdk::string &name, const cdk::string &original)
  {
    cout <<"Column #" <<pos <<" name: " <<name
         <<" (" <<original <<")" <<endl;
  }

  virtual void col_table(col_count_t pos,
                         const string &table, const string &original)
  {
    cout <<"Column #" <<pos <<" table: " <<table
         <<" (" <<original <<")" <<endl;
  }

  virtual void col_schema(col_count_t pos,
                          const string &schema, const string &catalog)
  {
    cout <<"Column #" <<pos <<" schema: " <<catalog <<"." <<schema
         <<endl;
  }

  virtual void col_charset(col_count_t pos, collation_id_t cs)
  { cout <<"Column #" <<pos <<" charset: " <<cs <<endl; }

  virtual void col_decimals(col_count_t pos, unsigned short decimals)
  { cout <<"Column #" <<pos <<" decimals: " <<decimals <<endl; }

  virtual void col_content_type(col_count_t pos, unsigned short type)
  { cout <<"Column #" <<pos <<" content type: " <<type <<endl; }
};


class Row_handler
  : public Handler<cdk::protocol::mysqlx::Row_processor>
{
protected:
  byte buf[128];
  size_t pos;
  row_count_t m_row_num;

public:

  Row_handler() : Base("Rows"), m_row_num(0)
  {}

  virtual bool row_begin(row_count_t row)
  {
    m_row_num = row;
    cout <<"== begin row #" <<row <<endl;
    return true;
  }
  virtual void row_end(row_count_t row)
  {
    cout <<"== end row #" <<row <<endl;
  }

  virtual void col_null(col_count_t col)
  {
    cout <<" - col#" <<col <<": NULL" <<endl;
  }

  virtual size_t col_begin(col_count_t col, size_t data_len)
  {
    cout <<" - col#" <<col <<" (" <<data_len <<" bytes): ";
    pos= 0;
    return sizeof(buf);
  }

  virtual size_t col_data(col_count_t, bytes data)
  {
    //cout <<"... received " <<data.size() <<" bytes for column " <<col <<endl;
    memcpy(buf+pos, data.begin(), data.size());
    pos += data.size();
    return sizeof(buf) - pos;
  }

  virtual void col_end(col_count_t, size_t)
  {
    for (unsigned i=0; i < 8 && i < pos; ++i)
      cout <<hex <<(unsigned)buf[i] <<" ";
    cout <<dec <<endl;
  }

  virtual void done(bool eod, bool more)
  {
    cout <<"==== end of rows";
    if (eod)
      cout <<" (all rows fetched)";
    cout <<endl;
    if (more)
      cout <<"more result sets follow" <<endl;
    else
      cout <<"no more result sets" <<endl;
    m_row_num = 0;
  }

};


// TODO: Authenticate using MYSQL41 mechanism (PLAIN is not going to work soon).

inline
void Protocol_mysqlx_xplugin::authenticate(const char *usr =NULL, const char *pwd = NULL)
{
  using cdk::byte;

  if (m_sess)
    return;

  Protocol &proto= get_proto();

  cout <<"=== Authentication handshake ===" <<endl;

  if (NULL == usr)
    usr = getenv("XPLUGIN_USER");
  if (NULL == pwd)
    pwd = getenv("XPLUGIN_PASSWORD");

  std::string xplugin_usr = usr ? usr : "root";
  std::string xplugin_pwd = pwd ? pwd : "";

  cout <<"Authenticating as user: " <<xplugin_usr <<endl;
  cout <<"Password: " <<(xplugin_pwd.empty() ? "<none>" : xplugin_pwd) <<endl;

  cout <<"initial message..." <<endl;

  proto.snd_AuthenticateStart("MYSQL41",
                              bytes((byte*)NULL, (size_t)0),
                              bytes((byte*)NULL, (size_t)0)).wait();

  Auth_handler ah(xplugin_usr, &xplugin_pwd);
  proto.rcv_AuthenticateReply(ah).wait();

  proto.snd_AuthenticateContinue(ah.m_cont_data).wait();

  proto.rcv_AuthenticateReply(ah).wait();

  m_sess= true;
}

/*
  This function is only for queries that do not produce result-sets
*/
inline
void Protocol_mysqlx_xplugin::do_query(const string &query)
{
  Protocol &proto= get_proto();

  cout << "=== Execute query === [" << query << "]";
  proto.snd_StmtExecute("sql", query, NULL).wait();

  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  if (mdh.m_col_count != 0)
  {
    Row_handler rhc;
    proto.rcv_Rows(rhc).wait();
  }

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();
}


/*
  Check if the server version is less than required
*/
inline
bool Protocol_mysqlx_xplugin::is_server_version_less(int test_upper_version,
                                                     int test_lower_version,
                                                     int test_release_version)
{
  Protocol &proto= get_proto();
  proto.snd_StmtExecute("sql", "SELECT VERSION()", NULL).wait();

  Mdata_handler mdh;
  proto.rcv_MetaData(mdh).wait();

  class Row_handler_data : public Row_handler
  {
    public:
    char *get_data() { return (char*)buf; }
    size_t get_size() { return pos; }
  };

  Row_handler_data rhd;
  proto.rcv_Rows(rhd).wait();

  Stmt_handler sh;
  proto.rcv_StmtReply(sh).wait();

  std::stringstream version;
  version << std::string(rhd.get_data(), rhd.get_size());

  int upper_version, minor_version, release_version;
  char sep;
  version >> upper_version;
  version >> sep;
  version >> minor_version;
  version >> sep;
  version >> release_version;

  if ((upper_version < test_upper_version) ||
    (upper_version == test_upper_version &&
     minor_version << test_lower_version) ||
     (upper_version == test_upper_version &&
      minor_version == test_lower_version &&
      release_version < test_release_version))
  {
    return true;
  }
  return false;

}


#ifdef TODO

/*
  Base class for defining Row_source classes. Expression which defines value
  of column 'col' in row 'row' is described by a call to
  process_col(row, col, prc) where prc is an expression processor. Derived
  class should also implement count() method of Expr_list to define number
  of columns in each row.
*/

class Row_source_base
  : public protocol::mysqlx::Row_source
  , public protocol::mysqlx::api::Expression
{
  row_count_t m_row_count;
  row_count_t m_row;
  col_count_t m_col;

public:

  typedef protocol::mysqlx::api::Expression Expression;
  typedef Expression::Processor  Processor;

  Row_source_base(row_count_t rows =1)
    : m_row_count(rows)
    , m_row(0)
  {}

  virtual void process_col(row_count_t row, col_count_t col, Processor &prc) const =0;

  bool next()
  {
    if (m_row == m_row_count)
      return false;
    ++m_row;
    return true;
  }

  Expression& get(col_count_t col) const
  {
    if (col >= count())
      THROW("Column out of range");
    Row_source_base *self= const_cast<Row_source_base*>(this);
    self->m_col= col;
    return *self;
  }

  void process(Processor &ep) const
  {
    process_col(m_row-1, m_col, ep);
  }
};

#endif

/*
  Test server which parses incoming messages and passes them to a processor.
  It uses in-memory connection for communication with client.
*/

using namespace ::cdk::protocol::mysqlx;


/*
  Processor that handles parsed protobuf messages received by test server.
*/

class Msg_processor : public Processor_base
{
public:
  virtual void process_msg(msg_type_t, Message&) =0;
};


class Test_server_base : public Protocol_impl
{
  class Rcv_op;

public:

  Test_server_base(Protocol::Stream *str, Protocol_side side)
    : Protocol_impl(str, side)
  {}

  void rcv_msg(Msg_processor &prc)
  {
    rcv_start<Rcv_op>(prc).wait();
  }
};


template <size_t SIZE>
class Test_server : public Test_server_base
{
  typedef cdk::foundation::test::Mem_stream<SIZE> Stream;
  Stream m_conn;

public:

  Test_server(Protocol_side side =CLIENT);

  Stream& get_connection() { return m_conn; }
  void reset() { m_conn.reset(); }
};



template <class C>
class Test_stream : public Protocol::Stream
{
  typedef typename C::Read_op  Rd_op;
  typedef typename C::Write_op Wr_op;

  C &m_conn;

public:

  Test_stream(C &conn) : m_conn(conn)
  {}

  Op* read(const buffers &buf)
  { return new Rd_op(m_conn, buf); }

  Op* write(const buffers &buf)
  { return new Wr_op(m_conn, buf); }
};


template <size_t S>
inline
Test_server<S>::Test_server(Protocol_side side)
  : Test_server_base(new Test_stream<Stream>(m_conn), side)
{}




class Test_server_base::Rcv_op : public Op_rcv
{
public:

  Rcv_op(Protocol_impl &proto) : Op_rcv(proto)
  {}

  void process_msg(msg_type_t type, Message &msg)
  {
    static_cast<Msg_processor*>(m_prc)->process_msg(type, msg);
  }

  void resume(Msg_processor &prc)
  {
    read_msg(prc);
  }

  Next_msg next_msg(msg_type_t)
  { return EXPECTED; }
};

#define SKIP_IF_SERVER_VERSION_LESS(x,y,z)\
  if (Protocol_mysqlx_xplugin::is_server_version_less(x, y, z)) \
  {\
    std::cerr <<"SKIPPED: " << \
    "Server version not supported (" \
    << x << "." << y <<"." << ")" << z <<std::endl; \
    return; \
  }


}}  // cdk::test

#endif
