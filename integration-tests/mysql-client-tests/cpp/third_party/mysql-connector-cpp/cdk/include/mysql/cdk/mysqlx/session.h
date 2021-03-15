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

#ifndef CDK_MYSQLX_SESSION_H
#define CDK_MYSQLX_SESSION_H

#include <mysql/cdk/data_source.h>
#include <mysql/cdk/codec.h>
#include <mysql/cdk/protocol/mysqlx/collations.h>

#include "common.h"

PUSH_SYS_WARNINGS_CDK
#include <deque>
POP_SYS_WARNINGS_CDK

#undef max
using cdk::shared_ptr;

// TODO: CS handling
#define BINARY_CS_ID 63


struct cdk::Format<cdk::TYPE_INTEGER>::Access
{
  typedef cdk::Format<cdk::TYPE_INTEGER> Format;
  static void set_fmt(Format &o, Format::Fmt fmt) { o.m_fmt= fmt; }
  static void set_length(Format &o, size_t len)   { o.m_length= len; }
};


struct cdk::Format<cdk::TYPE_FLOAT>::Access
{
  typedef cdk::Format<cdk::TYPE_FLOAT> Format;
  static void set_fmt(Format &o, Format::Fmt fmt) { o.m_fmt= fmt; }
};


struct cdk::Format<cdk::TYPE_BYTES>::Access
{
  typedef cdk::Format<cdk::TYPE_BYTES> Format;
  static void set_width(Format &o, uint64_t width) { o.m_width= width; }
};


struct cdk::Format<cdk::TYPE_STRING>::Access
{
  typedef cdk::Format<cdk::TYPE_STRING> Format;
  static void set_width(Format &o, uint64_t width) { o.m_width= width; }
  static void set_cs(Format &o, Charset::value cs) { o.m_cs= cs; }
  static void set_kind_set(Format &o) { o.m_kind= Format::SET; }
  static void set_kind_enum(Format &o) { o.m_kind= Format::ENUM; }
};


struct cdk::Format<cdk::TYPE_DATETIME>::Access
{
  //typedef cdk::Format<cdk::TYPE_FLOAT> Format;
  static void set_fmt(Format &o, Format::Fmt fmt, bool has_time)
  {
    o.m_fmt = fmt;
    o.m_has_time = has_time;
  }
};


namespace cdk {
namespace mysqlx {

class Session;
class Cursor;


// ---------------------------------------------------------

/*
  Note: other Session implementations might need to translate genric
  cdk types to something that is specific to the implementation.
*/

using cdk::Row_source;
using cdk::Projection;
using cdk::Limit;
using cdk::Order_by;
using cdk::Sort_direction;
using cdk::Param_source;
using cdk::View_spec;


class Reply;
class Cursor;
class SessionAuth;
class Stmt_op;

typedef Stmt_op* Reply_init;
typedef protocol::mysqlx::api::Protocol_fields Protocol_fields;
typedef protocol::mysqlx::api::Compression_type Compression_type;


/*
  An asynchronous operation which performs authentication.

  It performs authentication rounds, starting with AuthenticateStart
  message and replying to AuthenticateContinue challenges from server
  until server either accepts a session or reports error. The result of
  a completed operation is a Boolean value telling whether session was accepted.
  Errors and notices reported by server during the handshake are stored
  in Session object.

  After completing, the whole process can be restarted (using the same
  credentials) by calling restart() method.

  This class should be specialized to implement concrete authentication
  methods by overriding auth_data() and auth_response() methods.
*/

class SessionAuth
  : public cdk::api::Async_op<bool>
  , protected protocol::mysqlx::Auth_processor
{
public:

  SessionAuth(Session&, const char *method);
  virtual ~SessionAuth() {}

  /*
    Authentication data to be sent in the AuthenticateStart message,
    such as user name and credentials.
  */

  virtual bytes auth_data()  = 0;

  /*
    Response to the challenge in the given round of the authentication
    handshake. This method is also called to produce initial response
    for AuthenticateStart message. In this case round is 0 and challenge
    is empty.
  */

  virtual bytes auth_response(unsigned round, bytes challenge) = 0;

  // Async_op

  void restart();

  bool is_completed() const override;
  const cdk::api::Event_info* get_event_info() const override;

private:

  void do_wait() override;
  bool do_cont() override;
  void do_cancel() override;
  bool do_get_result() override;

  // Auth_processor

  void auth_ok(bytes) override;
  void auth_continue(bytes) override;

  void error(
    unsigned int /*code*/, short int /*severity*/,
    sql_state_t /*sql_state*/, const string &/*msg*/
  ) override;

  void notice(unsigned int /*type*/,
    short int /*scope*/,
    bytes /*payload*/
  ) override;

  // Local state

  Session &m_sess;
  enum { INIT, START, CONT, DONE, ERROR } m_state = INIT;
  Proto_op *m_op   = nullptr;
  unsigned m_round = 0;
  const char *m_am;
};


/*
  Represents active session with a server.

  - is an async. operation which establishes the session when completed.
  - initiates sending server commands via methods such as coll_find().
  - is used by Reply and Cursor objects to send commands to the server and
    process its replies.
*/

class Session
    : public api::Diagnostics
    , public Async_op
    //, private protocol::mysqlx::Auth_processor
    , private protocol::mysqlx::SessionState_processor
    , private protocol::mysqlx::Reply_processor
{

  friend Stmt_op;
  friend Cursor;
  friend SessionAuth;

protected:

  Protocol  m_protocol;
  std::unique_ptr<SessionAuth> m_auth;

  option_t  m_isvalid = false;
  Diagnostic_arena m_da;

  /*
    Pointer to the list of currently active statements registered with
    this session. We point at the last registered statement, that waits
    at all others to complete, because this is where we append new ones
    (see (de)register_stmt() methods).
  */

  Stmt_op* m_last_stmt = nullptr;

  unsigned long m_id = 0;
  bool m_expired = false;
  string m_cur_schema;
  uint64_t m_proto_fields = UINT64_MAX;


public:

  typedef ds::Options<ds::mysqlx::Protocol_options> Options;
  using compression_mode_t = ds::mysqlx::Protocol_options::compression_mode_t;

  template <class C>
  Session(C &conn, const Options &options)
    : m_protocol(conn)
  {

    /*
      Check if the compression is needed and the compression type
      supported by the server
    */
    Compression_type::value compression = Compression_type::NONE;

    if (options.compression() != compression_mode_t::DISABLED)
    {
      compression = negotiate_compression();

      if (compression == Compression_type::NONE &&
          options.compression() == compression_mode_t::REQUIRED)
        throw_error("Compression requested but the server does not support it.");
    }
    send_connection_attr(options);
    authenticate(options, conn.is_secure());
    m_isvalid = true;
    // TODO: make "lazy" checks instead, deferring to the time when given
    // feature is used.
    check_protocol_fields();

    // start using compression now with the default threshold (1000)
    m_protocol.set_compression(compression, 1000);
  }

  /*
    Get the most suitable compression type supported by the server.
    Function prioritizes compression types as follows:
      LZ4
      DEFLATE

    If the higher priority type is not available the function will
    check a lower priority type.

    NONE is returned if the server does not support compression
  */
  Compression_type::value negotiate_compression();

  virtual ~Session();

  /*
    Check if given session is valid. Function is_valid() performs
    a lightweight, local check while check_valid() might communicate with
    the data store to perform this check. Both is_valid() and check_valid()
    return UNKNOWN if session state could not be determined.
  */

  option_t is_valid();
  option_t check_valid();

  /*
    Check that xplugin is supporting certain new fields in the protocol
    such as row locking, etc. The function sets binary flags in
    m_proto_fields member variable
  */

  void check_protocol_fields();
  bool has_prepared_statements();
  void set_has_prepared_statements(bool);
  bool has_keep_open();

  /*
    Clear diagnostic information that accumulated for the session.
    Diagnostics interface methods such as Diagnostics::error_count()
    and Diagnostics::get_errors() report only new diagnostics entries
    since last call to clear_errors() (or since session creation if
    clear_errors() was not called).
  */

  void clear_errors()
  { m_da.clear(); }

  /*
    Clean up the session by completing all pending statements and rollback
    the currently open transaction (if any). Results of these pending statements
    are discarded (note: if there are cursors opened for these results, an error
    will be thrown).
  */

  void clean_up();

  void reset();
  void close();

  /*
    Transactions
  */

  void begin();
  void commit();
  void rollback(const string &savepoint);
  void savepoint_set(const string &savepoint);
  void savepoint_remove(const string &savepoint);

  /*
    Prepared Statments
  */

  Reply_init prepared_execute(uint32_t stmt_id,
                               const Limit *lim,
                               const Param_source *param
                               );
  Reply_init prepared_execute(uint32_t stmt_id,
                               const cdk::Any_list *list
                               );

  Reply_init prepared_deallocate(uint32_t stmt_id);

  /*
     SQL API
  */

  Reply_init sql(uint32_t stmt_id, const string&, Any_list*);

  Reply_init admin(const char*, const cdk::Any::Document&);

  /*
    CRUD API
  */

  Reply_init coll_add(const Table_ref&,
                       Doc_source&,
                       const Param_source *param = nullptr,
                       bool upsert = false);

  Reply_init coll_remove(uint32_t stmt_id,
                          const Table_ref&,
                          const Expression *expr = nullptr,
                          const Order_by *order_by = nullptr,
                          const Limit *lim = nullptr,
                          const Param_source *param = nullptr);
  Reply_init coll_find(uint32_t stmt_id,
                        const Table_ref&,
                        const View_spec *view = nullptr,
                        const Expression *expr = nullptr,
                        const Expression::Document *proj = nullptr,
                        const Order_by *order_by = nullptr,
                        const Expr_list *group_by = nullptr,
                        const Expression *having = nullptr,
                        const Limit *lim = nullptr,
                        const Param_source *param = nullptr,
                        const Lock_mode_value lock_mode = Lock_mode_value::NONE,
                        const Lock_contention_value lock_contention
                          = Lock_contention_value::DEFAULT);
  Reply_init coll_update(uint32_t stmt_id,
                          const api::Table_ref&,
                          const Expression*,
                          const Update_spec&,
                          const Order_by *order_by = nullptr,
                          const Limit* = nullptr,
                          const Param_source * = nullptr);

  Reply_init table_delete(uint32_t stmt_id,
                           const Table_ref&,
                           const Expression *expr = nullptr,
                           const Order_by *order_by = nullptr,
                           const Limit *lim = nullptr,
                           const Param_source *param = nullptr);
  Reply_init table_select(uint32_t stmt_id,
                           const Table_ref&,
                           const View_spec *view = nullptr,
                           const Expression *expr = nullptr,
                           const Projection *proj = nullptr,
                           const Order_by *order_by = nullptr,
                           const Expr_list *group_by = nullptr,
                           const Expression *having = nullptr,
                           const Limit *lim = nullptr,
                           const Param_source *param = nullptr,
                           const Lock_mode_value lock_mode = Lock_mode_value::NONE,
                           const Lock_contention_value lock_contention = Lock_contention_value::DEFAULT);
  Reply_init table_insert(uint32_t stmt_id,
                           const Table_ref&,
                           Row_source&,
                           const api::Columns *cols,
                           const Param_source *param = nullptr);
  Reply_init table_update(uint32_t stmt_id,
                           const api::Table_ref &coll,
                           const Expression *expr,
                           const Update_spec &us,
                           const Order_by *order_by = nullptr,
                           const Limit *lim = nullptr,
                           const Param_source *param = nullptr);

  Reply_init view_drop(const api::Table_ref&, bool check_existence = false);



  /*
      Async (cdk::api::Async_op)
  */

  bool is_completed() const;
  const api::Event_info* get_event_info() const;


  // Diagnostics API

  unsigned int entry_count(Severity::value level=Severity::ERROR)
  { return m_da.entry_count(level); }

  Iterator& get_entries(Severity::value level=Severity::ERROR)
  { return m_da.get_entries(level); }

  const Error& get_error()
  { return m_da.get_error(); }


  const string& get_current_schema() const
  {
    return m_cur_schema;
  }

private:

  // Send Connection Attributes
  void send_connection_attr(const Options &options);
  // Authentication (cdk::protocol::mysqlx::Auth_processor)
  void authenticate(const Options &options, bool secure = false);
  void do_authenticate(const Options &options, int auth_method, bool secure);

  //  Reply registration
  virtual void register_stmt(Stmt_op* reply);
  virtual void deregister_stmt(Stmt_op*);

  /*
    Errors and notices.
  */

  void notice(unsigned int type, short int scope, bytes payload);
  void error(unsigned int code, short int severity, sql_state_t sql_state, const string& msg);

  /*
    SessionState_processor
  */

  void client_id(unsigned long);
  void account_expired();
  void current_schema(const string&);
  void row_stats(row_stats_t, row_count_t) {}
  void last_insert_id(insert_id_t) {}
  // TODO: void trx_event(trx_event_t);
  void generated_document_id(const std::string&) {}

  /*
      Async (cdk::api::Async_op)
  */

  bool do_cont();
  void do_wait();
  void do_cancel();

};


using cdk::api::Column_ref;
using cdk::api::Table_ref;
using cdk::api::Schema_ref;
using cdk::api::Object_ref;


}} //cdk::mysqlx



#endif // CDK_MYSQLX_SESSION_H
