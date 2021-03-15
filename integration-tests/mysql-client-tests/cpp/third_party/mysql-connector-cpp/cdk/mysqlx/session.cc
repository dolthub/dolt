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

#include <mysql/cdk/foundation.h>
#include <mysql/cdk/mysqlx.h>
#include <mysql/cdk/protocol/mysqlx.h>

PUSH_SYS_WARNINGS_CDK
#include <iostream>
#include "auth_hash.h"
POP_SYS_WARNINGS_CDK

#include "stmt.h"


namespace cdk {
namespace mysqlx {

Compression_type::value Session::negotiate_compression()
{
  Compression_type::value compression = Compression_type::NONE;

  struct : cdk::protocol::mysqlx::api::Any::Document
  {
    std::string m_algorithm = "lz4_message";

    void process(Processor &prc) const
    {
      prc.doc_begin();
      {
        auto doc_prc = cdk::safe_prc(prc)->key_val("compression")->doc();
        doc_prc.doc_begin();

        doc_prc.key_val("algorithm")->scalar()->str(m_algorithm);
        doc_prc.key_val("server_combine_mixed_messages")->scalar()->yesno(false);

        doc_prc.doc_end();
      }
      prc.doc_end();
    }
  } compress_caps;

  struct : cdk::protocol::mysqlx::Reply_processor
  {
    bool m_compression_ok = true;

    void ok(string msg)
    {
      m_compression_ok = true;
    }

    void error(unsigned int, short int,
               cdk::protocol::mysqlx::sql_state_t, const string&)
    {
      m_compression_ok = false;
    }
  } cap_prc;

  /*
    The compression types must be attempted with increaing
    priority. The last successful type will be applied.
  */
  compress_caps.m_algorithm = "deflate_stream";
  m_protocol.snd_CapabilitiesSet(compress_caps).wait();
  compress_caps.m_algorithm = "lz4_message";
  m_protocol.snd_CapabilitiesSet(compress_caps).wait();
  compress_caps.m_algorithm = "zstd_stream";
  m_protocol.snd_CapabilitiesSet(compress_caps).wait();

  m_protocol.rcv_Reply(cap_prc).wait();
  if (cap_prc.m_compression_ok)
    compression = Compression_type::DEFLATE;

  m_protocol.rcv_Reply(cap_prc).wait();
  if (cap_prc.m_compression_ok)
    compression = Compression_type::LZ4;

  m_protocol.rcv_Reply(cap_prc).wait();
  if (cap_prc.m_compression_ok)
    compression = Compression_type::ZSTD;

  return compression;
}

/*
  A structure to check if xplugin we are connecting supports a
  specific field
*/

struct Proto_field_checker
  : public cdk::protocol::mysqlx::api::Expectations
{
  cdk::bytes m_data;
  cdk::protocol::mysqlx::Protocol &m_proto;

  Proto_field_checker(cdk::protocol::mysqlx::Protocol &proto) :
    m_proto(proto)
  {}

  struct Check_reply_prc : cdk::protocol::mysqlx::Reply_processor
  {
    unsigned int m_code = 0;

    void error(unsigned int code, short int,
               cdk::protocol::mysqlx::sql_state_t, const string &)
    {
      m_code = code;
    }

    void ok(string)
    {
      m_code = 0;
    }
  };

  void process(Processor &prc) const
  {
    prc.list_begin();
    prc.list_el()->set(FIELD_EXISTS, m_data);
    prc.list_end();
  }

  /*
  This method sets the expectation and returns
  the field flag if it is supported, otherwise 0 is returned.
  */
  uint64_t is_supported(Protocol_fields::value v)
  {
    switch (v)
    {
    case Protocol_fields::ROW_LOCKING:
      // Find=17, locking=12
      m_data = bytes("17.12");
      break;
    case Protocol_fields::UPSERT:
      // Insert=18, upsert=6
      m_data = bytes("18.6");
      break;
    case Protocol_fields::PREPARED_STATEMENTS:
      m_data = bytes("40");
      break;
    case Protocol_fields::KEEP_OPEN:
      m_data = bytes("6.1");
      break;
    case Protocol_fields::COMPRESSION:
      m_data = bytes("46");
      break;
    default:
      return 0;
    }
    m_proto.snd_Expect_Open(*this, false).wait();

    Check_reply_prc prc;
    m_proto.rcv_Reply(prc).wait();
    uint64_t ret = prc.m_code == 0 ? (uint64_t)v : 0;

    if (prc.m_code == 0 || prc.m_code == 5168)
    {
      /*
      The expectation block needs to be closed if no error
      or expectation failed error (5168)
      */
      m_proto.snd_Expect_Close().wait();
      m_proto.rcv_Reply(prc).wait();
    }
    return ret;
  }
};


class error_category_server : public foundation::error_category_base
{
public:

  error_category_server() {}

  virtual const char* name() const NOEXCEPT { return "server"; }
  virtual std::string message(int) const NOEXCEPT { return "Server Error"; }
  virtual error_condition do_default_error_condition(int) const;
  virtual bool  do_equivalent(int, const error_condition&) const;

};

const error_category& server_error_category()
{
  static const error_category_server instance;
  return instance;
}

error_code server_error(int code)
{
  return error_code(code, server_error_category());
}


error_condition
error_category_server::do_default_error_condition(int errc) const
{
  switch (errc)
  {
  case 0:
    return errc::no_error;
  // TODO: Define appropriate error conditions for server errors.
  default:
    THROW("No error condition defined for server error");
    //return errc::operation_not_permitted;
  }
}

bool error_category_server::do_equivalent(int code,
                                       const error_condition &ec) const
{
  try
  {
    return ec == default_error_condition(code);
  }
  catch (...)
  {
    return false;
  }
}


/*
   Authentication
*/


SessionAuth::SessionAuth(Session &s, const char *method)
  : m_sess(s), m_am(method)
{}


void SessionAuth::restart()
{
  if (INIT != m_state && !is_completed())
    THROW("Attempt to restart on-going authentication.");

  m_state = START;
  m_op = &m_sess.m_protocol.snd_AuthenticateStart(
    m_am, auth_data(), auth_response(0, {})
  );
}

// TODO: true asynchronous implementation.

bool SessionAuth::do_cont()
{
  do_wait();
  return true;
}

void SessionAuth::do_wait()
{
  if (!is_completed() && !m_op)
    restart();

  while (!is_completed())
  {
    if (m_op)
      m_op->wait();
    m_op = nullptr;

    switch (m_state)
    {
    case START:
    case CONT:
    {
      // note: while processing incoming message, m_op might be set
      // to another operation that needs to be executed.
      m_sess.m_protocol.rcv_AuthenticateReply(*this).wait();
      continue;
    }
    default:
      return;
    }
  }
}


bool SessionAuth::is_completed() const
{
  switch (m_state)
  {
  case DONE:
  case ERROR:
    return true;
  default:
    return false;
  }
}

bool SessionAuth::do_get_result()
{
  return DONE == m_state;
}

const cdk::api::Event_info* SessionAuth::get_event_info() const
{
  return m_op ? m_op->waits_for() : nullptr;
}


void SessionAuth::do_cancel()
{
  if (!m_op)
    return;
  m_op->cancel();
}

//void SessionAuth::do_cancel()
//{
//  THROW("Canceling authentication process not implemented.");
//}


void  SessionAuth::auth_ok(bytes)
{
  m_state = DONE;
}

void  SessionAuth::auth_continue(bytes data)
{
  m_state = CONT;
  m_op = &m_sess.m_protocol.snd_AuthenticateContinue(
    auth_response(++m_round, data)
  );
}

void SessionAuth::error(
  unsigned int code, short int severity,
  sql_state_t sql_state, const string &msg
)
{
  m_state = ERROR;
  m_sess.error(code, severity, sql_state, msg);
}

void SessionAuth::notice(
  unsigned int type, short int scope, bytes payload
)
{
  m_sess.notice(type, scope, payload);
}


/*
  Specializations of SessionAuth that implement different
  authentication mechanisms.
*/


class AuthPlain
    : public SessionAuth
{
  std::string m_data;

  public:

  AuthPlain(Session &s, const Session::Options &options)
    : SessionAuth(s, "PLAIN")
  {
    std::string user(options.user());  // convert to utf8 before sending

    // TODO: Check if this is correct way to sepcify default schema

    if (options.database())
      m_data.append(*options.database());

    m_data.push_back('\0'); // authz
    m_data.append(user).push_back('\0'); // authc
    if (options.password())
      m_data.append(*options.password()); // pass
  }

  virtual bytes auth_data() override
  {
    return bytes((byte*)m_data.c_str(), m_data.size());
  }

  virtual bytes auth_response(unsigned round, bytes) override
  {
    if (0 == round)
      return {};

    THROW("Unexpected auth continuation");
  }

};


class AuthExternal
  : public SessionAuth
{

  std::string m_data;

public:

  AuthExternal(Session &s, const Session::Options &options)
    : SessionAuth(s, "EXTERNAL")
  {
    if (options.database())
      m_data.append(*options.database());
  }

  virtual bytes auth_data() override
  {
    return bytes((byte*)m_data.c_str(), m_data.size());
  }

  virtual bytes auth_response(unsigned round, bytes) override
  {
    if (0 == round)
      return {};

    THROW("Unexpected auth continuation");
  }
};


class HashAuth
    : public SessionAuth
{

protected:

  std::string m_user;
  std::string m_pass;
  std::string m_db;

  std::string m_cont_data;

  virtual std::string build_hash(bytes data) = 0;

public:

  HashAuth(Session &s, const char *method, const Session::Options &options)
    : SessionAuth(s, method)
    , m_user(options.user())
  {
    if (options.password())
      m_pass = *options.password();
    if (options.database())
      m_db = *options.database();
  }


  virtual bytes auth_data() override
  {
    return {};
  }

  virtual bytes auth_response(unsigned round, bytes data) override
  {
    if (0 == round)
      return {};

    m_cont_data = build_hash(data);

    return bytes((byte*)m_cont_data.c_str(), m_cont_data.size());
  }

};


class AuthMysql41
  : public HashAuth
{
public:

  AuthMysql41(Session &s, const Session::Options &options)
    : HashAuth(s, "MYSQL41", options)
  {}

  std::string build_hash(bytes data) override
  {
    return ::mysqlx::build_mysql41_authentication_response(
      std::string(data.begin(), data.end()), m_user, m_pass, m_db
    );
  }
};


class AuthSha256Memory
    : public HashAuth
{
public:

  AuthSha256Memory(Session &s, const Session::Options &options)
    : HashAuth(s, "SHA256_MEMORY", options)
  {}

  std::string build_hash(bytes data) override
  {
    return ::mysqlx::build_sha256_authentication_response(
      std::string(data.begin(), data.end()), m_user, m_pass, m_db
    );
  }
};



/*
   Class Session
*/

void Session::send_connection_attr(const Options &options)
{

  struct Attr_converter
      : cdk::protocol::mysqlx::api::Any::Document
      , ds::Attr_processor
  {
    Attr_converter(const ds::Session_attributes* attr)
      :m_attr(attr)
    {}

    const ds::Session_attributes * m_attr;
    Processor::Any_prc::Doc_prc *m_attr_prc;

    void process(Processor &prc) const override
    {
      auto *self  = const_cast<Attr_converter*>(this);
      prc.doc_begin();
      self->m_attr_prc = prc.key_val("session_connect_attrs")->doc();
      self->m_attr_prc->doc_begin();
      m_attr->process(*self);
      self->m_attr_prc->doc_end();
      prc.doc_end();
    }

    void attr(const string &key, const string &val) override
    {
      m_attr_prc->key_val(key)->scalar()->str(bytes(val));
    }

  } ;

  if (options.attributes())
  {
    m_protocol.snd_CapabilitiesSet(Attr_converter(options.attributes())).wait();

    struct Check_reply_prc : cdk::protocol::mysqlx::Reply_processor
    {
      string m_msg;
      unsigned int m_code = 0;
      cdk::protocol::mysqlx::sql_state_t m_sql_state;
      void error(unsigned int code, short int,
                 cdk::protocol::mysqlx::sql_state_t state, const string &msg) override
      {
        m_code = code;
        m_sql_state = state;
        m_msg = msg;
      }

      void ok(string) override
      {}
    };

    Check_reply_prc prc;

    m_protocol.rcv_Reply(prc).wait();

    if(prc.m_code != 0 &&    prc.m_code != 5002)
    {
      //code: 5002
      //msg: "Capability \'session_connect_attrs\' doesn\'t exist"
      throw Server_error(prc.m_code, prc.m_sql_state, prc.m_msg);
    }

  }
}


void Session::do_authenticate(const Options &options,
                              int original_am,
                              bool  secure_conn)
{
  using cdk::ds::mysqlx::Protocol_options;

  auto am = original_am;
  if (Protocol_options::DEFAULT == am)
    am = secure_conn ? Protocol_options::PLAIN : Protocol_options::MYSQL41;

  switch (am)
  {
  case Protocol_options::MYSQL41:
    m_auth.reset(new AuthMysql41(*this, options));
    break;
  case Protocol_options::PLAIN:
    m_auth.reset(new AuthPlain(*this, options));
    break;
  case Protocol_options::EXTERNAL:
    m_auth.reset(new AuthExternal(*this, options));
    break;
  case Protocol_options::SHA256_MEMORY:
    m_auth.reset(new AuthSha256Memory(*this, options));
    break;
  case Protocol_options::DEFAULT:
    assert(false);  // should not happen
  default:
    THROW("Unknown authentication method");
  }

  if (m_auth->get_result())
    return;

  // second attempt

  if (Protocol_options::DEFAULT == original_am && !secure_conn)
  {
    //Cleanup Diagnostic_area
    clear_errors();

    m_auth.reset(new AuthSha256Memory(*this, options));

    if (!m_auth->get_result())
    {
      throw_error("Authentication failed using MYSQL41 and SHA256_MEMORY, "
                    "check username and password or try a secure connection");
    }
  }
}


void Session::authenticate(const Options &options, bool  secure_conn)
{
  do_authenticate(options, options.auth_method(),secure_conn);
  if (entry_count())
    get_error().rethrow();
}


Session::~Session()
{
  //TODO: add timeout to close session!
  try
  {
    close();
  }
  catch (...)
  {
    // Something went wrong - do not try to use this session again.
    m_isvalid = false;
  }
}


option_t Session::is_valid()
{
  wait();
  // TODO: should errors be thrown here, if any?
  return m_isvalid;
}


void Session::check_protocol_fields()
{
  wait();
  if (0 < entry_count())
    get_error().rethrow();
  if (m_proto_fields == UINT64_MAX)
  {
    Proto_field_checker field_checker(m_protocol);
    m_proto_fields = 0;
    /* More fields checks will be added here */
    m_proto_fields |= field_checker.is_supported(Protocol_fields::ROW_LOCKING);
    m_proto_fields |= field_checker.is_supported(Protocol_fields::UPSERT);
    m_proto_fields |= field_checker.is_supported(Protocol_fields::PREPARED_STATEMENTS);
    m_proto_fields |= field_checker.is_supported(Protocol_fields::KEEP_OPEN);
    m_proto_fields |= field_checker.is_supported(Protocol_fields::COMPRESSION);
  }
}


bool Session::has_prepared_statements()
{
  check_protocol_fields();
  return (m_proto_fields & Protocol_fields::PREPARED_STATEMENTS) != 0;
}

void Session::set_has_prepared_statements(bool x)
{
  if (x)
    m_proto_fields |= Protocol_fields::PREPARED_STATEMENTS;
  else
    m_proto_fields &= ~Protocol_fields::PREPARED_STATEMENTS;
}

bool Session::has_keep_open()
{
  check_protocol_fields();
  return (m_proto_fields & Protocol_fields::KEEP_OPEN) != 0;
}


option_t Session::check_valid()
{
  //TODO: contact server to check session

  return  is_valid() ? true : false;
}

void Session::reset()
{
  clean_up();

  if (is_valid())
  {
    // TODO: Do it in asnyc fashion using the fact that session is an
    // async operation

    m_protocol.snd_SessionReset(has_keep_open()).wait();
    m_protocol.rcv_Reply(*this).wait();

    if (!has_keep_open())
    {
      // Re-authenticate for servers not supporting keep-open
      m_isvalid = false;
      clear_errors();
      m_auth->restart();
      m_auth->wait();
      if (entry_count())
        get_error().rethrow();
      m_isvalid = m_auth->get_result();
    }
  }
}


/*
  Discard results for the given statement and all previous statements
  for which it is waiting (if any).
*/

void discard_results(Stmt_op *stmt)
{
  if (!stmt)
    return;
  if (stmt->m_prev_stmt)
    discard_results(stmt->m_prev_stmt);
  stmt->discard();
  stmt->wait();
}


void Session::clean_up()
{
  if (!is_valid())
    return;
  discard_results(m_last_stmt);
  rollback({});
  clear_errors();
}


void Session::close()
{
  if (is_valid())
  try {
    clean_up();
    m_protocol.snd_ConnectionClose().wait();
    m_protocol.rcv_Reply(*this).wait();
  }
  catch (...)
  {
    m_isvalid = false;
    throw;
  }
  m_isvalid = false;
}


/*
  Statements registered with a session are put into a double linked
  list, with stmt->m_prev_stmt pointing at the previous statement that
  needs to be completed before the given one can finish (see Stmt_op).
  In Session we store pointer to the end of this list, because this is
  where we add new statements.
*/

void Session::register_stmt(Stmt_op *stmt)
{
  assert(stmt);
  assert(!stmt->m_session);

  stmt->m_session = this;

  // Append stmt to the end of the list of active statements.

  stmt->m_prev_stmt = m_last_stmt;
  if (m_last_stmt)
  {
    assert(nullptr == m_last_stmt->m_next_stmt);
    m_last_stmt->m_next_stmt = stmt;
  }
  m_last_stmt = stmt;
}


void Session::deregister_stmt(Stmt_op *stmt)
{
  assert(stmt);

  if (!stmt->m_session)
    return;

  assert(stmt->m_session == this);
  stmt->m_session = nullptr;

  // Remove stmt from the list of active statements.

  if (stmt->m_next_stmt)
    stmt->m_next_stmt->m_prev_stmt = stmt->m_prev_stmt;
  if (stmt->m_prev_stmt)
    stmt->m_prev_stmt->m_next_stmt = stmt->m_next_stmt;

  if (m_last_stmt == stmt)
    m_last_stmt = stmt->m_prev_stmt;

  stmt->m_prev_stmt = stmt->m_next_stmt = nullptr;
}



Reply_init
Session::sql(uint32_t stmt_id,const string &stmt, Any_list *args)
{
  return new Cmd_StmtExecute(*this, stmt_id, "sql", stmt, args);
}


Reply_init
Session::admin(const char *cmd, const cdk::Any::Document &args)
{

  if (!is_valid())
    throw_error("admin: invalid session");

  return new Cmd_StmtExecute(*this, 0U, "mysqlx", cmd, &args);
}


/*
  Note: current implementation of transaction operations simply uses
  relevant SQL statements. Eventually we need something more fancy
  which will work well in a distributed environment.
*/

void Session::begin()
{
  std::unique_ptr<Stmt_op> op(sql(0, "START TRANSACTION", nullptr));
  op->wait();
  if (op->entry_count() > 0)
    op->get_error().rethrow();
}

void Session::commit()
{
  std::unique_ptr<Stmt_op> op(sql(0, "COMMIT", nullptr));
  op->wait();
  if (op->entry_count() > 0)
    op->get_error().rethrow();
}

void Session::rollback(const string &savepoint)
{
  string qry = "ROLLBACK";
  if (!savepoint.empty())
    qry += " TO `" + savepoint + "`";
  std::unique_ptr<Stmt_op> op(sql(0, qry, nullptr));
  op->wait();
  if (op->entry_count() > 0)
    op->get_error().rethrow();
}

void Session::savepoint_set(const string &savepoint)
{
  // TODO: some chars in savepoint name need to be quotted.
  string qry = u"SAVEPOINT `" + savepoint + u"`";
  std::unique_ptr<Stmt_op> op(sql(0, qry, nullptr));
  op->wait();
  if (op->entry_count() > 0)
    op->get_error().rethrow();
}

void Session::savepoint_remove(const string &savepoint)
{
  string qry = "RELEASE SAVEPOINT `" + savepoint + "`";
  std::unique_ptr<Stmt_op> op(sql(0, qry, nullptr));
  op->wait();
  if (op->entry_count() > 0)
    op->get_error().rethrow();
}


Reply_init Session::prepared_execute(
  uint32_t stmt_id, const Limit *lim, const Param_source *param
)
{
  return new Prepared<Query_stmt>(*this, stmt_id, lim, param);
}

Reply_init Session::prepared_execute(
  uint32_t stmt_id, const cdk::Any_list *list
)
{
  return new Prepared<Query_stmt>(*this, stmt_id, list);
}


Reply_init Session::prepared_deallocate(uint32_t stmt_id)
{
  struct Prepared_deallocate
    : public Stmt_op
  {
    uint32_t m_id;

    Prepared_deallocate(Session &s, uint32_t id)
      : Stmt_op(s)
      , m_id(id)
    {}

    Proto_op* send_cmd() override
    {
      return &get_protocol().snd_PrepareDeallocate(m_id);
    }
  };

  return new Prepared_deallocate(*this, stmt_id);
}



Reply_init Session::coll_add(
  const Table_ref &coll,
  Doc_source &docs,
  const Param_source *param,
  bool upsert
)
{
  return new Cmd_InsertDocs(*this, 0U, coll, docs, param, upsert);
}


Reply_init Session::coll_remove(
  uint32_t stmt_id,
  const Table_ref &coll,
  const Expression *expr,
  const Order_by *order_by,
  const Limit *lim,
  const Param_source *param
)
{
  return new Cmd_Delete<protocol::mysqlx::DOCUMENT>(
    *this, stmt_id, coll, expr,order_by, lim, param
  );
}


Reply_init Session::coll_find(
  uint32_t stmt_id,
  const Table_ref &coll,
  const View_spec *view,
  const Expression *expr,
  const Expression::Document *proj,
  const Order_by *order_by,
  const Expr_list *group_by,
  const Expression *having,
  const Limit *lim,
  const Param_source *param,
  const Lock_mode_value lock_mode,
  const Lock_contention_value lock_contention
)
{
  if (lock_mode != Lock_mode_value::NONE &&
      !(m_proto_fields & Protocol_fields::ROW_LOCKING))
    throw_error("Row locking is not supported by this version of the server");

 auto *find
    =  new Cmd_Find<protocol::mysqlx::DOCUMENT>(
        *this, stmt_id, coll, expr, proj,
        order_by,group_by, having, lim, param, lock_mode, lock_contention
       );

  if (view)
    return new Cmd_ViewCrud<protocol::mysqlx::DOCUMENT>(*this, *view, find);

  return find;
}


Reply_init Session::coll_update(
  uint32_t stmt_id,
  const api::Table_ref &coll,
  const Expression *expr,
  const Update_spec &us,
  const Order_by *order_by,
  const Limit *lim,
  const Param_source *param
)
{
  return new Cmd_Update<protocol::mysqlx::DOCUMENT>(
    *this, stmt_id, coll, expr, us, order_by, lim, param
  );
}



Reply_init Session::table_insert(
  uint32_t stmt_id,
  const Table_ref &coll,
  Row_source &rows,
  const api::Columns *cols,
  const Param_source *param
)
{
  return new Cmd_InsertRows(*this, stmt_id, coll, rows, cols, param);
}


Reply_init Session::table_delete(
  uint32_t stmt_id,
  const Table_ref &coll,
  const Expression *expr,
  const Order_by *order_by,
  const Limit *lim,
  const Param_source *param
)
{
  return new Cmd_Delete<protocol::mysqlx::TABLE>(
    *this, stmt_id, coll, expr, order_by, lim, param
  );
}


Reply_init Session::table_select(
  uint32_t stmt_id,
  const Table_ref &coll,
  const View_spec *view,
  const Expression *expr,
  const Projection *proj,
  const Order_by *order_by,
  const Expr_list *group_by,
  const Expression *having,
  const Limit *lim,
  const Param_source *param,
  const Lock_mode_value lock_mode,
  const Lock_contention_value lock_contention
)
{
  if (lock_mode != Lock_mode_value::NONE &&
      !(m_proto_fields & Protocol_fields::ROW_LOCKING))
    throw_error("Row locking is not supported by this version of the server");

  auto* select_cmd =
      new Cmd_Find<protocol::mysqlx::TABLE>(
        *this, stmt_id, coll, expr, proj, order_by,
        group_by, having, lim, param, lock_mode, lock_contention
        );

  if (view)
    return new Cmd_ViewCrud<protocol::mysqlx::TABLE>(*this, *view, select_cmd);

  return select_cmd;
}


Reply_init Session::table_update(
  uint32_t stmt_id,
  const api::Table_ref &coll,
  const Expression *expr,
  const Update_spec &us,
  const Order_by *order_by,
  const Limit *lim,
  const Param_source *param
)
{
  return new Cmd_Update<protocol::mysqlx::TABLE>(
    *this, stmt_id, coll, expr, us, order_by, lim, param
  );
}


Reply_init Session::view_drop(const api::Table_ref &view, bool check_existence)
{
  struct Drop_view
    : public Stmt_op
  {
    bool m_check;

    Drop_view(Session &s, const api::Table_ref &view, bool check)
      : Stmt_op(s)
      , m_check(check)
    {
      set(view);
    }

    Proto_op* send_cmd() override
    {
      return &get_protocol().snd_DropView(*this, m_check);
    }
  };

  return new Drop_view(*this, view, check_existence);
}


void Session::notice(unsigned int type, short int scope, bytes payload)
{
  using namespace protocol::mysqlx;

  switch (type)
  {
  case notice_type::Warning:
    process_notice<notice_type::Warning>(
      payload,
      *static_cast<Reply_processor*>(this)
    );
    return;

  case notice_type::SessionStateChange:
    if (notice_scope::LOCAL != scope)
      return;
    process_notice<notice_type::SessionStateChange>(payload, *this);
    return;

  default: return;
  }
}


void Session::error(unsigned int code, short int severity,
                    sql_state_t sql_state, const string &msg)
{
  Severity::value level;
  switch (severity)
  {
  case 0: level = Severity::INFO; break;
  case 1: level = Severity::WARNING; break;
  case 2:
  default:
    level = Severity::ERROR; break;
  }
  m_da.add_entry(level, new Server_error(code, sql_state, msg));
}


/*
  Processing session state change notices.
*/

void Session::client_id(unsigned long val)
{
  m_id = val;
}

void Session::account_expired()
{
  m_expired = true;
}

void Session::current_schema(const string &val)
{
  m_cur_schema = val;
}


// Asynchronous operation.


bool Session::is_completed() const
{
  if (m_auth && !m_auth->is_completed())
    return false;

  return true;
}


bool Session::do_cont()
{
  if (m_auth && !m_auth->cont())
    return false;

  return true;
}


void Session::do_wait()
{
  if (m_auth)
    m_auth->wait();
}


void Session::do_cancel()
{
  if (m_auth)
    m_auth->cancel();
}


const cdk::api::Event_info* Session::get_event_info() const
{
  if (m_auth && !m_auth->is_completed())
    return m_auth->waits_for();

  return nullptr;
}


}} // cdk mysqlx




