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

#ifndef CDK_MYSQLX_RESULT_H
#define CDK_MYSQLX_RESULT_H

#include "common.h"
#include "session.h"


namespace cdk {
namespace mysqlx {

class Reply;
class Cursor;


// ---------------------------------------------------------

/*
  Classes to store meta-data information received from server.
*/


template <class Base>
class Obj_ref : public Base
{
protected:

  string m_name;
  string m_name_original;
  bool   m_has_name_original;

public:

  Obj_ref()
    : m_has_name_original(false)
  {}

  Obj_ref(const cdk::api::Ref_base &ref)
    : m_name(ref.name())
    , m_name_original(ref.orig_name())
    , m_has_name_original(true)
  {}

  const string name() const { return m_name; }
  const string orig_name() const
  {
    return m_has_name_original ? m_name_original : m_name;
  }

  friend class Stmt_op;
};


/*
  Determine charset from collation id reported by the protocol. The mapping
  is given by COLLATIONS_XXX() lists in collations.h.
*/

inline
cdk::Charset::value get_collation_cs(collation_id_t id)
{
  /*
    If collation id is 0, that is, there is no collation info in server
    reply, we assume utf8.
  */

  if (0 == id)
    return cdk::Charset::utf8;

#undef  CS
#define COLL_TO_CS(CS) COLLATIONS_##CS(COLL_TO_CS_CASE) return cdk::Charset::CS;
#define COLL_TO_CS_CASE(CS,ID,COLL,CC)  case ID:

  switch (id)
  {
    CDK_CS_LIST(COLL_TO_CS)
  default:
    THROW("Unkonwn collation id");
  }
}


class Col_metadata
  : public Obj_ref<cdk::Column_info>
  , public cdk::Format_info
{
  typedef Column_info::length_t length_t;

  int          m_type;
  int          m_content_type;
  length_t     m_length;
  unsigned int m_decimals;
  collation_id_t m_cs;
  uint32_t     m_flags;

  struct : public Obj_ref<cdk::api::Table_ref>
  {
    struct : public Obj_ref<cdk::api::Schema_ref>
    {
      Obj_ref<cdk::api::Ref_base> m_catalog;
      const cdk::api::Ref_base* catalog() const { return &m_catalog; }
    } m_schema;

    bool m_has_schema;

    const cdk::api::Schema_ref* schema() const
    {
      return m_has_schema ? &m_schema : nullptr;
    }

  } m_table;

  bool      m_has_table;

  const cdk::api::Table_ref* table() const
  {
    return m_has_table ? &m_table : nullptr;
  }

  /*
    Format_info interface
    ---------------------
  */

  bool for_type(Type_info type) const
  {
    switch (m_type)
    {
    case protocol::mysqlx::col_type::SINT:
    case protocol::mysqlx::col_type::UINT:
      return TYPE_INTEGER == type;

    case protocol::mysqlx::col_type::FLOAT:
    case protocol::mysqlx::col_type::DOUBLE:
    case protocol::mysqlx::col_type::DECIMAL:
      return TYPE_FLOAT == type;

    case protocol::mysqlx::col_type::TIME:
    case protocol::mysqlx::col_type::DATETIME:
      return TYPE_DATETIME == type;

    case protocol::mysqlx::col_type::BYTES:
      switch (m_content_type)
      {
      case content_type::JSON: return TYPE_DOCUMENT == type;
      case content_type::GEOMETRY: return TYPE_GEOMETRY == type;
      case content_type::XML: return TYPE_XML == type;
      default: break;
      }

      FALLTHROUGH;
    case protocol::mysqlx::col_type::ENUM:
    default:
      return TYPE_BYTES == type || TYPE_STRING == type;
    }
  }

  /*
    Methods get_info() update a Type_info object to describe the
    encoding format used by this column data.
  */

  void get_info(Format<TYPE_INTEGER>& fmt) const
  {
    switch (m_type)
    {
    case protocol::mysqlx::col_type::SINT:
      Format<TYPE_INTEGER>::Access::set_fmt(fmt, Format<TYPE_INTEGER>::SINT);
      break;
    case protocol::mysqlx::col_type::UINT:
      Format<TYPE_INTEGER>::Access::set_fmt(fmt, Format<TYPE_INTEGER>::UINT);
      break;
    }
    Format<TYPE_INTEGER>::Access::set_length(fmt, m_length);
  }

  void get_info(Format<TYPE_FLOAT>& fmt) const
  {
    switch (m_type)
    {
    case protocol::mysqlx::col_type::FLOAT:
      Format<TYPE_FLOAT>::Access::set_fmt(fmt, Format<TYPE_FLOAT>::FLOAT);
      break;
    case protocol::mysqlx::col_type::DOUBLE:
      Format<TYPE_FLOAT>::Access::set_fmt(fmt, Format<TYPE_FLOAT>::DOUBLE);
      break;
    case protocol::mysqlx::col_type::DECIMAL:
      Format<TYPE_FLOAT>::Access::set_fmt(fmt, Format<TYPE_FLOAT>::DECIMAL);
      break;
    }
  }

  void get_info(Format<TYPE_STRING>& fmt) const
  {
    Format<TYPE_STRING>::Access::set_cs(fmt, get_collation_cs(m_cs));

    /*
      Note: Types ENUM and SET are generally treated as
      strings, but we set a 'kind' flag in the format description
      to be able to distinguish them from plain strings.
    */

    switch (m_type)
    {
    case protocol::mysqlx::col_type::BYTES:
      Format<TYPE_STRING>::Access::set_width(fmt, m_length);
      break;
    case protocol::mysqlx::col_type::SET:
      Format<TYPE_STRING>::Access::set_kind_set(fmt);
      break;
    case protocol::mysqlx::col_type::ENUM:
      Format<TYPE_STRING>::Access::set_kind_enum(fmt);
      break;
    }
  }

  void get_info(Format<TYPE_DATETIME>& fmt) const
  {
    switch (m_type)
    {
    case protocol::mysqlx::col_type::TIME:
      Format<TYPE_DATETIME>::Access::set_fmt(fmt, Format<TYPE_DATETIME>::TIME, true);
      break;

    case protocol::mysqlx::col_type::DATETIME:

      // Note: flag 0x01 distinguishes TIMESTAMP from DATETIME type.

      if (m_flags & 0x01)
        Format<TYPE_DATETIME>::Access::set_fmt(fmt,
          Format<TYPE_DATETIME>::TIMESTAMP, true);
      else
      {
        /*
        Note: presence of time part is detected based on the length
        of the column. Full DATETIME values occupy more than 10
        positions.
        */

        Format<TYPE_DATETIME>::Access::set_fmt(fmt,
          Format<TYPE_DATETIME>::DATETIME, m_length > 10);
      }
      break;
    }
  }

  void get_info(Format<TYPE_BYTES> &fmt) const
  {
    // Note: flag 0x01 means that bytes should be padded with 0x00

    if (m_flags & 0x01)
    {
      Format<TYPE_BYTES>::Access::set_width(fmt, m_length);
    }
  }

  /*
    Note: Access to default implementation for all overloads that
    are not explicitly defined above
    (see: http://stackoverflow.com/questions/9995421/gcc-woverloaded-virtual-warnings
  */

  using Format_info::get_info;

public:

  Col_metadata()
    : m_type(0)
    , m_content_type(0)
    , m_length(0)
    , m_decimals(0)
    , m_cs(BINARY_CS_ID)
    , m_flags(0)
    , m_has_table(false)
  {}

  length_t length() const { return m_length; }
  length_t decimals() const { return m_decimals; }
  collation_id_t collation() const { return m_cs; }

  friend class Stmt_op;
  friend class Cursor;
};


typedef std::map<col_count_t, Col_metadata>  Mdata_storage;


// ---------------------------------------------------------

/*
  Asynchronous operation which sends a command to the server and processes
  its reply. In this base form it sends single command and expects simple OK
  reply from the server. Errors and notices are processed as well. Derived
  classes can extend functionality by pipelining more commands and doing more
  complex reply processing.

  Asynchronous sending of command(s) to the server is implemented
  by do_send() method. It can be called several times to drive the sending
  operation(s) forward until it returns true. Default implementation
  drives single protocol operation returned by send_cmd() which is supposed
  to send a single command to the server.

  After sending commands (when stmt_sent() returns true), server reply is
  processed. By default single OK reply from the server is expected (m_state
  equals OK). If at this point state is changed from OK to MDATA, then a full
  server reply is processed instead.

  When processing full server reply and the operation completes then it can
  be in one of the following states.

  - There are no (more) result sets in the reply (check_results() returns
    false). In this case processing server reply is completed and statement
    execution statistic are available (has_stats() returns true).

  - A result set is available for consuming with a cursor (check_results()
    returns true). Member m_nr_cols stores number of columns in the result set
    and m_col_metadata stores its meta-data. One can create a cursor to get
    rows from the result set or call discard_result() after which the operation
    will discard this result set.

  - Current result set has been consumed with a cursor or discarded after
    a call to discard_result(). If check_results() returns true indicating
    that there is another result set in the reply, one has to call
    next_result() to continue processing the reply. Creating a cursor will
    also proceed to the next result set (as it calls next_result() internally).

  A loop for processing a complete server reply using instance op of Stmt_op
  might look as follows:

    while (op.check_results())  // note: does implicit op.wait()
    {
      Cursor c(op);
      // access meta-data using cursor
      // read rows using cursor
    }

  When the cursor is destroyed, it discards any remaining rows in the current
  result set, so that the next check_esults() call will inform about existence
  of another result set in the reply.

  Below is a loop which will discard all result sets without creating a cursor
  for each of them (note that there is discard() method that does the same):

    while (op.check_results())
    {
      op.discard_result();
      op.next_result();
    }

  There can be several active instances of Stmt_op in a single session. A list
  of active statement operations is built when new such operations
  are registered with the session (Session::(de)register_stmt()). Later
  operations first wait for earlier ones before doing their own job.

  Note: Derived classes can override do_send(), do_cont() and other methods
  to implement more complex statements, such as sending a pipeline of server
  commands and then processing several replies to these commands.
*/

class Stmt_op
  : public Async_op
  , public api::Event_info
  , public protocol::mysqlx::api::Db_obj
  , protected Diagnostic_arena
  , protected protocol::mysqlx::Stmt_processor
  , protected protocol::mysqlx::Mdata_processor
  , protected protocol::mysqlx::Row_processor
  , protected protocol::mysqlx::SessionState_processor
{
  friend Cursor;

public:

  using Row_processor::col_count_t;
  using Row_processor::row_count_t;

  /*
    This points at statement's session as long as the statement is active and
    registered with it.
  */

  Session *m_session = nullptr;

  /*
    If several asynchronous statements have been issued, these pointers
    build a double linked list of all currently active statements:

    - m_prev_stmt - if not null, points at the previous statement that needs
                    to be completed before this one can finish

    - m_next_stmt - if not null points at the next statement waiting for this
                    one to complete.

    The list is built when statements are registered with a session using
    Session::(de)register_stmt() methods.
  */

  Stmt_op *m_prev_stmt = nullptr;
  Stmt_op *m_next_stmt = nullptr;

  /*
    Points to a cursor which processes a result set in the reply,
    if one was created.
  */

  Cursor *m_current_cursor = nullptr;

  Stmt_op(Session &s)
  {
    s.register_stmt(this);
    // Note: m_session is set during registration.
    assert(m_session);
  }

  virtual ~Stmt_op()
  {
    discard();
    wait();
    if (m_session)
      m_session->deregister_stmt(this);
  }

  Session& get_session()
  {
    // Note: get_session() should not be called for inactive statement.
    assert(m_session);
    return *m_session;
  }

  Protocol& get_protocol()
  {
    return get_session().m_protocol;
  }

  // Async_op

  enum {
    WAIT,    // wait for previous statement(s) to send their commands
    SEND,    // sending commands to the server
    OK,      // expecting OK reply from the server
    MDATA,   // expecting result set meta-data
    ROWS,    // expecting rows of a result set
    DISCARD, // discarding rows of a result set
    NEXT,    // expecting another result set in the reply
    FINISH,  // expecting final packets in the server reply
    DONE,
    ERROR
  }
  m_state = WAIT;

  /*
    Protocol-level operation which sends commands to the server or receives
    its reply.
  */

  Proto_op  *m_op = nullptr;

  /*
   Flag which indicates that operation m_op receives result-set meta-data
   (was obtained from protocol's rcv_MetaData() method).
  */

  bool m_op_mdata = false;

  // Flag which indicates that all result sets in the reply should be discarded.

  bool m_discard = false;

  /*
    This method drives asynchronous sending commands to the server. It will
    be repeatedly called until it returns true. Default implementation sends
    a single command given by send_cmd() method.

    Note: if do_send() sets m_state to DONE or ERROR, then no reply
    is expected.
  */

  virtual bool do_send();

  /*
    Returns protocol async. operation which is used to send a single command to
    the server by the default implementation of do_send().
  */

  virtual Proto_op* send_cmd()
  {
    return nullptr;
  }

  /*
    Returns true if all outgoing commands have been already sent and this
    operation will only read packets from server.
  */

  virtual bool stmt_sent()
  {
    return m_state > SEND;
  }


  bool do_cont() override;
  void do_wait() override;
  bool is_completed() const override;

  void do_cancel() override
  {
    THROW("Stmt_op::cancel() not implemented");
  }

  const api::Event_info* get_event_info() const override
  {
    return m_prev_stmt ? this : nullptr;
  }

  // Event_info

  event_type type() const override
  {
    return ASYNC_OP;
  }


  // Reply

  /*
    Returns false if there are no (more) results in the reply. If it returns
    true, then there is a result set in the reply. It is either the current
    result set that was not yet completely consumed, or, in case the current
    result set was already consumed, there is another result set in the reply.

    Note: In the case that the current result set has been consumed and
    check_results() informs about another result set in the reply,
    next_result() must be called to proceed to this next result set.
  */

  virtual bool check_results();

  /*
    Proceed to processing next result set if the current one has been consumed.

    Returns true if the statement is prepared for reading the next result set.
    If there are no (more) result sets in the reply or if the current result
    set has not been completely consumed, next_result() will return false (as
    it does not proceed to the next result set in that case).
  */

  virtual bool next_result();

  /*
    Discard the current result set (if any).

    This method puts the statement in a state where it will discard (remaining)
    rows of the current result set (if any). The rows will be discarded when
    the statement operation continues after a call to this method.
    The operation will complete when all rows have been discarded. After that
    either:

    - there are no more result sets in the reply and the whole reply has been
      consumed (check_results() returns false), or

    - there is another result set in the reply (check_results()
      returns true). To continue processing the reply one has to call
      next_result() which will move to the next result set. Creating a cursor
      implicitly calls next_result(), so it also proceeds to processing
      the next result set.
  */

  virtual void discard_result();

  /*
    Discard complete reply with all remaining result sets.

    Calling this method sets the statement into discard mode. Completing
    the operation will consume whole reply, discarding all its result sets
    (if any).
  */

  virtual void discard()
  {
    if (m_current_cursor)
      throw_error("Discarding reply while cursor is in use");

    /*
      Call to discard_result() ensures that the current result set (if any)
      is discarded, the m_discard flag ensures that all the following result
      sets (if any) will be discarded too.
    */

    discard_result();
    m_discard = true;
  }


  // stmt statistics

  struct
  {
    row_count_t  last_insert_id;
    row_count_t  rows_affected;
    row_count_t  rows_found;
    row_count_t  rows_matched;

    void clear()
    {
      last_insert_id = 0;
      rows_affected = 0;
      rows_found = 0;
      rows_matched = 0;
    }
  }
  m_stmt_stats;

  virtual bool has_stats()
  {
    wait();
    return DONE == m_state;
  }

  virtual row_count_t affected_rows()
  {
    if (!has_stats())
      throw_error("Only available after end of query execute");
    return m_stmt_stats.rows_affected;
  }

  row_count_t last_insert_id()
  {
    if (!has_stats())
      throw_error("Only available after end of query execute");
    return m_stmt_stats.last_insert_id;
  }

  std::vector<std::string> m_generated_ids;

  const std::vector<std::string>& generated_ids()
  {
    if (!has_stats())
      throw_error("Only available after end of query execute");
    return m_generated_ids;
  }


  // diagnostics

  using Diagnostic_arena::entry_count;
  using Diagnostic_arena::get_entries;
  using Diagnostic_arena::get_error;

  // Db_obj

  string   m_name;
  string   m_schema;
  bool     m_has_schema = false;

  const string& get_name() const override
  {
    return m_name;
  }

  const string* get_schema() const override
  {
    return m_has_schema ? &m_schema : nullptr;
  }

  void set(const api::Object_ref &obj)
  {
    m_name = obj.name();
    m_has_schema = (nullptr != obj.schema());
    if (m_has_schema)
      m_schema = obj.schema()->name();
  }

  /*
    SessionState_processor
  */

  void row_stats(row_stats_t, row_count_t) override;
  void last_insert_id(insert_id_t) override;
  void generated_document_id(const std::string&) override;

  /*
     Mdata_processor (cdk::protocol::mysqlx::Mdata_processor)
  */

  cdk::scoped_ptr<Mdata_storage> m_col_metadata;
  col_count_t m_nr_cols = 0;

  void col_count(col_count_t nr_cols) override;
  void col_type(col_count_t pos, unsigned short type) override;
  void col_content_type(col_count_t pos, unsigned short type) override;
  void col_name(col_count_t pos,
                const string &name, const string &original) override;
  void col_table(col_count_t pos,
                 const string &table, const string &original) override;
  void col_schema(col_count_t pos,
                  const string &schema, const string &catalog) override;
  void col_collation(col_count_t pos, collation_id_t cs) override;
  void col_length(col_count_t pos, uint32_t length) override;
  void col_decimals(col_count_t pos, unsigned short decimals) override;
  void col_flags(col_count_t, uint32_t) override;

  // Row_processor

  bool row_begin(row_count_t) override
  {
    return false;  // ignore all rows
  }

  void done(bool eod, bool more) override;

  /*
     Stmt_processor
  */

  void execute_ok() override;
  void ok(string) override;

  void error(
    unsigned int code, short int severity,
    sql_state_t sql_state, const string &msg
  ) override
  {
    if (Severity::ERROR == severity)
      m_state = ERROR;
    add_diagnostics(severity, new Server_error(code, sql_state, msg));
  }

  void notice(
    unsigned int /*type*/, short int /*scope*/, bytes /*payload*/
  ) override;

  virtual void add_diagnostics(short int severity, Server_error *err);

};



// ---------------------------------------------------------


class Cursor
  : public Async_op
  , private protocol::mysqlx::Row_processor
{
  friend Stmt_op;

protected:

  std::shared_ptr<Stmt_op>  m_reply;
  bool     m_closed = false;
  bool     m_init = true;

  Proto_op*               m_rows_op = nullptr;
  mysqlx::Row_processor*  m_row_prc = nullptr;

  row_count_t  m_rows_limit = 0;

  bool m_limited = false;
  bool m_more_rows = false;

  Mdata_storage& get_mdata();

  const Mdata_storage& get_mdata() const
  {
    return const_cast<Cursor*>(this)->get_mdata();
  }

public:

  Cursor(const std::shared_ptr<Stmt_op> &reply);
  ~Cursor();

  void get_rows(mysqlx::Row_processor& rp);
  void get_rows(mysqlx::Row_processor& rp, row_count_t limit);
  bool get_row(mysqlx::Row_processor& rp);

  void close();


  /*
    Metadata Interface
  */

  col_count_t col_count() const
  {
    size_t cnt = get_mdata().size();
    assert(cnt <= std::numeric_limits<col_count_t>::max());
    return (col_count_t)cnt;
  }

  // Information about type and encoding format of a column

  Type_info type(col_count_t pos) const;
  const Format_info& format(col_count_t pos) const;

  // Give other information about the column (if any).

  const Column_info& col_info(col_count_t pos) const
  {
    return get_metadata(pos);
  }


  /*
    Async (cdk::api::Async_op)
  */

  bool is_completed() const;
  const cdk::api::Event_info* get_event_info() const;

private:

  const Col_metadata& get_metadata(col_count_t pos) const;
  void internal_get_rows(mysqlx::Row_processor& rp);

  /*
    Async (cdk::api::Async_op)
  */
  bool do_cont();
  void do_wait();
  void do_cancel();

  /*
    Row_processor (cdk::protocol::mysqlx::Row_processor)
  */

  bool   row_begin(row_count_t row);
  void   row_end(row_count_t row);
  void   col_null(col_count_t pos);
  void   col_unknown(col_count_t pos, int fmt);
  size_t col_begin(col_count_t pos, size_t data_len);
  size_t col_data(col_count_t pos, bytes data);
  void   col_end(col_count_t pos, size_t data_len);
  void   done(bool eod, bool more);
  bool message_end();

  void error(unsigned int code, short int severity,
    sql_state_t sql_state, const string &msg)
  {
    assert(m_reply);
    m_more_rows = false;
    m_reply->error(code, severity, sql_state, msg);
  }

  void notice(unsigned int type, short int scope, bytes payload)
  {
    assert(m_reply);
    m_reply->notice(type, scope, payload);
  }

};


}} //cdk::mysqlx



#endif // CDK_MYSQLX_SESSION_H
