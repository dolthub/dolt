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

#include <mysql/cdk/mysqlx.h>
#include "stmt.h"
#include <iostream>


namespace cdk {
namespace mysqlx {


bool Stmt_op::do_send()
{
  if (!m_op)
  {
    m_op = send_cmd();
    // note: after returning true, this is never called again
    if (!m_op)
    {
      // No command was sent, so nothing more to do.
      m_state = DONE;
      return true;
    }
  }

  return m_op->cont();
}


bool Stmt_op::do_cont()
{
  /*
    This method should be called only when the async operation is not completed
    yet. In case m_state is DONE, another do_cont() call might be needed to
    clear m_op and only then async operation becomes completed.
  */

  assert(ERROR != m_state);
  assert(DONE != m_state || m_op);
  assert(m_session);

  try {

    if (WAIT == m_state)
    {
      if (m_prev_stmt && !m_prev_stmt->stmt_sent())
        return m_prev_stmt->cont();
      m_state = SEND;
    }

    if (SEND == m_state)
    {
      /*
        When sending command(s), call do_send() until it returns true.
        Then move to OK state to read OK packet from server (unless
        next_result() is called which will change state to MDATA).
      */

      if (do_send())
      {
        m_stmt_stats.clear();
        m_op = nullptr;

        /*
          Prepare for server reply processing unless m_state was set
          to DONE or ERROR has been detected.
        */

        if (DONE == m_state || ERROR == m_state)
          return true;
        m_state = OK;
      }
      return false;
    }
    else if (!m_op)
    {
      /*
        If there is a previous statement, first complete it. If, after that
        its reply has not been completely processed we are blocked
        -- waits_for() gives information about statement we are waiting for.
      */

      if (m_prev_stmt)
      {
        assert(m_prev_stmt->stmt_sent());
        if (!m_prev_stmt->cont())
          return false;
        if (m_prev_stmt->check_results())
          return false;

        // TODO: What to do if there was an error?
        assert(m_prev_stmt->is_completed());

        /*
          Since we have completed processing the previous statement, it must
          have been the first one in the queue. Now we can remove it so that
          this statement becomes the first one. On that occasion m_prev_stmt
          will be set to NULL so that we don't enter this branch again.
        */

        m_session->deregister_stmt(m_prev_stmt);
        assert(nullptr == m_prev_stmt);
      }

      m_op_mdata = false;

      if (m_discard)
      {
        switch (m_state)
        {
        case ROWS: m_state = DISCARD; break;
        case NEXT: m_state = MDATA; break;
        default: break;
        }
      }

      switch (m_state)
      {
      case OK:
        m_op = &get_protocol().rcv_Reply(*this);
        break;
      case MDATA:
        m_nr_cols = 0;
        m_col_metadata.reset(new Mdata_storage());
        m_op = &get_protocol().rcv_MetaData(*this);
        m_op_mdata = true;
        break;
      case DISCARD:
        m_op = &get_protocol().rcv_Rows(*this);
        break;
      case FINISH:
        m_op = &get_protocol().rcv_StmtReply(*this);
        break;
      default:
        break;
      }
    }

    if (m_op && !m_op->cont())
      return false;

    if (ERROR == m_state)
      return true;

    m_op = nullptr;

    /*
      If we completed receiving meta-data and non-zero column count was
      reported we expect rows from a result set (and the operation is completed
      for now). Otherwise there is no result set and we proceed to FINISH state.
    */

    if (m_op_mdata)
    {
      m_state = m_nr_cols > 0 ? (m_discard ? DISCARD : ROWS) : FINISH;
    }

    return is_completed();

  }
  catch (...)
  {
    m_state = ERROR;
    throw;
  }
}


bool Stmt_op::is_completed() const
{
  if (!m_session)
  {
    assert(DONE == m_state || ERROR == m_state);
    return true;
  }

  switch (m_state)
  {
  case ERROR:
    return true;
  case ROWS:
  case NEXT:
    if (m_discard)
      return false;
    FALLTHROUGH;
  case DONE:
    /*
      In one of these states we still continue until do_cont() finishes the
      m_op and does clean ups (which should reset m_op to NULL).
    */
    return nullptr == m_op;
  default:
    return false;
  }
}


void Stmt_op::do_wait()
{
  while (!cont())
  {
    if (m_op)
      m_op->wait();

    //  Break a deadlock if previous reply is not completely consumed.

    if (
      m_prev_stmt
      && m_prev_stmt->is_completed()
      && m_prev_stmt->check_results()
    )
      throw_error("Reply blocked by a previous one.");
  }
}


void Stmt_op::ok(string)
{
  m_state = DONE;
}

void Stmt_op::execute_ok()
{
  m_state = DONE;
}

void Stmt_op::done(bool eod, bool more)
{
  if (!eod)
    return;

  m_state = more ? (m_discard ? MDATA : NEXT) : FINISH;
}


void Stmt_op::discard_result()
{
  if (m_discard || ERROR == m_state)
    return;

  // Finish current activity to see if we have any pending rows.

  wait();
  assert(!m_op || ERROR == m_state);

  switch (m_state)
  {
  case ROWS:
    assert(!m_current_cursor);
    m_state = DISCARD;
    return;
  case NEXT:
  case DONE:
  case ERROR:
    return;
  default:
    assert(false);
  }
}


bool Stmt_op::check_results()
{
  wait();
  return (ROWS == m_state) || (NEXT == m_state);
}


bool Stmt_op::next_result()
{
  if (!check_results())
    return false;

  if (NEXT != m_state)
    return false;

  m_state = MDATA;

  /*
    Note: we need to process meta-data because only then we know if we have
    a result set (m_state is ROWS) or not.
  */

  wait();

  assert((ROWS == m_state) || (DONE == m_state) || (ERROR == m_state));
  return ROWS == m_state;
}


// =====================================================================

/*
  Processing reply notices.
*/

void Stmt_op::last_insert_id(insert_id_t val)
{
  m_stmt_stats.last_insert_id = val;
}

void Stmt_op::row_stats(row_stats_t stats, row_count_t val)
{
  switch (stats)
  {
  case ROWS_AFFECTED: m_stmt_stats.rows_affected = val; return;
  case    ROWS_FOUND: m_stmt_stats.rows_found = val;    return;
  case  ROWS_MATCHED: m_stmt_stats.rows_matched = val;  return;
  }
}

void Stmt_op::generated_document_id(const std::string& id)
{
  m_generated_ids.push_back(id);
}


void Stmt_op::notice(unsigned int type, short int scope, bytes payload)
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

void Stmt_op::add_diagnostics(short int severity, Server_error *err)
{
  Severity::value level;
  switch (severity)
  {
  case 0: level = Severity::INFO; break;
  case 1: level = Severity::WARNING; break;
  case 2:
  default:
    level = Severity::ERROR;
    break;
  }
  add_entry(level, err);
}


// =====================================================================

/*
  Processing result set meta-data.
*/


void Stmt_op::col_count(col_count_t nr_cols)
{
  //When all columns metadata arrived...
  m_nr_cols = nr_cols;
  //m_has_results = m_nr_cols != 0;
}


void Stmt_op::col_type(col_count_t pos, unsigned short type)
{
  if (m_discard)
    return;

  (*m_col_metadata)[pos].m_type = type;
}


void Stmt_op::col_content_type(col_count_t pos, unsigned short type)
{
  if (m_discard)
    return;

  (*m_col_metadata)[pos].m_content_type = type;
}

// TODO: original name should be optional (pointer)

void Stmt_op::col_name(col_count_t pos,
                       const string &name, const string &original)
{
  if (m_discard)
    return;

  Col_metadata &md= (*m_col_metadata)[pos];

  md.m_name= name;
  md.m_name_original = original;
  md.m_has_name_original= true;
}


void Stmt_op::col_table(col_count_t pos,
                        const string &table, const string &original)
{
  if (m_discard)
    return;

  Col_metadata &md= (*m_col_metadata)[pos];

  md.m_has_table= true;
  md.m_table.m_name= table;
  md.m_table.m_name_original = original;
  md.m_table.m_has_name_original= true;
}


// TODO: catalog is optional - should be a pointer?

void Stmt_op::col_schema(col_count_t pos,
                         const string &schema, const string &catalog)
{
  if (m_discard)
    return;

  Col_metadata &md= (*m_col_metadata)[pos];

  md.m_table.m_has_schema= true;
  md.m_table.m_schema.m_name= schema;
  md.m_table.m_schema.m_catalog.m_name = catalog;
}


void Stmt_op::col_collation(col_count_t pos, collation_id_t cs)
{
  if (m_discard)
    return;

  (*m_col_metadata)[pos].m_cs = cs;
}


void Stmt_op::col_length(col_count_t pos, uint32_t length)
{
  if (m_discard)
    return;

  (*m_col_metadata)[pos].m_length = length;
}


void Stmt_op::col_decimals(col_count_t pos, unsigned short decimals)
{
  if (m_discard)
    return;

  (*m_col_metadata)[pos].m_decimals = decimals;
}


void Stmt_op::col_flags(col_count_t pos, uint32_t flags)
{
  if (m_discard)
    return;

  (*m_col_metadata)[pos].m_flags = flags;
}


// =====================================================================

/*
  Cursor class
*/


Cursor::Cursor(const std::shared_ptr<Stmt_op> &reply)
  : m_reply(reply)
{
  assert(m_reply);

  if (m_reply->m_current_cursor)
  {
    THROW("Only one cursor for now");
  }

  // complete any pending operations

  m_reply->wait();

  if (m_reply->entry_count() > 0)
    m_reply->get_error().rethrow();

  /*
    We can be in two situations here:

    1. A result set has been consumed. In this case, if there is a next
       result set in the reply, next_result() will return true and will
       prepare for processing this next result set.

    2. Otherwise we might have a current result set that has not been
       completely consumed. In that case check_results() will return
       true.

    If neither of the above holds, then we have no result set available
    and the cursor can not be created.
  */

  if (!m_reply->next_result() && !m_reply->check_results())
  {
      // TODO: better error
      throw_error("No results when creating cursor");
  }

  m_more_rows = true;
  m_reply->m_current_cursor = this;
}


Cursor::~Cursor()
{
  close();
}


Mdata_storage& Cursor::get_mdata()
{
  assert(m_reply);
  return *(m_reply->m_col_metadata);
}


void Cursor::internal_get_rows(mysqlx::Row_processor& rp)
{
  if (m_closed)
    throw_error("get_rows: Closed cursor");

  // wait previous get_rows();
  if (m_rows_op)
    m_rows_op->wait();

  if (!m_more_rows)
  {
    m_rows_op = NULL;
    m_row_prc = NULL;
    rp.end_of_data();
    return;
  }

  m_more_rows = true;
  m_rows_op = &m_reply->get_protocol().rcv_Rows(*this);
  m_row_prc = &rp;
}


void Cursor::get_rows(mysqlx::Row_processor& rp)
{
  internal_get_rows(rp);
  m_limited = false;
}

void Cursor::get_rows(mysqlx::Row_processor& rp, row_count_t limit)
{
  internal_get_rows(rp);
  m_rows_limit = limit;
  m_limited = true;
}



bool Cursor::get_row(mysqlx::Row_processor& rp)
{
  get_rows(rp, 1);
  wait();
  return m_rows_limit  == 0;
}


void Cursor::close()
{
  if (m_reply && this == m_reply->m_current_cursor)
  {
    if (m_rows_op)
      m_rows_op->wait();
    m_rows_op = nullptr;

    /*
      Discard remaining rows in the result set so that if there is another
      result set in the reply, it will become accessible (for example, if
      another cursor is created).
    */

    m_reply->m_current_cursor = nullptr;
    m_reply->discard_result();
  }

  m_reply.reset();
  m_closed = true;
}


// Meta data

/*
  Types used by X protocol must be mapped to generic type/format
  info used by CDK, as defined by cdk::Type_info and  cdk::Format_info
  types.

  TODO: Encoding formats.
*/

Type_info Cursor::type(col_count_t pos) const
{
  typedef protocol::mysqlx::col_type  col_type;
  typedef mysqlx::content_type        content_type;

  const Col_metadata &md= get_metadata(pos);

  switch (md.m_type)
  {
  case col_type::SINT:
  case col_type::UINT:
    return TYPE_INTEGER;

  case col_type::FLOAT:
  case col_type::DOUBLE:
  case col_type::DECIMAL:
    return TYPE_FLOAT;

  case col_type::TIME:
  case col_type::DATETIME:
    return TYPE_DATETIME;

  case col_type::BYTES:
    switch (md.m_content_type)
    {
    case content_type::JSON: return TYPE_DOCUMENT;
    case content_type::GEOMETRY: return TYPE_GEOMETRY;
    case content_type::XML: return TYPE_XML;
    default: return md.m_cs != BINARY_CS_ID ? TYPE_STRING : TYPE_BYTES;
    }

  case col_type::SET:
  case col_type::ENUM:
    return TYPE_STRING;

  default:
    // TODO: correctly handle all X types (BIT)
    return TYPE_BYTES;
  }
}


const Format_info& Cursor::format(col_count_t pos) const
{
  return get_metadata(pos);
}


const Col_metadata& Cursor::get_metadata(col_count_t pos) const
{
  if (!m_reply)
    THROW("Attempt to get metadata from unitialized cursor");
  Mdata_storage::const_iterator it = get_mdata().find(pos);
  if (it == get_mdata().end())
    // TODO: Report nice error if no metadata present
    THROW("No meta-data for requested column");
  return it->second;
}


// Async_op


bool Cursor::is_completed() const
{
  if (m_closed)
    return true;

  if (m_init) //m_reply && !m_reply->is_completed())
    return false;

  if (!m_rows_op)
    return true;

  return m_rows_op->is_completed();
}


bool Cursor::do_cont()
{
  assert(!m_closed);

  if (m_init)
  {
    m_init = false;
    if(m_reply)
      m_reply->wait();
  }

  if (m_rows_op)
    m_rows_op->cont();

  return is_completed();
}


void Cursor::do_wait()
{
  assert(!m_closed);

  //if (m_closed)
  //  throw_error("wait: Closed cursor");

  if (m_init && m_reply)
    m_reply->wait();
  m_init = false;

  if (m_rows_op)
  {
    m_rows_op->wait();
    assert(is_completed());
  }
}


void Cursor::do_cancel()
{
  //same as closed for now
  close();
}


const cdk::api::Event_info* Cursor::get_event_info() const
{
  if (!m_closed && m_rows_op)
    return m_rows_op->waits_for();
  return NULL;
}



/*
   Row_processor (cdk::protocol::mysqlx::Row_processor)
*/


bool Cursor::row_begin(row_count_t row)
{
  if (m_row_prc)
    return m_row_prc->row_begin(row);
  return false;
}


void Cursor::row_end(row_count_t row)
{
  if (m_row_prc)
  {
    m_row_prc->row_end(row);
    if (m_limited)
      --m_rows_limit;
  }
}


void Cursor::col_null(col_count_t pos)
{
  if (m_row_prc)
    m_row_prc->field_null(pos);
}


void Cursor::col_unknown(col_count_t /*pos*/, int /*fmt*/)
{
  //TODO: How to match this cdk::mysqlx::Row_processor vs cdk::protocol::mysqlx::Row_processor
  //      Ignore for now
}


size_t Cursor::col_begin(col_count_t pos, size_t data_len)
{
  if (m_row_prc)
    return m_row_prc->field_begin(pos, data_len);
  return 0;
}


size_t Cursor::col_data(col_count_t pos, bytes data)
{
  if (m_row_prc)
    return m_row_prc->field_data(pos, data);
  return 0;
}


void Cursor::col_end(col_count_t pos, size_t /*data_len*/)
{
  // TODO: data_len
  if (m_row_prc)
    m_row_prc->field_end(pos);
}


void Cursor::done(bool eod, bool more)
{
  if (eod && m_row_prc)
    m_row_prc->end_of_data();

  m_more_rows = !eod;
  m_rows_op = nullptr;

  if (m_reply)
    m_reply->done(eod, more);
}

/*
  FIXME: The logic to call done(false, false) when all requested rows
  have been read could/should be implemented somewhere on the protocol
  level (Rcv_result class in rset.cc?).
*/

bool Cursor::message_end()
{
  if (!m_row_prc)
    return true;
  if (!m_limited || 0 < m_rows_limit)
    return true;

  done(false, false);
  return false;
}


}} // cdk mysqlx




