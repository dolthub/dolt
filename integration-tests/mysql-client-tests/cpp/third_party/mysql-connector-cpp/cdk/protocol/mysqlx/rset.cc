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
  Implementation of mysqlx protocol API: result sets
  ==================================================

  Class Rcv_result implements an asynchronous operation which reads server
  reply after a query or command. It derives from Op_rcv using the general
  message processing framework defined there.
*/


#include "protocol.h"

PUSH_PB_WARNINGS
#include "protobuf/mysqlx_sql.pb.h"
POP_PB_WARNINGS


using namespace cdk::foundation;
using namespace google::protobuf;
using namespace cdk::protocol::mysqlx;

namespace cdk {
namespace protocol {
namespace mysqlx {


/*
  Base of asynchronous operation that processes server reply to a query
  or statement.

  Such reply is processed in several stages, each stage initiated by an
  appropriate Protocol::rcv_XXX() call (and user is responsible for making
  these calls in the correct order, see docs/???).

  The Rcv_result class inherits from Op_rcv and implements a multi-stage
  receive operation. It means that after creating the operation it signals
  completion (via is_completed() method) at the end of the first processing
  stage (such as reading meta-data). To continue processing server reply,
  the operation should be resumed, after which it can be continued again until
  the next stage is completed. If there are no more processing stages to
  be done, method is_done() returns true (and then it is an error to resume
  the operation). Thus, the usage pattern for Rcv_result operation is as
  follows:

    Rcv_result op(...);

    // perform first stage of processing

    while (!op.is_completed())
      op.cont();

    // first stage is completed, but more might be needed

    if (!op.is_done())
    {
      // start next stage of processing
      op.resume(...);
      // execute it
      while (!op.is_completed())
        op.cont();
    }

*/

class Rcv_result_base : public Op_rcv
{
public:

  Rcv_result_base(Protocol_impl&);

  void resume(Stmt_processor&);
  void resume(Row_processor&);
  void resume(Mdata_processor&);

protected:

  /*
    State tells at which stage the processing of the server reply is.

      state MDATA { reading result-set meta-data (if any) }
      state ROWS  { reading result-set rows (if any) }
      state CLOSE { reding closing StmtExecuteOk packet }
      state DONE  {
        processing server reply is completed and there are no more
        stages to be performed.
      }
  */

  enum result_state_t
  { START, MDATA, ROWS, CLOSE, DONE };

  result_state_t m_result_state =START, m_next_state;
  row_count_t m_rcount;
  col_count_t m_ccount;

  bool is_done() const
  {
    return DONE == m_result_state;
  }

  /*
    These functions determine how to process each incoming message and
    whether to continue reading the following messages after the current
    one.
  */

  Next_msg next_msg(msg_type_t);
  Next_msg do_next_msg(msg_type_t);
  bool process_next();
  bool do_process_next();

  /*
    Dispatchers for different message and processor types.

    If this method is not overridden in Rcv_result for a given <MSG,PRC>
    pair, then it means a miss-match and an error is thrown.
  */

  template<class MSG, class PRC>
  void process_msg_with(MSG&, PRC&)
  {
    // TODO: better error description (message/processor type info)
    throw_error("Invalid processor used to process server reply");
  }

};


class Rcv_result : public Message_dispatcher<Rcv_result_base>
{
public:

  Rcv_result(Protocol_impl &proto) : Dispatcher(proto)
  {}

  /*
    These methods handle incoming, parsed messages. They pass them
    to appropriate process_msg_with<MSG,PRC>() dispatchers.
  */

  void process_msg(msg_type_t, Message&);
  void do_process_msg(msg_type_t, Message&);
};



Rcv_result_base::Rcv_result_base(Protocol_impl &proto)
  : Op_rcv(proto)
  , m_result_state(START)
  , m_rcount(0)
  , m_ccount(0)
{
}


/*
  Once created, Rcv_result operation is resumed after each completed
  processing stage. This starts new processing stage which uses new
  processor to report data from this stage. The processor type must
  match the processing stage. If this is the case, new stage is started
  by initiating reading of the next message (read_msg() method).
*/

void Rcv_result_base::resume(Mdata_processor &prc)
{
  if (START != m_result_state && MDATA != m_result_state)
    throw_error("Rcv_result: incorrect resume: attempt to read meta-data"); //TODO: Improve error report
  m_ccount = 0;
  m_completed = false;
  read_msg(prc);
}

void Rcv_result_base::resume(Stmt_processor &prc)
{
  if (CLOSE != m_result_state || !m_completed)
    throw_error("Rcv_result: incorrect resume: attempt to read final OK"); //TODO: Improve error report
  m_completed= false;
  read_msg(prc);
}

void Rcv_result_base::resume(Row_processor &prc)
{
  if (ROWS != m_result_state || !m_completed)
    throw_error("Rcv_result: incorrect resume: attempt to read rows"); //TODO: Improve error report

  // reset the row counter
  m_rcount = 0;
  m_completed= false;
  read_msg(prc);
}


/*
  Before processing each message, m_next_state is set to the current state
  of the operation, and after processing each message, m_reslut_state is set
  to value of m_next_state. This achieves two goals:

  - member m_result_state keeps the current state during message processing
    (as it is changed only *after* message is processed).

  - if processing logic do not set m_next_state, the state remains unchanged.

  To maintain the state, methods next_msg() and process_next() are overridden.
  First one is called after reading message header but before processing its
  payload and the second one is called after processing the payload.
*/

Op_rcv::Next_msg Rcv_result_base::next_msg(msg_type_t type)
{
  m_next_state = m_result_state;
  return Op_rcv::next_msg(type);
}

bool Rcv_result_base::process_next()
{
  m_result_state = m_next_state;
  return Op_rcv::process_next();
}


/*
  Determine whether next received message is expected, whether it ends the
  current processing stage and if yes, whether it will be processed in the
  next stage.

  This method is called after reading message header, but before reading and
  processing its payload (See Rcv_op::???). It maintains the state-machine
  for processing server reply. After looking at the current state the logic
  of this method determines the following things:

  - Whether message of this type is expected, if not UNEXPECTED is returned.
  - Whether this message ends current processing stage, if this is the case
    then m_completed flag is set.
  - In case current stage is ended, whether this message should be considered
    part of the ended stage or of the new stage. If it belongs to the ended
    stage then EXPECTED is returned. Otherwise STOP is returned and the message
    will be processod again when the new stage is started.
  - What is the next state after processing this message, this is stored in
    m_next_state.

  Note: current state is changed to the new one only after processing message
  payload (so that state information is correct while processing the payload).
  However, if current message will be processed in the next stage (this
  method returns STOP), then the current state iformation needs to be updated
  here.

  Structure of a server reply which is traced by the state machine.

  <reply> ::= (<rset> <more>)? StmtExecuteOk
  <more> ::= FetchDone
           | FetchDoneMoreResultsets <rset>? <more>
  <rset> ::= MetaData+ Row*

  Below are few examples of valid message sequences in server reply and how
  they are distrbuted between different processing stages:
  A = reading meta-data, B = reading rows, C = reading final OK.

  1. A:[MetaData ...] B:[Row ... FetchDone] C:[StmtExecuteOk]
  2. A:[MetaData ... FetchDone] C:[StmtExecuteOk]
  3. A:[] C:[StmtExecuteOk]
  4. A:[MetaData ...] B:[Row ... FetchDoneMoreResultsets] A:[MetaData ...] ... C:[StmtExecuteOk]
  5. A:[MetaData ...] B:[Row ... FetchDoneMoreResultsets] A:[FetchDone] C:[StmtExecuteOk]

  Example 1 is a typical result set with rows. Example 2 is a result-set
  without any rows in it. Example 3 is a server reply without a result-set
  (such as after INSERT/UPDATE statement). Example 4 is a multi-result-set.
  Example 5 shows a multi-result set with no result-set at the end (such
  sequence can be sent after stored routine exectuion).

  TODO: Handle result-set for stored routine output parameters when xplugin
  supports it.
*/

Op_rcv::Next_msg Rcv_result_base::do_next_msg(msg_type_t type)
{

  switch (m_result_state)
  {

  case START:

    if (msg_type::Ok == type)
    {
      m_next_state = DONE;
      m_completed = true;
      return EXPECTED;
    }

    m_next_state = MDATA;
    // fall through to MDATA case

  case MDATA:

    /*
      In this state we expect (<rset>? <more>)? StmtExecuteOk sequence. It can
      start with one of the following messages:

      - ColumnMetaData - in this case we have a row-set (possibly empty), we
                         continue in MDATA state until we see a row message
                         or FetchDoneXXX which terminates empty row-set;

      - Row            - such message should appear only after reading some
                         ColumnMetaData ones and it starts the sequence of rows
                         from the row-set;

      - StmtExecuteOK  - this is the case where server reply contains no result
                         set;

      - FetchDoneXXX   - this is the case where there is no <rset> part but
                         possibly more result sets follow;
    */

    switch (type)
    {

    case msg_type::ColumnMetaData: return EXPECTED;

    /*
      If we see Row message we move to ROWS state and will start next stage
      (reading rows). The Row message belongs to the next stage.
    */

    case msg_type::Row:
      if (0 == m_ccount)
        return UNEXPECTED;
      m_next_state = ROWS;
      break;

    /*
      If we see FetchDoneXXX then there are 2 cases:

      1. If there was some meta-data info before (m_ccount > 0) then we have an
      empty row-set without any rows in it. We will start row reading stage to
      report 0 rows. This message will be part of the row reading stage.

      2. If there was no meta-data info before (m_ccount == 0) then there is
      no result set. We consume the current message as part of this stage, end
      the meta-data stage and proceed to the next one (either CLOSE or MDATA
      if another rset follows).
    */

    case msg_type::FetchDone:
    case msg_type::FetchDoneMoreResultsets:
      if (0 == m_ccount)
        m_next_state = msg_type::FetchDone == type ? CLOSE : MDATA;
      else
        m_next_state = ROWS;
      break;

    /*
      If we see StmtExecuteOk then the meta-data processing stage ends and we
      proceed to the final stage. The message will be part of the next stage.
    */

    case msg_type::StmtExecuteOk:
      if (0 < m_ccount)
        return UNEXPECTED;
      m_next_state = CLOSE;
      break;

    default: return UNEXPECTED;
    };

    //  If we reached here, then the current stage is completed.
    m_completed = true;

    /*
      Processing meta-data has ended now - report the column count to
      the processor. Note that it can be 0 if no result-set (and thus
      no meta-data) was present in the reply.
    */
    static_cast<Mdata_processor*>(m_prc)->col_count(m_ccount);

    /*
      If there is no result-set (no meta-data info was seen) then we either
      look at StmtExecuteOk, which shuld be processed by the next stage, or at
      some FetchDoneXXX messages which are consumed as part of this stage
      (and ignored).
    */

    if (0 == m_ccount && msg_type::StmtExecuteOk != type)
      return EXPECTED;

    /*
      Since we stop before processing message payload, we must update
      state here as process_next() will not be called in this case.
    */
    m_result_state = m_next_state;

    return STOP;

  case ROWS:

    /*
      In this state we expect Row* <more> sequence. It can start with one of
      the following messages:

      - Row - next row from the row-set, we continue reading rows until we
        see <more>;

      - FetchDoneXXX - these messages start <more> sequence; they are consumed
        as part of this stage and the next stage starts.
    */

    switch (type)
    {
    case msg_type::Row: return EXPECTED;
    case msg_type::FetchDone:
      m_next_state = CLOSE;  // no more result-sets
      break;
    case msg_type::FetchDoneMoreResultsets:
      m_next_state = MDATA;  // proceed to next result-set



      break;
    default: return UNEXPECTED;
    };

    /*
      This stage is completed and the Fetch* message will be the last one
      processed during this stage.
    */

    m_completed= true;
    return EXPECTED;

  case CLOSE:

    /*
      In this state the final StmtExecuteOk message is expected from the
      server. After processing this message the current processing stage
      is completed.
    */

    m_completed = true;
    m_next_state = DONE;

    return msg_type::StmtExecuteOk == type ? EXPECTED : UNEXPECTED;

  case DONE:
  default:

    // No message should be processed if state is not one of the above.
    assert(false);
    return UNEXPECTED;
  }

}


/*
  Determine whether to read next message after processing the current one.

  We always want to see the next message unless current processing stage
  is completed.
*/

bool Rcv_result_base::do_process_next()
{
  return !m_completed;
}

}}}



/*
  Implementation of Protocol methods using the internal implementation.
*/

namespace cdk {
namespace protocol {
namespace mysqlx {


Protocol::Op& Protocol::rcv_Rows(Row_processor &prc)
{
  return get_impl().rcv_start<Rcv_result>(prc);
}


Protocol::Op& Protocol::rcv_MetaData(Mdata_processor &prc)
{
  return get_impl().rcv_start<Rcv_result>(prc);
}


Protocol::Op& Protocol::rcv_StmtReply(Stmt_processor &prc)
{
  return get_impl().rcv_start<Rcv_result>(prc);
}


}}}  // cdk::protocol::mysqlx



namespace cdk {
namespace protocol {
namespace mysqlx {


/*
  Process messages received from server in ROWS context
*/


template<>
void Rcv_result_base::process_msg_with(
  Mysqlx::Resultset::FetchDoneMoreResultsets&, Row_processor &rp
)
{
  /*
    Inform the processor about finishing reading all rows from the
    current result-set, but the server has another result-set.
    Next messages will not be read automatically.
  */
  rp.done(true, true);
}



template<>
void Rcv_result_base::process_msg_with(
  Mysqlx::Resultset::FetchDone&, Row_processor &rp
)
{
  /*
    Fetching all rows from the cursor is finished.
    No need to parse because there is no payload in this message.
    Notify the processor.
  */
  rp.done(true, false);
}



template<>
void Rcv_result_base::process_msg_with(
  Mysqlx::Resultset::Row &row, Row_processor &rp
)
{
  row_count_t rcount= m_rcount++;

  if(!rp.row_begin(rcount))
    return; // skip this row if the processor doesn't want it

  col_count_t ccount = 0;

  for (RepeatedPtrField< ::std::string>::const_iterator it = row.field().begin();
        it != row.field().end(); ++it, ++ccount)
  {

    if (it->length() == 0)
    {
      rp.col_null(ccount);
      continue;
    }

    size_t read_window = rp.col_begin(ccount, it->length());
    size_t pos= 0;

    while (it->length() > pos && read_window)
    {
      size_t bytes_to_feed = it->length() - pos > read_window ? read_window : it->length() - pos;
      size_t read_window_new = rp.col_data(ccount, bytes((byte*)(it->c_str() + pos), bytes_to_feed));
      pos += read_window;
      read_window = read_window_new;
    }

    rp.col_end(ccount, it->length());
  }

  rp.row_end(rcount);
}


/*
  Process column metadata
*/


template<>
void Rcv_result_base::process_msg_with(
  Mysqlx::Resultset::ColumnMetaData &col_mdata, Mdata_processor &mdata_proc
)
{
    col_count_t ccount= m_ccount++;

    assert(col_mdata.type() < std::numeric_limits<unsigned short>::max());
    mdata_proc.col_type(ccount, static_cast<unsigned short>(col_mdata.type()));

    mdata_proc.col_name(ccount, col_mdata.name(),
      col_mdata.has_original_name() ? col_mdata.original_name() : "");

    if (col_mdata.has_table())
      mdata_proc.col_table(ccount, col_mdata.table(),
        col_mdata.has_original_table() ? col_mdata.original_table() : "");

    if (col_mdata.has_schema())
      mdata_proc.col_schema(ccount, col_mdata.schema(),
        col_mdata.has_catalog() ? col_mdata.catalog() : "");

    if (col_mdata.has_collation())
      mdata_proc.col_collation(ccount, col_mdata.collation());

    if (col_mdata.has_length())
      mdata_proc.col_length(ccount, col_mdata.length());

    if (col_mdata.has_fractional_digits())
    {
      assert(col_mdata.fractional_digits() < std::numeric_limits<unsigned short>::max());
      mdata_proc.col_decimals(ccount,
        static_cast<unsigned short>(col_mdata.fractional_digits())
      );
    }

    if (col_mdata.has_content_type())
    {
      assert(col_mdata.content_type() < std::numeric_limits<unsigned short>::max());
      mdata_proc.col_content_type(ccount,
        static_cast<unsigned short>(col_mdata.content_type()));
    }

    if (col_mdata.has_flags())
      mdata_proc.col_flags(ccount, col_mdata.flags());
}

template<>
void Rcv_result_base::process_msg_with(Mysqlx::Ok &ok, Mdata_processor &prc)
{
  prc.ok(ok.msg());
}


template<>
void Rcv_result_base::process_msg_with(
  Mysqlx::Sql::StmtExecuteOk&, Stmt_processor &prc
)
{
  prc.execute_ok();
}


void Rcv_result::process_msg(msg_type_t type, Message &msg)
{
  // Mark operation as completed if we see error message.

  if (type == msg_type::Error)
  {
    m_next_state = DONE;
    m_completed = true;
  }

  // Invoke the default message processing

  Op_rcv::process_msg(type, msg);
}


void Rcv_result::do_process_msg(msg_type_t type, Message &msg)
{
  assert(m_prc);

  switch (m_result_state)
  {
  case START:
  case MDATA:
    Dispatcher::process_msg_with(type, msg, *static_cast<Mdata_processor*>(m_prc));
    break;

  case ROWS:
    Dispatcher::process_msg_with(type, msg, *static_cast<Row_processor*>(m_prc));
    break;

  case CLOSE:
    Dispatcher::process_msg_with(type, msg, *static_cast<Stmt_processor*>(m_prc));
    break;

  case DONE:
    THROW("processing message in wrong state");
  }
}



}}}  // cdk::protocol::mysqlx
