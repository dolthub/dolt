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

/*
  Implementation of mysqlx protocol API: SQL statement execution
  ==============================================================
*/


#include "protocol.h"
#include "builders.h"

PUSH_PB_WARNINGS
#include "protobuf/mysqlx_sql.pb.h"
POP_PB_WARNINGS


using namespace cdk::foundation;
using namespace google::protobuf;
using namespace cdk::protocol::mysqlx;


/*
  Implementation of Protocol methods using the internal implementation.
*/

namespace cdk {
namespace protocol {
namespace mysqlx {


/*
  To use Array_builder template to create a builder for storing statement
  arguments inside Mysqlx::Sql::StmtExecute message, define Arr_msg_traits<>
  for this type of message. The traits tell that adding new element to
  the array means adding a new argument inside StmtExecute message.
*/

template<>
struct Arr_msg_traits<Mysqlx::Sql::StmtExecute>
{
  typedef Mysqlx::Sql::StmtExecute Array;
  typedef Mysqlx::Datatypes::Any   Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_args();
  }
};

template<>
struct Arr_msg_traits<Mysqlx::Prepare::Execute>
{
  typedef Mysqlx::Prepare::Execute Array;
  typedef Mysqlx::Datatypes::Any   Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_args();
  }
};

template <class MSG> void set_args_(const api::Any_list &args, MSG &msg)
{
  Array_builder<Any_builder, MSG> args_builder;
  args_builder.reset(msg);
  args.process(args_builder);
}

template<msg_type::value T>
void Msg_builder<T>::set_args(const api::Any_list *args)
{
  if (args)
  {
    if (m_stmt_id != 0)
      set_args_(*args, m_prepare_execute);
    else
    {
      set_args_(*args, m_msg);
    }
  }
}


Protocol::Op& Protocol::snd_StmtExecute(uint32_t stmt_id,
                                        const char *ns,
                                        const string &stmt,
                                        const api::Any_list *args)
{
  Msg_builder<msg_type::msg_type::cli_StmtExecute> stmt_exec(get_impl(), stmt_id);

  stmt_exec.set_args(args);

  auto &msg = stmt_exec.msg();

  if (ns)
    msg.set_namespace_(ns);

  msg.set_stmt(stmt);

  return stmt_exec.send();
}


Protocol::Op& Protocol_server::snd_StmtExecuteOk()
{
  Mysqlx::Sql::StmtExecuteOk ok;
  return get_impl().snd_start(ok, msg_type::StmtExecuteOk);
}


Protocol::Op&
Protocol::snd_PrepareExecute(uint32_t stmt_id,
                             const api::Any_list *args)
{
  auto& prepare_execute = get_impl().m_prepare_execute;

  if (args)
  {
    prepare_execute.Clear();
    set_args_(*args, prepare_execute);
  }

  prepare_execute.set_stmt_id(stmt_id);

  return get_impl().snd_start(prepare_execute, msg_type::cli_PrepareExecute);
}


}}}  // cdk::protocol::mysqlx


namespace cdk {
namespace protocol {
namespace mysqlx {


// Commented out because of protocol changes

#if 0

template<>
void Protocol::Impl::process_msg_with(Mysqlx::Sql::CursorFetchDone &msg,
                                 Stmt_processor &prc)
{
  throw_error("Invalid rcv_StmtReply() after snd_StmtExecute() that"
              " produced results");
}

template<>
void Protocol::Impl::process_msg_with(Mysqlx::Sql::PrepareStmtOk &msg,
                                 Stmt_processor &prc)
{
  prc.prepare_ok();
}
*/

template<>
void Protocol::Impl::process_msg_with(Mysqlx::Sql::PreparedStmtExecuteOk &msg,
                                 Stmt_processor &prc)
{
  prc.rows_affected(msg.rows_affected());
  prc.last_insert_id(msg.last_insert_id());
  prc.execute_ok();
}

template<>
void Protocol::Impl::process_msg_with(Mysqlx::Ok &msg,
                                 Stmt_processor &prc)
{
  prc.stmt_close_ok();
}


template<>
void Protocol::Impl::process_msg_with(Mysqlx::Sql::CursorCloseOk &msg,
                                 Stmt_processor &prc)
{
  prc.cursor_close_ok();
}

#endif


}}}  // cdk::protocol::mysqlx

