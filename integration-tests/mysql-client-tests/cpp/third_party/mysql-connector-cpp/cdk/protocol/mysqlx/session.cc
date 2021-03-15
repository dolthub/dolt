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
  Implementation of mysqlx protocol API: session handling
  =======================================================
*/


#include "protocol.h"
#include "builders.h"

PUSH_SYS_WARNINGS_CDK
#include <iostream>
POP_SYS_WARNINGS_CDK

PUSH_PB_WARNINGS
#include "protobuf/mysqlx_session.pb.h"
#include "protobuf/mysqlx_crud.pb.h"
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


// Client-side API


struct Cap_builder : api::Any::Document::Processor
{
  Mysqlx::Connection::Capabilities *m_msg;
  Any_builder m_ab;

  Cap_builder() : m_msg(NULL)
  {}

  void reset(Mysqlx::Connection::CapabilitiesSet &msg)
  {
    m_msg = msg.mutable_capabilities();
  }

  Any_prc* key_val(const string &key)
  {
    Mysqlx::Connection::Capability *cap = m_msg->add_capabilities();
    cap->set_name(key);
    m_ab.reset(*cap->mutable_value());
    return &m_ab;
  }
};

void Protocol::start_Pipeline()
{
  get_impl().start_Pipeline();
}

Protocol::Op& Protocol::snd_Pipeline()
{
  return get_impl().snd_Pipeline();
}

void Protocol::clear_Pipeline()
{
  get_impl().clear_Pipeline();
}

void Protocol::set_compression(Compression_type::value compression_type,
                               size_t threshold)
{
  get_impl().set_compression(compression_type, threshold);
}

Protocol::Op& Protocol::snd_CapabilitiesSet(const api::Any::Document& caps)
{
  Mysqlx::Connection::CapabilitiesSet msg;
  Cap_builder builder;
  builder.reset(msg);
  caps.process(builder);
  return get_impl().snd_start(msg, msg_type::cli_CapabilitiesSet);
}


Protocol::Op& Protocol::snd_AuthenticateStart(const char* mechanism,
                                              bytes data,
                                              bytes response)
{
  Mysqlx::Session::AuthenticateStart auth_start;

  auth_start.set_mech_name(mechanism);
  auth_start.set_auth_data((const void*)data.begin(),
                           data.size());

  auth_start.set_initial_response((const void*)response.begin(),
                                  response.size());
  return get_impl().snd_start(auth_start, msg_type::cli_AuthenticateStart);
}


Protocol::Op& Protocol::snd_AuthenticateContinue(bytes data)
{
  Mysqlx::Session::AuthenticateContinue auth_cont;

  auth_cont.set_auth_data((const void*)data.begin(), data.size());

  return get_impl().snd_start(auth_cont, msg_type::cli_AuthenticateContinue);
}

struct Expectation_builder : api::Expectations::Processor, api::Expectation_processor
{
  Mysqlx::Expect::Open *m_msg;

  Expectation_builder(Mysqlx::Expect::Open *msg) : m_msg(msg)
  {}

  void set(uint32_t key)
  {
    Mysqlx::Expect::Open_Condition *cond = m_msg->add_cond();
    cond->set_op(Mysqlx::Expect::Open_Condition_ConditionOperation_EXPECT_OP_SET);
    cond->set_condition_key(key);
  }

  void set(uint32_t key, bytes data)
  {
    Mysqlx::Expect::Open_Condition *cond = m_msg->add_cond();
    cond->set_op(Mysqlx::Expect::Open_Condition_ConditionOperation_EXPECT_OP_SET);
    cond->set_condition_key(key);
    cond->set_condition_value(data.begin(), data.size());
  }

  void unset(uint32_t key)
  {
    Mysqlx::Expect::Open_Condition *cond = m_msg->add_cond();
    cond->set_op(Mysqlx::Expect::Open_Condition_ConditionOperation_EXPECT_OP_UNSET);
    cond->set_condition_key(key);
  }

  Element_prc *list_el()
  {
    return this;
  }

};


Protocol::Op& Protocol::snd_Expect_Open(api::Expectations &exp, bool reset)
{
  Mysqlx::Expect::Open ex_open;
  Expectation_builder builder(&ex_open);
  exp.process(builder);
  ex_open.set_op(reset ? Mysqlx::Expect::Open_CtxOperation_EXPECT_CTX_EMPTY :
                         Mysqlx::Expect::Open_CtxOperation_EXPECT_CTX_COPY_PREV);
  return get_impl().snd_start(ex_open, msg_type::cli_ExpectOpen);
}


Protocol::Op& Protocol::snd_Expect_Close()
{
  Mysqlx::Expect::Close ex_close;
  return get_impl().snd_start(ex_close, msg_type::cli_ExpectClose);
}


class Rcv_auth_base : public Op_rcv
{
public:

  Rcv_auth_base(Protocol_impl &proto) : Op_rcv(proto)
  {}

  void resume(Auth_processor &prc)
  {
    read_msg(prc);
  }

  Next_msg do_next_msg(msg_type_t type)
  {
    switch (type)
    {
    case msg_type::AuthenticateOk:
    case msg_type::AuthenticateContinue:
      return EXPECTED;
    default: return UNEXPECTED;
    }
  }

  template<class MSG, class PRC>
  void process_msg_with(MSG&, PRC&)
  {
    // TODO: better error description (message/processor type info)
    throw_error("Invalid processor used to process server reply");
  }
};


template<>
void Rcv_auth_base::process_msg_with(Mysqlx::Session::AuthenticateOk &msg,
                                Auth_processor &prc)
{
  bytes data((byte*)msg.auth_data().data(), msg.auth_data().length());
  prc.auth_ok(data);
}

template<>
void Rcv_auth_base::process_msg_with(Mysqlx::Session::AuthenticateContinue &msg,
                                Auth_processor &prc)
{
  bytes data((byte*)msg.auth_data().data(), msg.auth_data().length());
  prc.auth_continue(data);
}


class Rcv_auth : public Message_dispatcher<Rcv_auth_base>
{
public:

  Rcv_auth(Protocol_impl &impl) : Dispatcher(impl)
  {}

  void do_process_msg(msg_type_t type, Message &msg)
  {
    Dispatcher::process_msg_with(type, msg, *static_cast<Auth_processor*>(m_prc));
  }
};



Protocol::Op& Protocol::rcv_AuthenticateReply(Auth_processor &prc)
{
  return get_impl().rcv_start<Rcv_auth>(prc);
}


// Parsing and processing protocol notices.

template<>
void process_notice<notice_type::SessionStateChange>(
  const bytes &notice,
  SessionState_processor &prc
  )
{
  Mysqlx::Notice::SessionStateChanged msg;

  if (!msg.ParseFromString(std::string(notice.begin(), notice.end())))
    THROW("Could not parse notice payload");

#ifdef DEBUG_PROTOBUF

  using std::cerr;
  using std::endl;

  cerr << endl;
  cerr << "<--- Notice payload:" << endl;
  cerr << msg.DebugString();
  cerr << "<---" << endl << endl;

#endif

  switch (msg.param())
  {
  case Mysqlx::Notice::SessionStateChanged::CLIENT_ID_ASSIGNED:
  {
    assert(msg.value_size() == 1 && msg.value(0).has_v_unsigned_int());
    uint64_t id = msg.value(0).v_unsigned_int();
    assert(id < std::numeric_limits<unsigned long>::max());
    prc.client_id((unsigned long)id);
    break;
  }

  case Mysqlx::Notice::SessionStateChanged::ACCOUNT_EXPIRED:
    prc.account_expired();
    break;

  case Mysqlx::Notice::SessionStateChanged::CURRENT_SCHEMA:
    assert(msg.value_size() == 1 && msg.value(0).has_v_string());
    // NOTE: Assuming the reported schema name is in utf8 encoding
    prc.current_schema(msg.value(0).v_string().value());
    break;

  case Mysqlx::Notice::SessionStateChanged::ROWS_AFFECTED:
    assert(msg.value_size() == 1 && msg.value(0).has_v_unsigned_int());
    prc.row_stats(prc.ROWS_AFFECTED, msg.value(0).v_unsigned_int());
    break;

  case Mysqlx::Notice::SessionStateChanged::ROWS_FOUND:
    assert(msg.value_size() == 1 && msg.value(0).has_v_unsigned_int());
    prc.row_stats(prc.ROWS_FOUND, msg.value(0).v_unsigned_int());
    break;

  case Mysqlx::Notice::SessionStateChanged::ROWS_MATCHED:
    assert(msg.value_size() == 1 && msg.value(0).has_v_unsigned_int());
    prc.row_stats(prc.ROWS_MATCHED, msg.value(0).v_unsigned_int());
    break;

  case Mysqlx::Notice::SessionStateChanged::GENERATED_INSERT_ID:
    assert(msg.value_size() == 1 && msg.value(0).has_v_unsigned_int());
    prc.last_insert_id(msg.value(0).v_unsigned_int());
    break;

  case Mysqlx::Notice::SessionStateChanged::TRX_COMMITTED:
    prc.trx_event(prc.COMMIT);
    break;

  case Mysqlx::Notice::SessionStateChanged::TRX_ROLLEDBACK:
    prc.trx_event(prc.ROLLBACK);
    break;

  case Mysqlx::Notice::SessionStateChanged::PRODUCED_MESSAGE:
  default: break;

  case Mysqlx::Notice::SessionStateChanged::GENERATED_DOCUMENT_IDS:
    for (auto it = msg.value().begin();
         it != msg.value().end();
         ++it)
    {
      prc.generated_document_id(it->v_octets().value());
    }
    break;
  }
}

template<>
void process_notice<notice_type::Warning>(
  const bytes &notice,
  Error_processor &prc
  )
{
  Mysqlx::Notice::Warning msg;

  if (!msg.ParseFromString(std::string(notice.begin(), notice.end())))
    THROW("Could not parse notice payload");

#ifdef DEBUG_PROTOBUF

  using std::cerr;
  using std::endl;

  cerr << endl;
  cerr << "<--- Notice payload:" << endl;
  cerr << msg.DebugString();
  cerr << "<---" << endl << endl;

#endif

  short int level;

  switch (msg.level())
  {
  case Mysqlx::Notice::Warning::ERROR:   level = 2; break;
  case Mysqlx::Notice::Warning::WARNING: level = 1; break;
  case Mysqlx::Notice::Warning::NOTE:
  default:
    level = 0; break;
  }

  prc.error(msg.code(), level, sql_state_t(), msg.msg());
}


// Server-side API


Protocol::Op& Protocol_server::snd_AuthenticateContinue(bytes data)
{
  Mysqlx::Session::AuthenticateContinue auth_cont;
  auth_cont.set_auth_data(data.begin(), data.size());
  return get_impl().snd_start(auth_cont, msg_type::AuthenticateContinue);
}

Protocol::Op& Protocol_server::snd_AuthenticateOK(bytes data)
{
  Mysqlx::Session::AuthenticateOk msg_auth_ok;
  msg_auth_ok.set_auth_data(data.begin(), data.size());
  return get_impl().snd_start(msg_auth_ok, msg_type::AuthenticateOk);
}


class Rcv_init_base : public Op_rcv
{
public:

  Rcv_init_base(Protocol_impl &proto) : Op_rcv(proto)
  {}

  void resume(Init_processor &prc)
  {
    read_msg(prc);
  }

  Next_msg next_msg(msg_type_t type)
  {
    switch (type)
    {
    case msg_type::cli_AuthenticateStart:
    case msg_type::cli_AuthenticateContinue:
      return EXPECTED;
    default: return UNEXPECTED;
    }
  }

  template<class MSG, class PRC>
  void process_msg_with(MSG&, PRC&)
  {
    // TODO: better error description (message/processor type info)
    throw_error("Invalid processor used to process server reply");
  }
};


template<>
void Rcv_init_base::process_msg_with(Mysqlx::Session::AuthenticateStart &msg,
                                Init_processor &ip)
{
   bytes data((byte*)msg.auth_data().data(), msg.auth_data().length());
   bytes response((byte*)msg.initial_response().data(), msg.initial_response().length());
   ip.auth_start(msg.mech_name().c_str(), data, response);
}

template<>
void Rcv_init_base::process_msg_with(Mysqlx::Session::AuthenticateContinue &msg,
                                Init_processor &ip)
{
  bytes data((byte*)msg.auth_data().data(), msg.auth_data().length());
  ip.auth_continue(data);
}


class Rcv_init : public Message_dispatcher<Rcv_init_base>
{
public:

  Rcv_init(Protocol_impl &impl) : Dispatcher(impl)
  {}

  void process_msg(msg_type_t type, Message &msg)
  {
    Dispatcher::process_msg_with(type, msg, *static_cast<Init_processor*>(m_prc));
  }
};


Protocol::Op& Protocol_server::rcv_InitMessage(Init_processor &prc)
{
  return get_impl().rcv_start<Rcv_init>(prc);
}


}}}  // cdk::protocol::mysqlx

