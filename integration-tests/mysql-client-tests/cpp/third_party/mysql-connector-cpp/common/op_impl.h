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

#ifndef MYSQLX_COMMON_OP_IMPL_H
#define MYSQLX_COMMON_OP_IMPL_H

#include "common.h"
#include "session.h"
#include "result.h"
#include "db_object.h"

#include <mysql/cdk.h>
#include <mysqlx/common.h>
#include <mysqlx/common/op_if.h>

PUSH_SYS_WARNINGS
#include <bitset>
#include <list>
POP_SYS_WARNINGS


/*
  This file defines a hierarchy of classes which implement executable objects
  that execute various CRUD operations.
*/

namespace mysqlx {
namespace impl {
namespace common {

enum class Object_type
{
  SCHEMA,
  COLLECTION,
  TABLE,
  VIEW
};


/*
  Base for CRUD operation implementation classes.

  This class handles the final execution of an operation, which
  is performed as follows (see method `wait`).

  1. First, appropriate CRUD operation is sent to the server using
     underlying CDK session. This produces a cdk::Reply object which
     is used for further processing. Sending the CRUD operation is
     performed by method `send_command` which should be overwritten by
     derived class. Derived class has access to the underlying CDK session
     with method `get_cdk_session()`.

  2. After getting cdk::Reply object implementation waits for it to
     receive server reply and then returns Result_base instance created
     from the cdk::Reply object.

  The Op_base template is parametrized by the implementation interface
  `IF` that derived class wants to implement (see executable.h for interface
  definitions). The Op_base template implements some of the interface methods,
  other templates and derived class should implement the rest.
*/

template <class IF>
class Op_base
  : public IF
  , protected Result_init
{
public:

  enum Prepare_state
  {
    PS_EXECUTE,
    PS_PREPARE_EXECUTE,
    PS_EXECUTE_PREPARED
  };

protected:

  using string = std::string;
  using Shared_session_impl = shared_ptr<Session_impl>;
  using Shared_stmt_id = shared_ptr<uint32_t>;

  Shared_session_impl m_sess;

  /*
    Note: using cdk::scoped_ptr to be able to transfer ownership
    to a different object.
  */
  cdk::scoped_ptr<cdk::Reply> m_reply;

  Shared_stmt_id m_stmt_id;
  Prepare_state  m_prepare_state = PS_EXECUTE;
  bool m_inited = false;
  bool m_completed = false;

public:

  Op_base(const Shared_session_impl &sess)
    : m_sess(sess)
  {}

  /*
    Note: When objects of classes exteding Op_base are copied, only definiton
    of the operation to be executed is copied. The execution state such as
    m_reply, m_inited etc. is not copied from the other instance, but is
    initialized so that the copied operation is ready to be executed.
  */

  Op_base(const Op_base& other)
    : m_sess(other.m_sess)
    , m_stmt_id(other.m_stmt_id)
    , m_prepare_state(other.m_prepare_state)
  {}

  virtual ~Op_base() override
  {
    release_stmt_id();
  }

  cdk::Session& get_cdk_session()
  {
    assert(m_sess);
    return *(m_sess->m_sess);
  }

  uint32_t create_stmt_id()
  {
    assert(m_sess);
    if (!m_stmt_id.unique())
    {
      uint32_t id = m_sess->create_stmt_id();
      if(id != 0)
        m_stmt_id.reset(new uint32_t(id));
      else
        m_stmt_id.reset();
    }
    return get_stmt_id();
  }

  void release_stmt_id()
  {
    if (m_stmt_id.unique())
      m_sess->release_stmt_id(*m_stmt_id);
    m_stmt_id.reset();
  }


  /*
    Clears operation state and, if stmt_id != 0, informe session about error on
    PS
  */

  void reset_state()
  {
    if (m_stmt_id.unique())
      get_session()->error_stmt_id(*m_stmt_id);
    m_stmt_id.reset();
    m_prepare_state = PS_EXECUTE;
    m_reply.reset();
    m_inited = false;
    m_completed = false;
  }

  uint32_t get_stmt_id()
  {
    return m_stmt_id ? *m_stmt_id.get() : 0;
  }

  Prepare_state get_prepare_state()
  {
    return m_prepare_state;
  }

  void set_prepare_state(Prepare_state x)
  {
     m_prepare_state = x;
  }

  // Async execution

  /*
    Initialize statement execution (if not already done) by sending command
    to the server. This initializes m_reply to point to a cdk::Reply object
    that waits for the server reply.
  */

  void init()
  {
    if (m_inited)
      return;
    m_inited = true;

    assert(m_sess);

    /*
      Prepare session for sending a new command. This gives session a chance
      to do necessary cleanups, such as consuming pending reply to a previous
      command.

      TODO: It should be possible to send next command while previous results
      are still being consumed (rd/wr split). But this requires changes in CDK.
      At present calling send_command(), which creates a new CDK reply object,
      discards a previous reply (if any) and this confuses the overall result
      processing logic.
    */

    m_sess->prepare_for_cmd();
    m_reply.reset(send_command());
  }

  bool is_completed()
  {
    if (m_completed)
      return true;

    init();
    m_completed = (!m_reply) || m_reply->is_completed();
    return m_completed;
  }


  /*
    Drive statement execution operation. First call init() to initialize it
    if it was not done before. Then wait for the reply object to become ready.
  */

  void cont()
  {
    if (m_completed)
      return;
    init();
    if (m_reply)
    {
      m_reply->cont();
      try {
        check_errors();

      } catch (cdk::mysqlx::Server_prepare_error&)
      {
        /*
          If we are executing prepare + execute pipeline and prepare step
          failed, try again, this time executing without prepare.
          reset_state will set stmt_id= 0 which will trigger the direct execute
        */
        reset_state();
        cont();
      }
    }
  }

  /*
    Drive statement execution until server reply is available.
  */

  void wait()
  {
    init();
    if (m_reply)
    {
      m_reply->wait();
      try {
        check_errors();

      } catch (cdk::mysqlx::Server_prepare_error&)
      {
        /*
          If we are executing prepare + execute pipeline and prepare step
          failed, try again, this time executing without prepare.
          reset_state will set stmt_id= 0 which will trigger the direct execute
        */
        reset_state();
        wait();
      }

    }
  }


  // Synchronous execution

  /*
    If an error has happened and it was ignored, then the initializer will
    return NULL from get_reply().
  */

  Result_init& execute() override
  {
    // Can not execute operation that is already completed.
    assert(!m_completed);

    execute_prepare();
    wait();
    execute_cleanup();

    return *this;
  }

protected:

  /*
    For PS operations, do_send_command should be overriden to send a command to
    the server and return a cdk object representing server's reply to that
    command. For non-PS, send_command should then be overriden. The Op_base
    instance takes ownership of the returned reply object.

    TODO: Currently send_command() allocates new cdk::Reply object on heap
    and then passes it to result object which takes ownership. Avoid dynamic
    allocation: return cdk reply initializer instead and use it to initialize
    an instance of cdk::Reply inside result object (in its implementation
    actually). This way only result implementation has to be allocated on heap.
  */

  virtual cdk::Reply* send_command() = 0;

  virtual cdk::Reply* do_send_command()
  {
    assert(false);
    return nullptr;
  }

  /*
    Either call do_send_command() to send (and possibly prepare) a command or,
    if there is an up-to-date prepared statement for the original command, send
    command that executes this prepared statement.
  */

  cdk::Reply* send_prepared_command(const cdk::Limit* limit,
                           const cdk::Param_source* param
                           )
  {
    if (use_prepared_statement())
    {
      return new cdk::Reply(get_cdk_session().prepared_execute(
                              get_stmt_id(),
                              limit,
                              param
                              ));
    }
    return do_send_command();
  }

  cdk::Reply* send_prepared_command(const cdk::Any_list* list)
  {
    if (use_prepared_statement())
    {
      return new cdk::Reply(get_cdk_session().prepared_execute(
                              get_stmt_id(),
                              list
                              ));
    }
    return do_send_command();
  }


  /*
    Hooks that are called just before and after execution of the operation.

    TODO: Currently these hooks are not called when executing asynchronously.
  */

  // LCOV_EXCL_START
  virtual void execute_prepare()
  {}

  virtual void execute_cleanup()
  {}
  // LCOV_EXCL_STOP


  /*
    Handling errors reported by server.

    Normally when server reports erros, operation throws error during execution.
    But a derived class can define a list of server errors which are ignored
    and let the operation execute successfully (with empty result).
  */

  std::set<cdk::error_code> m_skip_errors;

  void skip_error(const cdk::error_code &ec)
  {
    m_skip_errors.emplace(ec);
  }

  void clear_skip_errors()
  {
    m_skip_errors.clear();
  }

  void check_errors()
  {
    if (0 == m_reply->entry_count())
      return;

    const cdk::Error &err = m_reply->get_error();

    /*
      If error is found on the skip list, we do not throw it. But the CDK reply
      object is not valid anymore. We delete it so that later get_reply()
      returns NULL. This indicates an empty result after ignoring server error.
    */

    if (m_skip_errors.end() != m_skip_errors.find(err.code()))
      m_reply.reset();
    else
      err.rethrow();
  }


  /*
    Result_init

    Op_base implements Result_init interface methods which are used to build
    a result object representing results of the operation. These results are
    obtained from server using the cdk reply object.
  */

  Shared_session_impl get_session() override
  {
    return m_sess;
  }

  cdk::Reply* get_reply() override
  {
    if (!is_completed())
      THROW("Attempt to get result of incomplete operation");

    /*
      Server reply to the command is now passed to the result instance.
      We reset m_inited and m_completed flag so that upon next execution the
      command will be sent to the server again and a new reply will be created.
    */

    m_inited = false;
    m_completed = false;

    /*
      Note: caller takes ownership of the returned cdk::Reply object and for
      that reason we need to release() here to avoid double deletion of the
      object.
    */

    return m_reply.release();
  }

private:

  /*
    Returns true if the statement has been prepared before and that prepared
    statement should be used for execution, false if we need to execute and
    prepare the original statement (either for the first time, or because it has
    been modified).
  */
  bool use_prepared_statement()
  {
    auto prepare = get_prepare_state();

    /*
      Upon first execution, prepare is on PS_EXECUTE state and get_stmt_id() is
      0. The new statement id is not allocated yet and function returns false,
      meaning that the statement will be executed directly without preparing.
      Also, prepare is set to PS_PREPARE_EXECUTE.

      On next execution, prepare is then on PS_PREPARE_EXECUTE. Then new
      PS id is allocated and function returns false, meaning that the statement
      gets prepared and executed. Also, the state is set to PS_EXECUTE_PREPARED.

      On 3rd and following executions, if state stays PS_EXECUTE_PREPARED, this
      function will return true meaning that the prepared statement should be
      used.
    */

    if (prepare == PS_PREPARE_EXECUTE )
    {
      create_stmt_id();
    }
    else if (prepare == PS_EXECUTE)
    {
      release_stmt_id();
    }

    switch(prepare)
    {
    case PS_EXECUTE:
      set_prepare_state(PS_PREPARE_EXECUTE);
      break;
    case PS_PREPARE_EXECUTE:
      set_prepare_state(PS_EXECUTE_PREPARED);
      break;
    case PS_EXECUTE_PREPARED:
      break;
    }

    return prepare == PS_EXECUTE_PREPARED &&
        get_stmt_id()!=0;
  }
};


/*
  This template adds to the given Base class implementations for Bind_if
  interface methods which handle storing values of named parameters. It
  works only for named parameters.

  Method get_params() returns stored parameter values in the form expected by
  CDK (cdk::Param_source). It returns NULL if no parameter values were defined.
*/

template <class Base>
class Op_bind
  : public Base
  , cdk::Param_source
{
protected:

  using string = std::string;
  using Shared_session_impl = typename Base::Shared_session_impl;

  Op_bind(Shared_session_impl sess) : Base(sess)
  {}

  typedef std::map<cdk::string, Value> param_map_t;
  param_map_t m_map;


  // Parameters

  void add_param(const string &name, const Value &val) override
  {
    auto el = m_map.emplace(name, val);
    //substitute if exists
    if (!el.second)
    {
      el.first->second = val;
    }
  }

  void add_param(const string &name, const cdk::string &val)
  {
    add_param(name, Value::Access::mk_str(val));
  }

  void add_param(const string &name, const std::string &val)
  {
    add_param(name, Value(val));
  }


  void add_param(Value) override
  {
    assert(false);
  }

  void add_param(const cdk::string &val)
  {
    add_param(Value::Access::mk_str(val));
  }

  void add_param(const std::string &val)
  {
    add_param(Value(val));
  }


  void clear_params() override
  {
    m_map.clear();
  }


  // cdk::Param_source

  void process(Processor &prc) const override
  {
    prc.doc_begin();

    for (auto it : m_map)
    {
      Value_scalar val(it.second);
      val.process_if(prc.key_val(it.first));
    }

    prc.doc_end();
  }

public:

  cdk::Param_source* get_params()
  {
    return m_map.empty() ? nullptr : this;
  }

  cdk::Reply* send_command() override
  {
    return Base::send_prepared_command(nullptr, get_params());
  }
};


/*
  This tempate adds to the given Base class implementations for Limit_if
  interface methods which set limits on query results.

  Method get_limit() returns the limits in the form expected by CDK
  (cdk::Limit). It returns NULL if no limits were set.
*/

template <class Base>
class Op_limit
  : public Base
  , cdk::Limit
{
protected:

  using Shared_session_impl = typename Base::Shared_session_impl;

  Op_limit(Shared_session_impl sess) : Base(sess)
  {}

  row_count_t m_limit = 0;
  row_count_t m_offset = 0;
  bool m_has_limit = false;
  bool m_has_offset = false;

  // Limit and offset

  void set_limit(unsigned lm) override
  {
    /*
      Setting limit is not treated as changing the statement
      completely. Re-prepare is needed only if the statement
      was already prepared without any limits.
    */

    if (nullptr == get_limit() &&
        Base::get_prepare_state() == Base::PS_EXECUTE_PREPARED)
    {
      Base::set_prepare_state(Base::PS_PREPARE_EXECUTE);
    }

    m_has_limit = true;
    m_limit = lm;
  }

  void clear_limit() override
  {
    /*
      Clearing limit is not treated as changing the statement
      completely. Re-prepare is needed only if the statement
      was already prepared with limits and now it will have
      no limits (because no offset was set).
    */

    if (nullptr != get_limit() && !m_has_offset &&
        Base::get_prepare_state() == Base::PS_EXECUTE_PREPARED)
    {
      Base::set_prepare_state(Base::PS_PREPARE_EXECUTE);
    }

    m_has_limit = false;
  }


  void set_offset(unsigned offset) override
  {
    /*
      Setting offset is not treated as changing the statement
      completely. Re-prepare is needed only if the statement
      was already prepared without any limits.
    */

    if (nullptr == get_limit() &&
        Base::get_prepare_state() == Base::PS_EXECUTE_PREPARED)
    {
      Base::set_prepare_state(Base::PS_PREPARE_EXECUTE);
    }

    m_has_offset = true;
    m_offset = offset;
  }

  void clear_offset() override
  {
    /*
      Clearing offset is not treated as changing the statement
      completely. Re-prepare is needed only if the statement
      was already prepared with limits and now it will have
      no limits (because no row limit was set).
    */

    if (nullptr != get_limit() && !m_has_limit &&
        Base::get_prepare_state() == Base::PS_EXECUTE_PREPARED)
    {
      Base::set_prepare_state(Base::PS_PREPARE_EXECUTE);
    }

    m_has_offset = false;
  }


  cdk::Limit* get_limit()
  {
    return m_has_limit || m_has_offset ? this : nullptr;
  }


  cdk::Reply* send_command() override
  {
    return Base::send_prepared_command(get_limit(), Base::get_params());
  }


  // cdk::Limit interface

  row_count_t get_row_count() const override
  { return m_has_limit ? m_limit : std::numeric_limits<row_count_t>::max(); }

  const row_count_t* get_offset() const override
  {
    return m_has_offset ? &m_offset : nullptr;
  }

};


/*
  This template adds to the given Base class implementations of Sort_if
  interface methods which specify sorting of a query results.

  Method get_order_by() returns sorting specifications in the form expected by
  CDK (cdk::Order_by). It returns NULL if no order specifications were given.

  The PM template parameter tells in which mode the expressions used in order
  specifications should be parsed.
*/

template <parser::Parser_mode::value PM, class Base>
class Op_sort
  : public Base
  , cdk::Order_by
{
protected:

  using Shared_session_impl = typename Base::Shared_session_impl;
  using direction_t = typename Base::direction_t;
  using string = std::string;

  struct order_item
  {
    enum {
      ASC  = cdk::api::Sort_direction::ASC,
      DESC = cdk::api::Sort_direction::DESC,
      PARSE = ASC + DESC + 1
    } m_dir;
    string m_expr;

    order_item(const string &expr)
      : m_dir(PARSE), m_expr(expr)
    {}

    order_item(const string &expr, direction_t dir)
      : m_dir(Base::ASC == dir ? ASC : DESC), m_expr(expr)
    {}
  };

  std::list<order_item> m_order;

  void add_sort(const string &expr, direction_t dir) override
  {
    Base::set_prepare_state(Base::PS_EXECUTE);
    m_order.emplace_back(expr, dir);
  }

  void add_sort(const string &sort) override
  {
    Base::set_prepare_state(Base::PS_EXECUTE);
    m_order.emplace_back(sort);
  }

  void clear_sort() override
  {
    if (get_order_by())
      Base::set_prepare_state(Base::PS_EXECUTE);
    m_order.clear();
  }

  Op_sort(Shared_session_impl sess) : Base(sess)
  {}

public:

  cdk::Order_by* get_order_by()
  {
    return m_order.empty() ? nullptr : this;
  }

private:

  // cdk::Order_by interface

  void process(Order_by::Processor& prc) const override
  {
    prc.list_begin();

    for (const order_item &item : m_order)
    {
      auto *el = prc.list_el();
      if (!el)
        continue;

      switch (item.m_dir)
      {
      case order_item::ASC:
      case order_item::DESC:
        {
          parser::Expression_parser parser(PM, item.m_expr);
          parser.process_if(el->sort_key(
            cdk::api::Sort_direction::value(item.m_dir)
          ));
        }
        break;

      case order_item::PARSE:
        {
          parser::Order_parser order_parser(PM, item.m_expr);
          order_parser.process_if(el);
        }
        break;
      }
    }

    prc.list_end();
  }
};


/*
  This template adds to the given Base class implementations of Having_if
  interface methods which specify grouping selection criteria.

  Method get_having() returns selection criteris as CDK expression.
  It returns NULL if no selection criteria was specified.

  The PM template parameter tells in which mode the selecting expression
  should be parsed.
*/

template <parser::Parser_mode::value PM, class Base>
class Op_having
  : public Base
  , cdk::Expression
{
protected:

  using string = std::string;

  string m_having;

public:

  using Shared_session_impl = typename Base::Shared_session_impl;

  Op_having(Shared_session_impl sess) : Base(sess)
  {}

  void set_having(const string &having) override
  {
    Base::set_prepare_state(Base::PS_EXECUTE);
    m_having = having;
  }

  void clear_having() override
  {
    if (get_having())
      Base::set_prepare_state(Base::PS_EXECUTE);
    m_having.clear();
  }

  cdk::Expression* get_having()
  {
    return m_having.empty() ? nullptr : this;
  }

private:

  // cdk::Expression processor

  void process(cdk::Expression::Processor& prc) const override
  {
    parser::Expression_parser expr_parser(PM, m_having);
    expr_parser.process(prc);
  }
};


/*
  This template adds to the given Base class implementations of Group_by_if
  interface methods which specify grouping of query results.

  Method get_group_by() returns grouping specification in the form expected
  by CDK (a list of expressions). It returns NULL if no grouping was specified.

  The PM template parameter tells in which mode the grouping expressions
  should be parsed.
*/

template <parser::Parser_mode::value PM, class Base>
class Op_group_by
  : public Base
  , cdk::Expr_list
{
  using string = std::string;
  std::vector<string> m_group_by;

public:

  using Shared_session_impl = typename Base::Shared_session_impl;

  void add_group_by(const string &group_by) override
  {
    Base::set_prepare_state(Base::PS_EXECUTE);
    m_group_by.push_back(group_by);
  }

  void clear_group_by() override
  {
    if (get_group_by())
      Base::set_prepare_state(Base::PS_EXECUTE);
    m_group_by.clear();
  }

  Op_group_by(Shared_session_impl sess) : Base(sess)
  {}

  cdk::Expr_list* get_group_by()
  {
    return m_group_by.empty() ? nullptr : this;
  }

private:

  // Expr_list

  void process(cdk::Expr_list::Processor& prc) const override
  {
    prc.list_begin();

    for (string el : m_group_by)
    {
      parser::Expression_parser expr_parser(PM, el);
      expr_parser.process_if(prc.list_el());
    }

    prc.list_end();
  }
};


/*
  This template adds to the given Base class implementations of Proj_if
  interface methods which specify a projection to be applied to query results.

  The Proj_if interface handles projections for both table queries and document
  queries. Method get_tbl_proj() returns projections specified for a table
  query in the form expected by CDK (cdk::Projection). Method get_doc_proj()
  returns projection specified for a document query as a single document
  specification expected by CDK. These methods return NULL if no projections
  were specified.
*/

template <class Base>
class Op_projection
    : public Base
    , cdk::Projection
    , cdk::Expression::Document
{
protected:

  using string = std::string;

  std::vector<string> m_projections;
  string  m_doc_proj;

  using Shared_session_impl = typename Base::Shared_session_impl;

public:

  Op_projection(Shared_session_impl sess) : Base(sess)
  {}

  void set_proj(const string& doc) override
  {
    m_doc_proj = doc;
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void add_proj(const string& field) override
  {
    m_projections.push_back(field);
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void clear_proj() override
  {
    if (get_tbl_proj())
      Base::set_prepare_state(Base::PS_EXECUTE);
    m_projections.clear();
  }

  cdk::Projection* get_tbl_proj()
  {
    return m_projections.empty() ? nullptr : this;
  }

  cdk::Expression::Document* get_doc_proj()
  {
    return m_projections.empty() && m_doc_proj.empty() ? nullptr : this;
  }

private:

  // cdk::Expression::Document

  void process(cdk::Expression::Document::Processor& prc) const override
  {
    /*
      Note: If m_doc_proj is not empty then it specifies the whole projection
      document. Otherwise we build document from the list of expressions with
      aliases (m_projections).
    */

    if (!m_doc_proj.empty())
    {
      struct : public cdk::Expression::Processor
      {
        Doc_prc *m_prc;

        Scalar_prc* scalar()
        {
          common::throw_error("Scalar expression can not be used as projection");
          return nullptr;
        }

        List_prc* arr()
        {
          common::throw_error("Array expression can not be used as projection");
          return nullptr;
        }

        // Report that any value is a document.

        Doc_prc* doc()
        {
          return m_prc;
        }

      }
      eprc;

      eprc.m_prc = &prc;

      parser::Expression_parser parser(
        parser::Parser_mode::DOCUMENT, m_doc_proj
      );

      parser.process(eprc);

      return;
    }

    prc.doc_begin();

    for (string field : m_projections)
    {
      parser::Projection_parser parser(
        parser::Parser_mode::DOCUMENT, field
      );

      parser.process(prc);
    }

    prc.doc_end();

  }

  // cdk::Projection

  void process(cdk::Projection::Processor& prc) const override
  {
    prc.list_begin();

    for (string el : m_projections)
    {

      parser::Projection_parser order_parser(parser::Parser_mode::TABLE, el);
      auto prc_el = prc.list_el();
      if (prc_el)
        order_parser.process(*prc_el);

    }

    prc.list_end();

  }

};


/*
  This template adds to the given Base class implementations of Select_if
  interface methods which specify query selection criteria and locking mode
  for query results.

  Method get_where() returns the CDK expression specified as query selection
  criteria (or NULL if no selection criteria was specified).

  The PM template parameter tells which mode should be used to parse the
  selection expression.
*/

template <parser::Parser_mode::value PM, class Base>
class Op_select : public Base
{
protected:

  using string = std::string;
  using Shared_session_impl = typename Base::Shared_session_impl;

  string m_where_expr;
  bool   m_where_set = false;
  std::unique_ptr<parser::Expression_parser> m_expr;
  cdk::Lock_mode_value        m_lock_mode = cdk::api::Lock_mode::NONE;
  cdk::Lock_contention_value
    m_lock_contention = cdk::api::Lock_contention::DEFAULT;


  // Note: we do not copy m_expr to avoid invoking copy ctor
  // for Expression_parser

  Op_select(const Op_select &other)
    : Base(other)
    , m_where_expr(other.m_where_expr)
    , m_where_set(other.m_where_set)
    , m_lock_mode(other.m_lock_mode)
    , m_lock_contention(other.m_lock_contention)
  {}

public:

  Op_select(Shared_session_impl sess) : Base(sess)
  {}

  void set_where(const string &expr) override
  {
    m_where_expr = expr;
    m_where_set = true;
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void set_lock_mode(Lock_mode lm, Lock_contention contention) override
  {
    // Note: assumes the cdk::Lock_mode enum uses the same values as
    // common::Select_if::Lock_mode.
    m_lock_mode = cdk::Lock_mode_value(lm);
    m_lock_contention = cdk::Lock_contention_value(int(contention));
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void clear_lock_mode() override
  {
    m_lock_mode = cdk::api::Lock_mode::NONE;
    m_lock_contention = cdk::api::Lock_contention::DEFAULT;
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  cdk::Expression* get_where() const
  {
    if (m_where_expr.empty())
    {
      if (m_where_set)
        common::throw_error("Invalid selection criteria");
      return nullptr;
    }

    auto *self = const_cast<Op_select*>(this);

    self->m_expr.reset(new parser::Expression_parser(PM, m_where_expr));
    return m_expr.get();
  }
};


// ============================================================================

/*
  An operation which executes an SQL query, possibly with placeholders.

  Values of placeholders are specified using Bind_if interface.
*/

struct Op_sql
  : public Op_base<Bind_if>
{
  using string = std::string;

  using Base = Op_base<Bind_if>;

  string m_query;

  typedef std::list<Value> param_list_t;

  Op_sql(Shared_session_impl sess, const string &query)
    : Op_base(sess), m_query(query)
  {}

  /*
    An object which presents parameter values as CDK list.
  */

  struct
    : public cdk::Any_list
    , cdk::Format_info
  {
    param_list_t m_values;

    void process(Processor &prc) const override
    {
      prc.list_begin();

      for (auto &val : m_values)
      {
        Processor::Element_prc::Scalar_prc
         *sprc = safe_prc(prc)->list_el()->scalar();
        if (!sprc)
          continue;
        Value::Access::process_val(val, *sprc);
      }

      prc.list_end();
    }

    // Trivial Format_info for raw byte values

    bool for_type(cdk::Type_info) const override { return true; }
    void get_info(cdk::Format<cdk::TYPE_BYTES>&) const override {}
    using cdk::Format_info::get_info;
  }
  m_params;


  void add_param(Value val) override
  {
    m_params.m_values.emplace_back(std::move(val));
  }

  void add_param(const string&, const Value&) override
  {
    assert(false);
  }

  void clear_params() override
  {
    m_params.m_values.clear();
  }

  cdk::Any_list* get_params()
  {
    return m_params.m_values.empty() ? nullptr : &m_params;
  }


  Executable_if* clone() const override
  {
    return new Op_sql(*this);
  }

  void execute_cleanup() override
  {
    clear_params();
  }

  cdk::Reply* send_command() override
  {
    return do_send_command();
  }

  cdk::Reply* do_send_command() override
  {
    return new cdk::Reply(
          get_cdk_session().sql(
            0,
            m_query,
            get_params()
            ));
  }
};


/*
  An operation which executes admin protocol command, possibly with parameters.
*/

struct Op_admin
  : public Op_bind< Op_base<common::Bind_if> >
{
  const char *m_cmd;

  Op_admin(Shared_session_impl sess, const char *cmd)
    : Op_bind(sess), m_cmd(cmd)
  {}

  cdk::Reply* send_command() override
  {
    auto *param = get_params();
    assert(param);
    return new cdk::Reply(
      get_cdk_session().admin(m_cmd, *param)
    );
  }

  Executable_if* clone() const override
  {
    return new Op_admin(*this);
  }
};


// ----------------------------------------------------------------------

/*
  Transaction operations.

  They are implemented as Op_trx template parametrized by operation type.
*/

enum class Trx_op {
  BEGIN, COMMIT, ROLLBACK, SAVEPOINT_SET, SAVEPOINT_REMOVE
};


template <Trx_op>
struct Op_trx
  : public Op_base<common::Executable_if>
{
  Op_trx(Shared_session_impl sess)
    : Op_base(sess)
  {}

  Executable_if* clone() const override
  {
    return new Op_trx(*this);
  }

  cdk::Reply* send_command() override;
};


template<>
inline
cdk::Reply* Op_trx<Trx_op::BEGIN>::send_command()
{
  get_cdk_session().begin();
  return nullptr;
}

template<>
inline
cdk::Reply* Op_trx<Trx_op::COMMIT>::send_command()
{
  get_cdk_session().commit();
  return nullptr;
}


struct Op_trx_savepoint
  : public Op_base<common::Executable_if>
{
  using string = std::string;

  string m_name;

  Op_trx_savepoint(Shared_session_impl sess, const string &name = string())
    : Op_base(sess), m_name(name)
  {}
};


template<>
struct Op_trx<Trx_op::ROLLBACK>
  : public Op_trx_savepoint
{
  using Op_trx_savepoint::Op_trx_savepoint;

  cdk::Reply* send_command() override
  {
    get_cdk_session().rollback(m_name);
    return nullptr;
  }

  Executable_if* clone() const override
  {
    return new Op_trx(*this);
  }
};


template<>
struct Op_trx<Trx_op::SAVEPOINT_SET>
  : public Op_trx_savepoint
{
  Op_trx(Shared_session_impl sess, const string &name)
    : Op_trx_savepoint(sess, name)
  {
    if (!name.empty())
      return;

    // Generate savepoint name.

    std::stringstream savepoint;
    savepoint << "SP" << sess->next_savepoint();
    m_name = savepoint.str();
  }

  cdk::Reply* send_command() override
  {
    get_cdk_session().savepoint_set(m_name);
    return nullptr;
  }

  const string& get_name() const
  {
    return m_name;
  }

  Executable_if* clone() const override
  {
    return new Op_trx(*this);
  }
};


template<>
struct Op_trx<Trx_op::SAVEPOINT_REMOVE>
  : public Op_trx_savepoint
{
  Op_trx(Shared_session_impl sess, const string &name)
    : Op_trx_savepoint(sess, name)
  {
    if (name.empty())
      common::throw_error("Invalid empty save point name");
  }

  cdk::Reply* send_command() override
  {
    get_cdk_session().savepoint_remove(m_name);
    return nullptr;
  }

  Executable_if* clone() const override
  {
    return new Op_trx(*this);
  }
};


// ----------------------------------------------------------------------

/*
  Operations which create database objects.

  They are implemented as Op_create<> template parametrized by the type of the
  object to create.
*/

template <Object_type T>
struct Op_create;


template<>
struct Op_create<Object_type::SCHEMA>
  : public Op_sql
{
  /*
    Note: Using ? placeholder in CREATE query did not work - server error
    about wrong SQL syntax.
  */
  Op_create(
    Shared_session_impl sess, const cdk::api::Schema_ref &schema,
    bool reuse = true
  )
    : Op_sql(sess,
        std::string("CREATE SCHEMA") + (reuse ? " IF NOT EXISTS " : " ")
        + "`" + schema.name() + "`"
      )
  {}
};

struct Op_create_modify_base
    : public Op_admin
{
  //TODO:
  //Allow validation schema and m_options to be a document, not only a JSON string.
  //Currently not possible because common layer does not have a document class
  //like DbDoc.
  std::string m_options;
  std::string m_validation_level;
  std::string m_validation_schema;
  //If true, means m_options refers to validation json, not the full options
  bool m_validation_options = false;

  Op_create_modify_base(Shared_session_impl sess, const char *cmd,
                        const cdk::api::Object_ref &coll,
                        std::string level,
                        std::string validation_schema)
    : Op_admin(sess, cmd)
    , m_validation_level(std::move(level))
    , m_validation_schema(std::move(validation_schema))
  {
    if (coll.schema())
      add_param("schema", Value::Access::mk_str(coll.schema()->name()));
    else
      common::throw_error("No schema specified for create collection operation");
    add_param("name", Value::Access::mk_str(coll.name()));
  }

  Op_create_modify_base(Shared_session_impl sess, const char *cmd,
                        const cdk::api::Object_ref &coll,
                        std::string json,
                        bool validation_json = false)
    : Op_create_modify_base(sess, cmd, coll, std::string(),std::string() )
  {
    m_options = std::move(json);
    m_validation_options = validation_json;
  }
  // cdk::Param_source

  void process(cdk::Any::Document::Processor &prc) const override;

};

template<>
struct Op_create<Object_type::COLLECTION>
  : public Op_create_modify_base
{
  Op_create(
        Shared_session_impl sess,
        const cdk::api::Object_ref &coll,
        bool reuse,
        std::string validation_level,
        std::string validation_schema
  )
    : Op_create_modify_base(sess, "create_collection", coll,
                            std::move(validation_level), std::move(validation_schema))
  {
    // 1050 = table already exists
    if (reuse)
      skip_error(cdk::server_error(1050));
  }

  Op_create(
        Shared_session_impl sess,
        const cdk::api::Object_ref &coll,
        bool reuse,
        std::string validation_json
  )
    : Op_create_modify_base(sess, "create_collection", coll,
                            std::move(validation_json), true)
  {
    // 1050 = table already exists
    if (reuse)
      skip_error(cdk::server_error(1050));
  }

  Op_create(
        Shared_session_impl sess,
        const cdk::api::Object_ref &coll,
        std::string json,
        bool validation_json = false
  )
    : Op_create_modify_base(sess, "create_collection", coll, std::move(json), validation_json)
  {}

};


/*
  A helper function which constructs an operation that creates a database object
  and executes it.
*/

template <Object_type T, typename... Ty>
inline
void create_object(
  Shared_session_impl sess, Ty&&... args
)
{
  try{
    Op_create<T> create(sess, std::forward<Ty>(args)...);
    Result_impl res(create.execute());
    res.next_result();
  }
  catch(const cdk::Error &err)
  {
    if(err.code().value() == 5015)
    { //Old server... doesn't support schema validation
      common::throw_error(
            "The server doesn't support the requested operation. " \
            "Please update the MySQL Server and or Client library");
    }
    throw;
  }

}


// ----------------------------------------------------------------------

/*
  Operations which modify database objects.

  They are implemented as Op_modify<> template parametrized by the type of the
  object to modify.
*/

template <Object_type T>
struct Op_modify;


template<>
struct Op_modify<Object_type::COLLECTION>
  : public Op_create_modify_base
{

  Op_modify(
        Shared_session_impl sess,
        const cdk::api::Object_ref &coll,
        std::string validation_level,
        std::string validation_schema
  ) : Op_create_modify_base(sess, "modify_collection_options", coll,
                            std::move(validation_level),
                            std::move(validation_schema))
  {}

  Op_modify(
        Shared_session_impl sess,
        const cdk::api::Object_ref &coll,
        std::string json,
        bool validation_json = false
  )
    : Op_create_modify_base(sess, "modify_collection_options",
                            coll,
                            std::move(json), validation_json)
  {}

};

/*
  A helper function which constructs an operation that modifies a database object
  and executes it.
*/

template <Object_type T, typename... Ty>
inline
void modify_object(
  Shared_session_impl sess, Ty&&... args
)
{
  try{
    Op_modify<T> modify(sess, std::forward<Ty>(args)...);
    Result_impl res(modify.execute());
    res.next_result();
  }
  catch(const cdk::Error &err)
  {
    if(err.code().value() == 5157)
    { //Old server... doesn't support modify collection
      common::throw_error(
            "The server doesn't support the requested operation. " \
            "Please update the MySQL Server and or Client library");
    }
    throw;
  }
}


/*
  Operations which drop database objects.

  They are implemented as Op_drop<> template parametrized by the type of the
  object to create.
*/

template <Object_type T>
struct Op_drop
  : public Op_admin
{
  Op_drop(Shared_session_impl sess, const cdk::api::Object_ref &obj)
    : Op_admin(sess, "drop_collection")
  {
    if (!obj.schema())
      common::throw_error("No schema specified for drop collection/table operation");
    add_param("schema", obj.schema()->name());
    add_param("name", obj.name());
    // 1051 = collection doesn't exist
    skip_error(cdk::server_error(1051));
  }
};


template <>
struct Op_drop<Object_type::VIEW>
  : public Op_base<Executable_if>
{
  Object_ref m_view;

  Op_drop(Shared_session_impl sess, const cdk::api::Object_ref &view)
    : Op_base(sess)
    , m_view(view)
  {
    /*
      Note: We ignore error 1347 treating it the same as the case when the view
      to be dropped does not exist. For example, if we say "drop view foo" and
      there is no view "foo" then the operation silently succeeds. If "foo" is
      a table, there is no view named "foo" so the behavior is the same.
    */
    // 1347 = object is not a view
    skip_error(cdk::server_error(1347));
  }

  cdk::Reply* send_command() override
  {
    /*
      Note: false argument to view_drop() means that we do not check for
      the existence of the view being dropped.
    */
    return new cdk::Reply(
      get_cdk_session().view_drop(m_view, false)
    );
  }

  Executable_if* clone() const override
  {
    return new Op_drop(*this);
  }
};


template <>
struct Op_drop<Object_type::SCHEMA>
  : public Op_sql
{
  Op_drop(Shared_session_impl sess, const cdk::api::Schema_ref &schema)
    : Op_sql(sess,
        std::string("DROP SCHEMA IF EXISTS `") + schema.name() + "`"
      )
  {}
};


/*
  A helper function which constructs an operation that drops a database object
  and executes it.
*/

template <Object_type T, typename... Ty>
inline
void drop_object(
  Shared_session_impl sess, Ty... args
)
{
  Op_drop<T> drop(sess, args...);
  Result_impl res(drop.execute());
  res.next_result();
}


// ----------------------------------------------------------------------------

/*
  Creating and droping collection indexes.
*/

struct Op_idx_admin
  : public Op_admin
{
  Op_idx_admin(
    Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    const string &name,
    const char *cmd
  )
    : Op_admin(sess, cmd)
  {
    assert(coll.schema());
    add_param("schema", coll.schema()->name());
    add_param("collection", coll.name());
    add_param("name", name);
  }
};


struct Op_idx_drop
  : public Op_idx_admin
{
  Op_idx_drop(
    Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    const string &name
  )
    : Op_idx_admin(sess, coll, name, "drop_collection_index")
  {
    // 1091 = "Can't drop ...; check that column/key exists"
    skip_error(cdk::server_error(1091));
  }
};


struct Op_idx_create
  : public Op_idx_admin
{
  string m_def;

  Op_idx_create(
    Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    const string &name,
    const string &idx_def
  )
    : Op_idx_admin(sess, coll, name, "create_collection_index")
    , m_def(idx_def)
  {
    // TODO: correct error number... 1051 = collection doesn't exist
    // skip_error(cdk::server_error(1051));
  }

  void process(cdk::Any::Document::Processor &prc) const override;
};


// ----------------------------------------------------------------------------


/*
  Operations which query the server for existing objects.
*/

template <Object_type T>
const char* obj_name();

template <>
inline
const char* obj_name<Object_type::COLLECTION>() { return "COLLECTION"; }

template <>
inline
const char* obj_name<Object_type::VIEW>() { return "VIEW"; }

template <>
inline
const char* obj_name<Object_type::TABLE>() { return "TABLE"; }


/*
  Base class for list operations which use server admin command "list_objects"
  which returns a list of objects in a schema that match given pattern.

  In the returned result first column contains object name and second column
  contains its type.
*/

struct Op_list_objects
  : public Op_admin
{
  using string = std::string;

  Op_list_objects(
    Shared_session_impl sess,
    const cdk::api::Schema_ref &schema,
    const string &pattern
  )
    : Op_admin(sess, "list_objects")
  {
    add_param("schema", schema.name());
    add_param("pattern", pattern);
  }

  // Static method to filter rows for a given object type.

  template <Object_type T>
  static bool check_type(const Row_data &row)
  {
    cdk::bytes  name_col = row.at(1).data();
    std::string name(name_col.begin(), name_col.end()-1);
    return name == obj_name<T>();
  }
};


template <Object_type T>
struct Op_list
  : public Op_list_objects
{
  using string = std::string;

  Op_list(
    Shared_session_impl sess,
    const cdk::api::Schema_ref &schema,
    const string &pattern
  )
    : Op_list_objects(sess, schema, pattern)
  {}

  void init_result(Result_impl &res) override
  {
    res.m_row_filter = check_type<T>;
  }
};


template <>
struct Op_list<Object_type::TABLE>
  : public Op_list_objects
{
  using string = std::string;

  bool m_include_views;

  Op_list(
    Shared_session_impl sess,
    const cdk::api::Schema_ref &schema,
    const string &pattern,
    bool  include_views = true
  )
    : Op_list_objects(sess, schema, pattern)
    , m_include_views(include_views)
  {}

  void init_result(Result_impl &res) override
  {
    /*
      Note: not binding to m_include_views inside lambdas to not make
      the result object dependent on this operation object.
    */

    if (m_include_views)
    {
      res.m_row_filter = [](const Row_data &row) -> bool
      {
        return Op_list_objects::check_type<Object_type::TABLE>(row)
            || Op_list_objects::check_type<Object_type::VIEW>(row);
      };
    }
    else
    {
      res.m_row_filter = check_type<Object_type::TABLE>;
    }
  }

};


template <>
struct Op_list<Object_type::SCHEMA>
  : public Op_sql
{
  using string = std::string;

  Op_list(
    Shared_session_impl sess,
    const string &pattern
  )
    : Op_sql(sess, "SHOW SCHEMAS LIKE ?")
  {
    add_param(pattern);
  }
};


/*
  Helper functions which use object list queries to check existence of objects
  in the database.
*/

inline
bool check_schema_exists(
  Shared_session_impl sess, cdk::api::Schema_ref &schema
)
{
  Op_list<Object_type::SCHEMA> find(sess, schema.name());
  Result_impl res(find.execute());
  return 0 < res.count();
}

template <Object_type T>
inline
bool check_object_exists(
  Shared_session_impl sess,
  const cdk::api::Object_ref &obj
)
{
  assert(obj.schema());
  Op_list<T> find(sess, *obj.schema(), obj.name());
  Result_impl res(find.execute());
  return 0 < res.count();
}



// ----------------------------------------------------------------------------


/*
  Implementation for collection CRUD add operation (Collection_add_if
  interface).

  Such operation can add (or update) a single document or several documents
  given by a list of JSON strings. If upsert flag is set to true, the operation
  should be used with a single document and then it updates the document if it
  already exists in the database.

  If documents to be added do not have defined ids, the ids are generated for
  them. These generated ids are then passed to the result object
  (in init_result() method).

  Implementation object stores list of JSON strings describing documents
  to be added and passed with `add_json` method. It presents this list
  of documents via cdk::Doc_source interface. See method `process` for
  details.

  Overriden method Op_base::send_command() sends the collection add
  command to the CDK session.
*/

class Op_collection_add
  : public Op_base<common::Collection_add_if>
  , public cdk::Doc_source
{
  using string = std::string;

  Object_ref    m_coll;
  std::vector<std::string> m_json;  // note: UTF8 JSON strings
  unsigned m_pos;
  const cdk::Expression *m_expr = nullptr;
  bool m_upsert = false;

public:

  Op_collection_add(
    Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    bool upsert = false
  )
    : Op_base(sess)
    , m_coll(coll)
    , m_pos(0)
    , m_upsert(upsert)
  {}

  Executable_if* clone() const override
  {
    return new Op_collection_add(*this);
  }

  /*
    Add a document specified by CDK expression. Only one such document can
    be specified. Another call to add_doc() overwrites previously specified
    document.
  */

  void add_doc(cdk::Expression &expr)
  {
    m_expr = &expr;
  }

  /*
    Add a document specified by an UTF8 JSON string. Several documents can be
    specified to create multi-document add operation.
  */

  void add_json(const std::string &json) override
  {
    m_json.push_back(json);
  }

  void clear_docs() override
  {
    m_json.clear();
  }


  void execute_prepare() override
  {
    m_pos = 0;
  }

  void execute_cleanup() override
  {
    // Doc source has been consumed - no need to keep the data
    clear_docs();
  }


  cdk::Reply* send_command() override
  {
    // Do nothing if no documents were specified.

    if (!m_expr && m_json.empty())
      return nullptr;

    // Issue coll_add statement where documents are described by list
    // of expressions defined by this instance.

    return new cdk::Reply(
      get_cdk_session().coll_add(m_coll, *this, nullptr, m_upsert)
    );
  }


  // Doc_source

  bool next() override
  {
    if (m_expr)
    {
      if (m_pos > 0)
        return false;
    }
    else if (m_pos >= m_json.size())
      return false;
    ++m_pos;
    return true;
  }

  void process(cdk::Expression::Processor &ep) const override;

};


/*
  Class describing elements of expression:

    JSON_INSERT(<json>, '$._id', <id>)

  where <json> and <id> are given as constructor parameters.
*/

struct Insert_id
  : public cdk::api::Table_ref
  , cdk::Expression
  , cdk::Expr_list
{
  typedef cdk::string string;

  const cdk::Expression &m_doc;
  std::string  m_id;

  Insert_id(const cdk::Expression &doc, const std::string &id)
    : m_doc(doc), m_id(id)
  {}


  // Table_ref (function name)

  const cdk::api::Schema_ref* schema() const override { return nullptr; }
  const string name() const override { return "JSON_INSERT"; }


  void process(cdk::Expression::Processor &prc) const override
  {
    auto args_prc = safe_prc(prc)->scalar()->call(*this);
    if (!args_prc)
      return;
    process(*args_prc);
  }

  // Expr_list (arguments)

  void process(cdk::Expr_list::Processor &prc) const override
  {
    auto sprc = safe_prc(prc);
    sprc->list_begin();
    m_doc.process_if(sprc->list_el()); // the document to modify
    sprc->list_el()->scalar()->val()->str("$._id");
    sprc->list_el()->scalar()->val()->str(m_id);
    sprc->list_end();
  }

};


/*
  Expression describing a single document to be inserted by add operation.

  If document id was generated, then expression is a call of
  JSON_INSERT() function that adds generated id. Otherwise it
  is plain JSON string of the document.

  TODO:
  - Append '_id' field at the end of document, instead of using
    JSON_INSERT() (should be more efficient). If duplicate key is
    found in a document string, only the first occurence is taken
    into account.
*/

inline
void Op_collection_add::process(cdk::Expression::Processor &ep) const
{
  assert(m_pos > 0);  // this method should be called after calling next()

  if (m_expr)
  {
    m_expr->process(ep);
    return;
  }

  const std::string &json = m_json.at(m_pos-1);

  // TODO: Report as opaque value of type DOCUMENT using JSON format.
  ep.scalar()->val()->str(json);
}


// ------

static const
parser::Parser_mode::value doc_mode = parser::Parser_mode::DOCUMENT;

static const
parser::Parser_mode::value tbl_mode = parser::Parser_mode::TABLE;

/*
  Implementation of collection CRUD find operation (Collection_find_if
  interface)
*/

class Op_collection_find
    : public  Op_select< doc_mode,
              Op_projection<
              Op_group_by< doc_mode,
              Op_having< doc_mode,
              Op_sort< doc_mode,
              Op_limit<
              Op_bind<
                Op_base<common::Collection_find_if>
              >>>>>>>
{
  using string = std::string;

  Object_ref m_coll;

public:

  Op_collection_find(Shared_session_impl sess, const cdk::api::Object_ref &coll)
    : Op_select(sess)
    , m_coll(coll)
  {}

  Op_collection_find(
    Shared_session_impl sess, const cdk::api::Object_ref &coll, const string &expr
  )
    : Op_collection_find(sess,coll)
  {
    set_where(expr);
  }

  Executable_if* clone() const override
  {
    return new Op_collection_find(*this);
  }

  cdk::Reply* do_send_command() override
  {
    return new cdk::Reply(get_cdk_session().coll_find(
                            get_stmt_id(),
                            m_coll,
                            nullptr,           // view spec
                            get_where(),
                            get_doc_proj(),
                            get_order_by(),
                            get_group_by(),
                            get_having(),
                            get_limit(),
                            get_params(),
                            m_lock_mode,
                            m_lock_contention
                            ));
  }

};


/*
  Internal implementation for collection CRUD remove operation
  (Collection_remove_if interface).
*/

class Op_collection_remove
  : public  Op_select< doc_mode,
            Op_sort< doc_mode,
            Op_limit<
            Op_bind<
              Op_base<common::Collection_remove_if>
            >>>>
{
  using string = std::string;

  Object_ref m_coll;

public:

  Op_collection_remove(Shared_session_impl sess, const cdk::api::Object_ref &coll)
    : Op_select(sess)
    , m_coll(coll)
  { }

  Op_collection_remove(
    Shared_session_impl sess, const cdk::api::Object_ref &coll, const string &expr
  )
    : Op_collection_remove(sess, coll)
  {
    set_where(expr);
  }

  Executable_if* clone() const override
  {
    return new Op_collection_remove(*this);
  }


  cdk::Reply* do_send_command() override
  {
    return new cdk::Reply(get_cdk_session().coll_remove(
                            get_stmt_id(),
                            m_coll,
                            get_where(),
                            get_order_by(),
                            get_limit(),
                            get_params()
                            ));
  }
};


/*
  Implementation of collection CRUD modify operation (Collection_modify_if
  interface).

  This implementation builds on top of Op_select<> and Op_sort<>
  templates. It adds storage for update operations specified by
  user. These update requests are presented via cdk::Update_spec
  interface.
*/

class Op_collection_modify
    : public
        Op_select< doc_mode,
        Op_sort< doc_mode,
        Op_limit<
        Op_bind<
          Op_base<common::Collection_modify_if>
        >>>>
    , public cdk::Update_spec
{
  using string = std::string;
  using Impl = common::Collection_modify_if;

  Object_ref m_coll;

  struct Field_Op
    : public cdk::Expression
  {
    using Operation = typename Impl::Operation;

    Operation m_op;
    string m_field;
    Value  m_val;
    const cdk::Expression *m_expr = nullptr;

    Field_Op(Operation op, const string &field)
      : m_op(op)
      , m_field(field)
    {}

    Field_Op(Operation op, const string &field, const cdk::Expression &expr)
      : m_op(op), m_field(field), m_expr(&expr)
    {}

    Field_Op(Operation op, const string &field, const Value &val)
      : m_op(op), m_field(field), m_val(val)
    {}

    void process(Processor &prc) const
    {
      if (m_expr)
        return m_expr->process(prc);

      Value::Access::process(parser::Parser_mode::DOCUMENT, m_val, prc);
    }
  };

  std::list<Field_Op> m_update;
  std::list<Field_Op>::const_iterator m_update_it;

public:

  Op_collection_modify(Shared_session_impl sess, const cdk::api::Object_ref &coll)
    : Op_select(sess)
    , m_coll(coll)
  {}

  Op_collection_modify(
    Shared_session_impl sess, const cdk::api::Object_ref &coll, const string &expr
  )
    : Op_collection_modify(sess, coll)
  {
    set_where(expr);
  }

  Executable_if* clone() const override
  {
    return new Op_collection_modify(*this);
  }

  cdk::Reply* do_send_command() override
  {
    // Do nothing if no update specifications were added

    if (m_update.empty())
      return nullptr;

    m_update_it = m_update.end();

    return new cdk::Reply(get_cdk_session().coll_update(
                            get_stmt_id(),
                            m_coll,
                            get_where(),
                            *this,
                            get_order_by(),
                            get_limit(),
                            get_params()
                            ));
  }


  void add_operation(typename Impl::Operation op,
                     const string &field) override
  {
    set_prepare_state(PS_EXECUTE);
    m_update.emplace_back(op, field);
  }

  void add_operation(typename Impl::Operation op,
                     const string &field,
                     const Value &val) override
  {
    set_prepare_state(PS_EXECUTE);
    m_update.emplace_back(op, field, val);
  }

  /*
    Note: the expression reference passed here must be valid at the time when
    the operation is executed.
  */

  void add_operation(typename Impl::Operation op,
                     const string &field,
                     cdk::Expression &expr)
  {
    set_prepare_state(PS_EXECUTE);
    m_update.emplace_back(op, field, expr);
  }


  void clear_modifications() override
  {
    if (m_update.size() != 0)
      set_prepare_state(PS_EXECUTE);
    m_update.clear();
  }


  // cdk::Update_spec implementation

  bool next() override
  {
    if (m_update_it == m_update.end())
    {
      m_update_it = m_update.begin();
      return m_update_it!= m_update.end();
    }
    ++m_update_it;
    return m_update_it!= m_update.end();
  }

  void process(Update_spec::Processor &prc) const override
  {
    parser::Doc_field_parser doc_field(m_update_it->m_field);

    switch (m_update_it->m_op)
    {
      case Impl::SET:
        m_update_it->process_if(prc.set(&doc_field));
        break;

      case Impl::UNSET:
        prc.remove(&doc_field);
        break;

      case Impl::ARRAY_INSERT:
        m_update_it->process_if(prc.array_insert(&doc_field));
        break;

      case Impl::ARRAY_APPEND:
        m_update_it->process_if(prc.array_append(&doc_field));
        break;

      case Impl::ARRAY_DELETE:
        prc.remove(&doc_field);
        break;

      case Impl::MERGE_PATCH:
        m_update_it->process_if(prc.patch());
        break;
    }
  }

};


/*
  Specialization of Op_collection_modify which is used to update a single
  document.
*/

class Op_collection_replace
  : public Op_collection_modify
  , Insert_id
{

public:

  Op_collection_replace(
    Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    const std::string &id,
    const cdk::Expression &doc
  )
    : Op_collection_modify(sess, coll, "_id = :id")
    , Insert_id(doc, id)
  {
    /*
      Note: treated as expression, *this corresponds
      to JSON_INSERT(doc,"_id",id).
    */
    add_operation(SET, "$", *this);
    add_param("id", id);
  }

};


/*
  Implementation of UPSERT operation.

  This adds or replaces a single document. The document id must be provided
  when the operation is constructed, but it can also be present in the document.
  This operation uses Insert_id expression to append provided document id for
  the case where it does not contain user-specified one. If it does, the id
  specified in the document will take precedence over the one added by
  Insert_id expression.
*/

class Op_collection_upsert
  : public Op_collection_add
  , Insert_id
{

public:

  Op_collection_upsert(
    Shared_session_impl sess,
    const cdk::api::Object_ref &coll,
    const std::string &id,
    const cdk::Expression &doc
  )
    : Op_collection_add(sess, coll, true)  // upsert flag set to true
    , Insert_id(doc, id)
  {
    add_doc(*(Insert_id*)this);
  }

};

// ----------------------------------------------------------------------------

/*
  Internal implementation for table CRUD select operation (Table_select_if
  interface).
*/

class Op_table_select
    : public  Op_select< tbl_mode,
              Op_projection<
              Op_group_by< tbl_mode,
              Op_having< tbl_mode,
              Op_sort< tbl_mode,
              Op_limit<
              Op_bind<
                Op_base<common::Table_select_if>
              >>>>>>>
{
  Object_ref m_table;
  const cdk::View_spec *m_view = nullptr;

  cdk::Reply* do_send_command() override
  {
    return new cdk::Reply(get_cdk_session().table_select(
                            get_stmt_id(),
                            m_table,
                            m_view,           // view spec
                            get_where(),
                            get_tbl_proj(),
                            get_order_by(),
                            get_group_by(),
                            get_having(),
                            get_limit(),
                            get_params(),
                            m_lock_mode,
                            m_lock_contention
                            ));
  }

  void set_view(const cdk::View_spec *view)
  {
    m_view = view;
  }

  Executable_if* clone() const override
  {
    return new Op_table_select(*this);
  }

public:

  Op_table_select(Shared_session_impl sess, const cdk::api::Object_ref &table)
    : Op_select(sess)
    , m_table(table)
  {}

};


/*
  Internal implementation for table CRUD insert operation (Table_insert_if
  interface).

  This template is parametrized by class VAL used to store values inside rows
  that are to be inserted by the operation (m_rows list). By default this is
  class common::Value but a different class, handling more/different types
  of values can be used.
*/

template <class VAL = common::Value>
class Op_table_insert
    : public Op_base<common::Table_insert_if<Row_impl<VAL>>>
    , public cdk::Row_source
    , public cdk::api::Columns
{
  using Base = Op_base<common::Table_insert_if<Row_impl<VAL>>>;
  using Value = VAL;
  using string = std::string;
  using Row_list = std::list < Row_impl<VAL> >;
  using Col_list = std::list < string >;
  using Object_ref = impl::common::Object_ref;

  Object_ref m_table;

  Row_list   m_rows;
  typename Row_list::iterator m_cur_row;

  Col_list m_cols;
  col_count_t  m_col_count = 0;

public:

  Op_table_insert(Shared_session_impl sess, const Object_ref &tbl)
    : Base(sess)
    , m_table(tbl)
  {}

  Op_table_insert(const Op_table_insert &other)
    : Base(other)
    , m_table(other.m_table)
    , m_rows(other.m_rows)
    , m_cols(other.m_cols)
  {
    m_cur_row = m_rows.begin();
  }

  Executable_if* clone() const override
  {
    return new Op_table_insert(*this);
  }

  // Table_insert_if

  void add_column(const string &column) override
  {
    m_cols.emplace_back(column);
    m_col_count++;
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void clear_columns() override
  {
    /*
      When changing column specification, old row data becomes obsolete and
      we remove it.
    */
    clear_rows();
    m_cols.clear();
    m_col_count = 0;
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void add_row(const Row_impl<VAL> &row) override
  {
    m_rows.emplace_back(row);
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void clear_rows() override
  {
    m_rows.clear();
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

  void clear()
  {
    clear_columns();
    clear_rows();
    Base::set_prepare_state(Base::PS_EXECUTE);
  }

private:

  // Executable

  bool m_started = false;

  cdk::Reply* send_command() override
  {
    return do_send_command();
  }

  cdk::Reply* do_send_command() override
  {
    // Do nothing if no rows were specified.

    if (m_rows.empty())
      return nullptr;

    // Prepare iterators to make a pass through m_rows list.
    m_started = false;

    return new cdk::Reply(Base::get_cdk_session().table_insert(
                            0,
                            m_table,
                            *this,
                            m_cols.empty() ? nullptr : this,
                            nullptr
                            )
                          );

  }


  // Row_source (Iterator)

  bool next() override
  {
    if (!m_started)
      m_cur_row = m_rows.begin();
    else
      ++m_cur_row;

    m_started = true;
    return m_cur_row != m_rows.end();
  }


  // Columns

  void process(cdk::api::Columns::Processor& prc) const override
  {
    prc.list_begin();
    for (auto el : m_cols)
    {
      cdk::safe_prc(prc)->list_el()->name(el);
    }
    prc.list_end();
  }

  // Row_source (Expr_list)

  void process(cdk::Expr_list::Processor &lp) const override
  {
    lp.list_begin();

    for (col_count_t pos = 0; pos < m_cur_row->col_count(); ++pos)
    {
      auto *el = lp.list_el();
      if (el)
        Value::Access::process(
          parser::Parser_mode::TABLE, m_cur_row->get(pos), *el
        );
    }

    lp.list_end();
  }

};


/*
  Internal implementation for table CRUD update operation (Table_update_if
  interface).

  This implementation is built from Op_select<> and Op_projection<>
  templates and it implements the `add_set` method of Table_update_impl
  implementation interface. Update requests are stored in m_set_values
  member and presented to CDK via cdk::Update_spec interface.

  It overrides Op_base::send_command() to send table update command
  to the CDK session.
*/

class Op_table_update
  : public  Op_select< tbl_mode,
            Op_sort< tbl_mode,
            Op_limit<
            Op_bind<
              Op_base<common::Table_update_if>
            >>>>
  , public cdk::Update_spec
  , public cdk::api::Column_ref
{
  using Base =  Op_select< tbl_mode,
                Op_sort< tbl_mode,
                Op_limit<
                Op_bind<
                  Op_base<common::Table_update_if>
                >>>>;
  using string = std::string;
  typedef std::map<string, Value> SetValues;

  Object_ref m_table;
  std::unique_ptr<parser::Table_field_parser> m_table_field;
  SetValues m_set_values;
  SetValues::const_iterator m_set_it;

public:

  Op_table_update(Shared_session_impl sess, const cdk::api::Object_ref &table)
    : Base(sess)
    , m_table(table)
  {}

  Op_table_update(const Op_table_update &other)
    : Base(other)
    , m_table(other.m_table)
    , m_set_values(other.m_set_values)
  {}


  // Table_update_if

  void add_set(const string &field, const Value &val) override
  {
    auto el = m_set_values.emplace(field, val);
    //substitute if exists
    if (!el.second)
    {
      el.first->second = val;
    }
    Base::set_prepare_state(PS_EXECUTE);
  }

  void clear_modifications() override
  {
    m_set_values.clear();
    Base::set_prepare_state(PS_EXECUTE);
  }

protected:

  Executable_if* clone() const override
  {
    return new Op_table_update(*this);
  }

  cdk::Reply* do_send_command() override
  {
    m_set_it = m_set_values.end();

    return new cdk::Reply(Base::get_cdk_session().table_update(
                            get_stmt_id(),
                            m_table,
                            get_where(),
                            *this,
                            get_order_by(),
                            get_limit(),
                            get_params()
                            ));
  }


  // cdk::Update_spec

  virtual bool next() override
  {
    if (m_set_it == m_set_values.end())
    {
      m_set_it = m_set_values.begin();
    }
    else
    {
      ++m_set_it;
    }

    bool more = m_set_it != m_set_values.end();

    if (more)
     m_table_field.reset(new parser::Table_field_parser(m_set_it->first));

    return more;
  }

  void process(cdk::Update_spec::Processor &prc) const override
  {
    prc.column(*this);

    auto *vprc
      = prc.set(m_table_field->has_path() ? m_table_field.get() : nullptr);
    if (vprc)
      Value::Access::process(
        parser::Parser_mode::TABLE, m_set_it->second, *vprc
      );
  }


  //  cdk::api::Column_ref

  const cdk::string name() const override
  {
    return m_table_field->name();
  }

  const cdk::api::Table_ref* table() const override
  {
    return m_table_field->table();
  }

};


/*
  Internal implementation for table CRUD remove operation (Table_remove_if
  interface).

  Notr: Table_remove_if interface methods are implemented by templates from
  which this class derives.
*/

class Op_table_remove
  : public  Op_select< tbl_mode,
            Op_sort< tbl_mode,
            Op_limit<
            Op_bind<
              Op_base<common::Table_remove_if>
            >>>>
{
  using Base =  Op_select< tbl_mode,
                Op_sort< tbl_mode,
                Op_limit<
                Op_bind<
                  Op_base<common::Table_remove_if>
                >>>>;
  using string = std::string;

  Object_ref m_table;

public:

  Op_table_remove(Shared_session_impl sess, const cdk::api::Object_ref &table)
    : Base(sess)
    , m_table(table)
  {}

protected:

  Executable_if* clone() const override
  {
    return new Op_table_remove(*this);
  }

  cdk::Reply* do_send_command() override
  {
    return new cdk::Reply(Base::get_cdk_session().table_delete(
                            get_stmt_id(),
                            m_table,
                            get_where(),
                            get_order_by(),
                            get_limit(),
                            get_params()
                            ));
  }

};


}  // internal
}  // impl
}  // mysqlx

#endif
