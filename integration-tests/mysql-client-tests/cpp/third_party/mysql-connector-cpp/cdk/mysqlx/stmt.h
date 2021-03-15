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

#ifndef CDK_MYSQLX_DELAYED_OP_H
#define CDK_MYSQLX_DELAYED_OP_H

#include <mysql/cdk/common.h>
#include <mysql/cdk/mysqlx/result.h>
#include <memory>
#include "converters.h"

#include <list>


namespace cdk {
namespace mysqlx {


/*
  Specialization of Stmt_op which expects a full server reply with result
  sets instead of a simple OK.
*/

class Query_stmt
  : public Stmt_op
{
protected:

  using Stmt_op::Stmt_op;

  virtual bool do_cont() override
  {
    if (SEND == m_state)
      return Stmt_op::do_cont();

    /*
      Note: Changing state to MDATA will tell Stmt_op to expect a full
      reply instead of simple OK.
    */

    if (OK == m_state)
      m_state = MDATA;

    assert(OK != m_state);

    return Stmt_op::do_cont();
  }

};


/*
  A base class for operations that use statements prepared on the server.

  By default this operation will execute prepared statement with stmt_id given
  when it is created and then will process its reply as usual (so the user
  of this class must ensure that the statement was prepared on the server
  beforehand).

  Otherwise, if derived class is used, it is assumed that it overrides
  send_cmd() method to send and prepare a statement on the server. Server
  reply is expected to be a reply to prepare + execute pipeline with first OK
  packet as a reply to the prepare command followed by a reply to the
  statement that was prepared (which is handled as usual by the base
  class).

  Note: The distinction between the two behaviors is based on the value of
  m_stmt_id member. Normally it should be non-zero but default implementation
  of send_cmd() resets it to 0 and then the "execute already prepared statement"
  path is executed. An overridden snd_cmd() that sends prepare + execute
  pipeline should not reset m_stmt_id.
*/

template <class Base>
class Prepared
    : public Base
{
protected:

  /*
    Note: m_stmt_id is reset to 0 by reply processing logic. Therefore one
    should not expect that it always contains the original stmt id.
  */

  uint32_t m_stmt_id=0;

  const Limit        *m_limit = nullptr;
  const protocol::mysqlx::api::Any_list *m_param_list = nullptr;
  const protocol::mysqlx::api::Args_map *m_param_map = nullptr;
  Any_list_converter m_list_conv;
  Param_converter    m_map_conv;
  bool m_prepare_error = false;

public:

  Prepared(
    Session &s,
    uint32_t stmt_id,
    const cdk::Limit *lim,
    const Param_source *param
  )
    : Base(s)
    , m_stmt_id(stmt_id)
    , m_limit(lim)
  {
    if (param)
    {
      m_map_conv.reset(*param);
      m_param_map = &m_map_conv;
    }
  }

  Prepared(
    Session &s,
    uint32_t stmt_id,
    const Any_list *list
  )
    : Base(s)
    , m_stmt_id(stmt_id)
  {
    if (list)
    {
      m_list_conv.reset(*list);
      m_param_list = &m_list_conv;
    }
  }

  Prepared(Session &s)
    : Base(s)
  {}


  /*
    This implementation just executes an already prepared statement with the
    given id and processes the reply as usual. Derived classes are supposed
    to override it with protocol operation that prepares and executes
    a statement (if m_stmt_id is not 0).
  */

  Proto_op* send_cmd() override
  {
    uint32_t id = m_stmt_id;
    m_stmt_id = 0;  // so that we directly process reply to Execute
    if (m_limit || m_param_map)
    {
      return &Base::get_protocol().snd_PrepareExecute(id, m_limit, m_param_map);
    }
    else
    {
      return &Base::get_protocol().snd_PrepareExecute(id, m_param_list);
    }
  }

  bool do_cont() override
  {
    /*
      If m_stmt_id is 0 (so no prepared statements are used) or we are still
      in the sending phase, continue as the base operation.
    */

    if ((0 == m_stmt_id) || !this->stmt_sent())
      return Base::do_cont();

    /*
      Here m_stmt_id != 0 and we know we are dealing with server reply to
      a pipeline starting with prepare command. We need to first process
      the reply to prepare command and then continue processing the rest
      of the reply as dictated by the base class.

      Note: we could execute rcv_Reply() operation asynchronously here, but
      for simplicity we just wait for it to complete before proceeding.
    */

    Base::get_protocol().rcv_Reply(*this).wait();
    m_stmt_id = 0; // continue processing as usual
    return false;
  }

  void error(
    unsigned int code, short int severity,
    sql_state_t sql_state, const string &msg
  ) override
  {
    /*
      If we see error after sending commands and while m_stmt_id != 0 then this
      is failed prepare command which we report as Server_prepare_error and also
      set m_prepare_error flag so that further errors are ignored. Otherwise
      we invoke base error handler.
    */

    if (this->stmt_sent() && (0 != m_stmt_id) && (Severity::ERROR == severity))
    {
      m_prepare_error = true;
      Base::add_diagnostics(Severity::ERROR, new Server_prepare_error(code, sql_state, msg));
      return;
    }
    else
      Base::error(code, severity, sql_state, msg);
  }

  void add_diagnostics(short int severity, Server_error *err) override
  {
    // ignore other errors after failed prepare
    if (m_prepare_error && Severity::ERROR == severity)
      return;
    Base::add_diagnostics(severity, err);
  }

  void ok(string) override
  {}
};


class Crud_stmt
  : public Prepared<Query_stmt>
{
protected:

  Crud_stmt(
    Session &s, uint32_t stmt_id,
    const api::Object_ref &obj,
    const cdk::Limit *lim,
    const Param_source *param
  )
    : Prepared(s, stmt_id, lim, param)
  {
    set(obj);
  }
};


// -------------------------------------------------------------------------


struct Doc_args : public Any_list
{
  Doc_args()
  {}

  Doc_args(const Any::Document *args)
    : m_doc(args)
  {}

  const cdk::Any::Document *m_doc = nullptr;
  void process(Processor &prc) const
  {
    Safe_prc<Any_list::Processor> sprc(prc);
    sprc->list_begin();
    if (m_doc)
      m_doc->process_if(sprc->list_el()->doc());
    sprc->list_end();
  }

  bool has_args() const
  {
    return nullptr != m_doc;
  }
};


class Cmd_StmtExecute
    : public Prepared<Query_stmt>
{
protected:

  const char *m_ns;
  const string m_stmt;
  Doc_args m_doc_args;

  Proto_op* send_cmd()
  {
    return &get_protocol().snd_StmtExecute(
      m_stmt_id, m_ns, m_stmt, m_param_list
    );
  }

public:

  Cmd_StmtExecute(Session &s, uint32_t stmt_id, const char *ns,
          const string& stmt, Any_list *args)
    : Prepared(s, stmt_id, args)
    , m_ns(ns)
    , m_stmt(stmt)
  {}

  Cmd_StmtExecute(Session &s, uint32_t stmt_id, const char *ns,
    const string& stmt, const cdk::Any::Document *args)
    : Prepared(s, stmt_id, &m_doc_args)
    , m_ns(ns)
    , m_stmt(stmt)
    , m_doc_args(args)
  {}

};


// -------------------------------------------------------------------------


class Cmd_InsertDocs
    : public Crud_stmt
    , public protocol::mysqlx::Row_source
{
protected:

  cdk::Doc_source &m_docs;
  const Param_source *m_param;
  bool m_upsert;

  Proto_op* send_cmd() override
  {
    Param_converter param_conv;

    if (m_param)
        param_conv.reset(*m_param);

    return &get_protocol().snd_Insert(cdk::protocol::mysqlx::DOCUMENT,
                                  m_stmt_id,
                                  *this,
                                  nullptr,
                                  *this,
                                  &param_conv,
                                  m_upsert);
  }

public:

  Cmd_InsertDocs(
    Session &s,
    uint32_t stmt_id,
    const api::Table_ref &coll,
    cdk::Doc_source &docs,
    const Param_source *param,
    bool upsert = false
  )
    : Crud_stmt(s, stmt_id, coll, nullptr, param)
    , m_docs(docs)
    , m_param(param)
    , m_upsert(upsert)
  {}

private:

  // Row_source

  void process(Processor &prc) const override
  {
    prc.list_begin();
    Processor::Element_prc *ep = prc.list_el();
    if (ep)
    {
      Expr_converter conv(m_docs);
      conv.process(*ep);
    }
    prc.list_end();
  }

  bool next() override
  {
    return m_docs.next();
  }

};


// -------------------------------------------------------------------------


class Cmd_InsertRows
  : public Crud_stmt
  , public protocol::mysqlx::Row_source
{
protected:

  Expr_converter  m_conv;
  cdk::Row_source &m_rows;
  const api::Columns *m_cols;
  const Param_source *m_param;

  Proto_op* send_cmd() override
  {
    Param_converter param_conv;

    if (m_param)
      param_conv.reset(*m_param);

    return &get_protocol().snd_Insert(cdk::protocol::mysqlx::TABLE,
                                  m_stmt_id,
                                  *this,
                                  m_cols,
                                  *this,
                                  &param_conv);
  }

public:

  // TODO: Life-time of rows instance...

  Cmd_InsertRows(
    Session &s,
    uint32_t stmt_id,
    const api::Table_ref &coll,
    cdk::Row_source &rows,
    const api::Columns *cols,
    const Param_source *param
  )
    : Crud_stmt(s, stmt_id, coll, nullptr, param)
    , m_rows(rows), m_cols(cols), m_param(param)
  {}

private:

  // Row_source

  void process(Processor &prc) const override
  {
    Expr_list_converter conv;
    conv.reset(m_rows);
    conv.process(prc);
  }

  bool next() override
  {
    return m_rows.next();
  }

};


// -------------------------------------------------------------------------

/*
  Conversion of order by specs which translates CDK sort key expressions
  to protocol expressions.
*/


class Order_prc_converter
  : public Converter<
  Order_prc_converter,
  Order_by::Expression::Processor,
  protocol::mysqlx::api::Order_by::Expression::Processor
  >
{
  Expr_prc_converter  m_conv;

  using Prc_from::Expr_prc;

public:

  virtual ~Order_prc_converter() {}

  Expr_prc* sort_key(Sort_direction::value dir)
  {
    Prc_to::Expr_prc *ep = m_proc->sort_key(dir);
    if (!ep)
      return nullptr;
    m_conv.reset(*ep);
    return &m_conv;
  }

};


typedef Expr_conv_base<
          List_prc_converter< Order_prc_converter >,
          Order_by,
          protocol::mysqlx::api::Order_by
        >
        Order_by_converter;


// -------------------------------------------------------------------------

/*
  Helper base class which implements protocol's Select_spec
  (or Find_spec) interface. This is used by CRUD operations
  which involve selecting a subset of rows/documents in the
  table/collection.

  A CRUD operation class which derives from this Cmd_Select
  can be used as selection criteria specification as required
  by protocol object methods.

  Note: This class uses converters to convert selection
  parameters from generic cdk types to types required by
  the protocol layer.
*/

template <class IF = protocol::mysqlx::Select_spec>
class Cmd_Select
  : public Crud_stmt
  , public IF
{
protected:

  Expr_converter     m_expr_conv;
  Order_by_converter m_ord_conv;

  Cmd_Select(
    Session &s,
    uint32_t stmt_id,
    const api::Object_ref &obj,
    const cdk::Expression *expr,
    const cdk::Order_by *order_by,
    const cdk::Limit *lim = nullptr,
    const cdk::Param_source *param = nullptr
  )
    : Crud_stmt(s, stmt_id, obj, lim, param)
    , m_expr_conv(expr)
    , m_ord_conv(order_by)
  {}


  virtual ~Cmd_Select()
  {}


  // Select_spec

  const protocol::mysqlx::api::Db_obj& obj() const { return *this; }

  const protocol::mysqlx::api::Expression* select() const
  {
    return m_expr_conv.get();
  }

  const protocol::mysqlx::api::Order_by* order() const
  {
    return m_ord_conv.get();
  }

  const protocol::mysqlx::api::Limit* limit() const
  {
    return m_limit;
  }

};


// -------------------------------------------------------------------------


template <protocol::mysqlx::Data_model DM>
class Cmd_Delete
    : public Cmd_Select<>
{
protected:

  Proto_op* send_cmd() override
  {
    return &get_protocol().snd_Delete(DM, m_stmt_id, *this, m_param_map);
  }

public:

  Cmd_Delete(
    Session& s,
    uint32_t stmt_id,
    const api::Object_ref &obj,
    const cdk::Expression *expr,
    const cdk::Order_by *order_by,
    const cdk::Limit *lim = nullptr,
    const cdk::Param_source *param = nullptr
  )
    : Cmd_Select(s, stmt_id, obj, expr, order_by, lim, param)
  {}

};


// -------------------------------------------------------------------------

/*
  Projection converters.

  Projections are specified differently for collections, where they are simply
  document expressions, and for tables, where they are expressed using Projection
  expressions.

  In either case we want to translate each projection specification to protocol
  Projection expression. The converters are built using appropriate processor
  converters.
*/


/*
  Processor converter for Expression::Document -> protocol::mysqlx::Projection
  conversion. Top-level keys become aliases and their values are projection
  expressions.
*/

class Doc_proj_prc_converter
  : public Converter<
      Doc_proj_prc_converter,
      cdk::Expression::Document::Processor,
      protocol::mysqlx::api::Projection::Processor>
{
  Expr_prc_converter m_conv;

  void doc_begin() { m_proc->list_begin(); }
  void doc_end()   { m_proc->list_end(); }

  Any_prc *key_val(const string &key)
  {
    Prc_to::Element_prc *ep = m_proc->list_el();
    if (!ep)
      return nullptr;
    ep->alias(key);
    Prc_to::Element_prc::Expr_prc *expp = ep->expr();
    if (!expp)
      return nullptr;
    m_conv.reset(*expp);
    return &m_conv;
  }
};


/*
  Processor converter for cdk::Projection -> protocol::mysqlx::Projection
  conversion. The two interfaces are identical - only expressions must be
  converted from cdk to protocol ones.
*/

class Table_proj_prc_converter
  : public Converter<
      Table_proj_prc_converter,
      Projection::Processor::Element_prc,
      protocol::mysqlx::api::Projection::Processor::Element_prc>
{
  Expr_prc_converter m_conv;

  Expr_prc* expr()
  {
    Prc_to::Expr_prc *prc = m_proc->expr();
    if (!prc)
      return nullptr;
    m_conv.reset(*prc);
    return &m_conv;
  }

  void alias(const string &name)
  {
    m_proc->alias(name);
  }

public:

  virtual ~Table_proj_prc_converter() {}
};


/*
  The SndFind statement below has two variants, for documents and
  for tables. Each variant uses different Projection type and different
  projection converter. Find_traits<> template defines these types for each
  variant.
*/

template <protocol::mysqlx::Data_model DM>
struct Find_traits;

template<>
struct Find_traits<protocol::mysqlx::DOCUMENT>
{
  typedef cdk::Expression::Document Projection;

  typedef Expr_conv_base<
      Doc_proj_prc_converter,
      Expression::Document, protocol::mysqlx::api::Projection >
    Projection_converter;
};

template<>
struct Find_traits<protocol::mysqlx::TABLE>
{
  typedef cdk::Projection Projection;

  typedef Expr_conv_base<
      List_prc_converter<Table_proj_prc_converter>,
      Projection, protocol::mysqlx::api::Projection >
    Projection_converter;
};


template <protocol::mysqlx::Data_model DM> class Cmd_ViewCrud;


template <protocol::mysqlx::Data_model DM>
class Cmd_Find
    : public Cmd_Select<protocol::mysqlx::Find_spec>
{
protected:

  typedef typename Find_traits<DM>::Projection Projection;
  typedef typename Find_traits<DM>::Projection_converter Projection_converter;

  Projection_converter  m_proj_conv;
  Expr_list_converter   m_group_by_conv;
  Expr_converter        m_having_conv;
  Lock_mode_value       m_lock_mode;
  Lock_contention_value m_lock_contention;

  Proto_op* send_cmd() override
  {
    return &get_protocol().snd_Find(DM, m_stmt_id, *this, m_param_map);
  }

public:

  Cmd_Find(
      Session& s,
      uint32_t stmt_id,
      const api::Table_ref &coll,
      const cdk::Expression *expr = nullptr,
      const Projection      *proj = nullptr,
      const cdk::Order_by   *order_by = nullptr,
      const cdk::Expr_list  *group_by = nullptr,
      const cdk::Expression *having = nullptr,
      const cdk::Limit      *lim = nullptr,
      const cdk::Param_source *param = nullptr,
      const Lock_mode_value locking = Lock_mode_value::NONE,
      const Lock_contention_value contention = Lock_contention_value::DEFAULT
  )
    : Cmd_Select(s, stmt_id, coll,  expr, order_by, lim, param)
    , m_proj_conv(proj)
    , m_group_by_conv(group_by), m_having_conv(having)
    , m_lock_mode(locking)
    , m_lock_contention(contention)
  {}

private:

  const protocol::mysqlx::api::Projection* project() const override
  {
    return m_proj_conv.get();
  }

  const protocol::mysqlx::api::Expr_list*  group_by() const override
  {
    return m_group_by_conv.get();
  }

  const protocol::mysqlx::api::Expression* having() const override
  {
    return m_having_conv.get();
  }

  Lock_mode_value locking() const override
  {
    return m_lock_mode;
  }

  Lock_contention_value contention() const override
  {
    return m_lock_contention;
  }

  friend class Cmd_ViewCrud<DM>;
};



// -------------------------------------------------------------------------

/*
  Conversion from string processor used to process a list of view column names
  to callbacks expected by protocol's column info processor.
  Basically, each string in a list is reported as column name. Other column
  specification parameters, such as alias, are not reported.
*/

struct String_to_col_prc_converter
  : public Converter<
    String_to_col_prc_converter,
    cdk::api::String_processor,
    cdk::protocol::mysqlx::api::Columns::Processor::Element_prc
  >
{
  void val(const string &col)
  {
    m_proc->name(col);
  }

  virtual ~String_to_col_prc_converter()
  {}
};

typedef List_prc_converter<String_to_col_prc_converter> Columns_prc_converter;


/*
  Statement which creates or updates a view. It can include a find message.
  Whether update or create command should be sent is determined by the view
  specification passed when creating this statement object.
*/

template <protocol::mysqlx::Data_model DM>
class Cmd_ViewCrud
  : public Stmt_op
  , public View_spec::Processor
  , public cdk::protocol::mysqlx::api::Columns
  , public protocol::mysqlx::api::View_options
{
  const View_spec *m_view = nullptr;
  Cmd_Find<DM> *m_find = nullptr;
  View_spec::op_type  m_type = CREATE;
  bool   m_has_cols = false;
  bool   m_has_opts = false;

  // Columns

  void
  process(cdk::protocol::mysqlx::api::Columns::Processor &prc) const override
  {
    assert(m_view);

    /*
      Column names are reported to the protocol layer as column specification
      (as used by snd_Insert() for example). We use processor converter
      to convert string list processor callbacks to these of Columns
      specification processor.
    */

    Columns_prc_converter conv;
    conv.reset(prc);

    /*
      Process view specification extracting columns information and passing
      it to the converter.
    */

    struct : public cdk::View_spec::Processor
    {
      String_list::Processor *m_prc;

      void name(const Table_ref&, op_type) {}

      Options::Processor* options()
      {
        return nullptr;
      }

      List_processor* columns()
      {
        return m_prc;
      }

    }
    vprc;

    vprc.m_prc = &conv;
    m_view->process(vprc);
  }

  protocol::mysqlx::api::Columns*
  get_cols()
  {
    return m_has_cols ? this : nullptr;
  }

  // View_options

  void
  process(protocol::mysqlx::api::View_options::Processor &prc) const override
  {
    assert(m_view);

    /*
      Process view specification extracting options information and passing
      it to the processor.
    */

    struct Opts : public cdk::View_spec::Processor
    {
      Options::Processor *m_prc;

      void name(const Table_ref&, op_type)
      {}

      Options::Processor* options()
      {
        return m_prc;
      }

      List_processor* columns()
      {
        return nullptr;
      }
    }
    vprc;

    vprc.m_prc = &prc;
    m_view->process(vprc);
  }

  protocol::mysqlx::api::View_options*
  get_opts()
  {
    return m_has_opts ? this : nullptr;
  }

  const protocol::mysqlx::api::Args_map*
  get_args()
  {
    return m_find->m_param_map;
  }


  Proto_op* send_cmd() override
  {
    switch (m_type)
    {
    case CREATE:
    case REPLACE:
      return &get_protocol().snd_CreateView(
        DM, *this, *m_find,
        get_cols(), REPLACE == m_type,
        get_opts(), get_args()
      );

    case UPDATE:
      return &get_protocol().snd_ModifyView(
        DM, *this, *m_find,
        get_cols(), get_opts(),
        m_find->m_param_map
      );

    default:
      assert(false);
      return nullptr;  // quiet compile warnings
    }
  }

public:

  Cmd_ViewCrud(Session &s, const View_spec &view, Cmd_Find<DM> *find = nullptr)
    : Stmt_op(s)
    , m_view(&view), m_find(find)
  {
    /*
      Process view specification to extract view name and information which
      type of view operation should be sent (m_update member). This also
      determines whether columns and options information is present in the
      specification.
    */
    view.process(*this);
  }

  ~Cmd_ViewCrud()
  {
    delete m_find;
  }

private:

  // View_spec::Processor

  void name(const Table_ref &view, View_spec::op_type type) override
  {
    set(view);
    m_type = type;
  }

  List_processor* columns() override
  {
    m_has_cols = true;
    /*
      Note: we do not process columns here, it is done above when this
      object acts as protocol Columns specification.
    */
    return nullptr;
  }

  Options::Processor* options() override
  {
    m_has_opts = true;
    return nullptr;
  }

};


#if 0

class SndDropView
  : public Crud_op_base
{
  bool m_check_exists;

  Proto_op* start()
  {
    return &m_protocol.snd_DropView(*this, m_check_exists);
  }

public:

  SndDropView(
    Protocol &protocol,
    const api::Object_ref &view,
    bool check_exists
  )
    : Crud_op_base(protocol, view)
    , m_check_exists(check_exists)
  {}

};

#endif

// -------------------------------------------------------------------------


/*
   Update_converter
*/

class Update_prc_converter
    : public Converter<
        Update_prc_converter,
        cdk::Update_processor,
        cdk::protocol::mysqlx::Update_processor
      >
{

public:


  cdk::protocol::mysqlx::Data_model m_dm;

//  protocol::mysqlx::update_op::value m_type;

  struct Table : public protocol::mysqlx::api::Db_obj
  {
    string m_table_name;
    bool m_has_schema;
    string m_schema_name;

    virtual ~Table() {}

    //DB_OBJ
    const string& get_name() const
    {
      return m_table_name;
    }

    const string* get_schema() const
    {
      return m_has_schema ? &m_schema_name : nullptr;
    }

  } m_table;

public:

  virtual ~Update_prc_converter() {}

  void set_data_model(cdk::protocol::mysqlx::Data_model dm)
  {
    m_dm = dm;
  }


  //Update_processor

  virtual void column(const api::Column_ref &column)
  {
    if (cdk::protocol::mysqlx::DOCUMENT == m_dm)
      cdk::throw_error("Using column() in document mode");

    m_proc->target_name(column.name());

    if (column.table())
    {
      m_table.m_table_name =  column.table()->name();
      if (column.table()->schema())
      {
        m_table.m_has_schema = true;
        m_table.m_schema_name = column.table()->schema()->name();
      }
      else
      {
        m_table.m_has_schema = false;
      }

      m_proc->target_table(m_table);
    }

  }

  virtual void remove(const Doc_path *path)
  {
    report_path(path);

    m_proc->update_op(protocol::mysqlx::update_op::ITEM_REMOVE);
  }


  Expr_prc_converter  m_conv;


  Expr_prc* set(const Doc_path *path, unsigned flags)
  {
    Prc_to::Expr_prc  *prc;

    report_path(path);

    if (flags & Update_processor::NO_INSERT)
      prc = m_proc->update_op(protocol::mysqlx::update_op::ITEM_REPLACE);
    else if(flags & Update_processor::NO_OVERWRITE)
      prc = m_proc->update_op(protocol::mysqlx::update_op::ITEM_MERGE);
    else
      if (cdk::protocol::mysqlx::DOCUMENT == m_dm)
        prc = m_proc->update_op(protocol::mysqlx::update_op::ITEM_SET);
      else
        prc = m_proc->update_op(path ? protocol::mysqlx::update_op::ITEM_SET
                                     : protocol::mysqlx::update_op::SET);

    if (!prc)
      return nullptr;

    m_conv.reset(*prc);
    return &m_conv;
  }


  Expr_prc* array_insert(const Doc_path *path)
  {
    report_path(path);

    Prc_to::Expr_prc *prc
      = m_proc->update_op(protocol::mysqlx::update_op::ARRAY_INSERT);

    if (!prc)
      return nullptr;

    m_conv.reset(*prc);
    return &m_conv;
  }

  Expr_prc* array_append(const Doc_path *path)
  {
    report_path(path);

    Prc_to::Expr_prc *prc
      = m_proc->update_op(protocol::mysqlx::update_op::ARRAY_APPEND);

    if (!prc)
      return nullptr;

    m_conv.reset(*prc);
    return &m_conv;
  }

  Expr_prc* patch()
  {
    Prc_to::Expr_prc *prc
      = m_proc->update_op(protocol::mysqlx::update_op::MERGE_PATCH);

    if (!prc)
      return nullptr;

    m_conv.reset(*prc);
    return &m_conv;
  }

  void report_path(const Doc_path *path)
  {
    if (path)
    {
      Doc_path_storage dp;
      path->process(dp);
      if (!dp.is_empty())
        m_proc->target_path(dp);
    }
  }
};


class Update_converter
  : public Expr_conv_base<
      Update_prc_converter,
      cdk::Update_spec,
      protocol::mysqlx::Update_spec
    >
{
public:

  Update_converter(cdk::protocol::mysqlx::Data_model dm,
                   const cdk::Update_spec &us)
  {
    m_conv.set_data_model(dm);
    reset(us);
  }

  bool next()
  {
    assert(m_expr);
    return const_cast<cdk::Update_spec*>(m_expr)->next();
  }
};


template <protocol::mysqlx::Data_model DM>
class Cmd_Update
    : public Cmd_Select<>
{
protected:

  Update_converter    m_upd_conv;

  Proto_op* send_cmd() override
  {
    return &get_protocol().snd_Update(
      DM, m_stmt_id, *this, m_upd_conv, m_param_map
    );
  }

public:

  Cmd_Update(
    Session& s,
    uint32_t stmt_id,
    const api::Table_ref &table,
    const cdk::Expression *expr,
    const cdk::Update_spec &us,
    const cdk::Order_by *order_by,
    const cdk::Limit *lim = nullptr,
    const cdk::Param_source *param = nullptr
  )
    : Cmd_Select(s, stmt_id, table, expr, order_by, lim, param)
    , m_upd_conv(DM, us)
  {}

};


}} // cdk::mysqlx

#endif
