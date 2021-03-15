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
  Implementation of mysqlx protocol API: CRUD Implementation
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
  Helper function template to set a db object for a given message of MSG class
*/

template <class MSG> void set_db_obj(const api::Db_obj &db_obj, MSG &msg)
{
  Mysqlx::Crud::Collection *proto_collect = msg.mutable_collection();

  proto_collect->set_name(db_obj.get_name());

  const string *collect_schema = db_obj.get_schema();
  if (collect_schema)
    proto_collect->set_schema(*collect_schema);
}


/*
  Helper function template to set a data model for a given message of MSG class
*/

template <class MSG> void set_data_model(Data_model dm, MSG &msg)
{
  if (dm != DEFAULT)
    msg.set_data_model(static_cast<Mysqlx::Crud::DataModel>(dm));
}


/*
  Helper function template to set `limit` field inside message of type MSG.
*/


void set_arg_scalar(Mysqlx::Datatypes::Scalar &msg, row_count_t val)
{
  msg.set_type( Mysqlx::Datatypes::Scalar_Type::Scalar_Type_V_UINT);
  msg.set_v_unsigned_int(val);
}

void set_arg_scalar(Mysqlx::Datatypes::Any &msg, row_count_t val)
{
  msg.set_type(::Mysqlx::Datatypes::Any_Type::Any_Type_SCALAR);
  set_arg_scalar(*msg.mutable_scalar(), val);
}

template <bool enable_offset,class MSG,
          class MSG_Params>
void set_limit_(const api::Limit &lim,
                MSG &msg,
                MSG_Params &msg_args)
{
  auto limit = msg.mutable_limit_expr();
  auto row_count_msg = limit->mutable_row_count();

  row_count_msg->set_type(Mysqlx::Expr::Expr::PLACEHOLDER);
  row_count_msg->set_position(0);
  set_arg_scalar(*msg_args.add_args(), lim.get_row_count());

  if (enable_offset)
  {
    auto offset_msg = limit->mutable_offset();
    offset_msg->set_type(Mysqlx::Expr::Expr::PLACEHOLDER);
    offset_msg->set_position(1);
  }
  //set_arg_scalar is out of enable_offset because the offset ARG is always sent
  //on the PrepareExecute, becaus ethere we don't know which type of message we
  //are executing
  set_arg_scalar(*msg_args.add_args(),
                 lim.get_offset() ? *lim.get_offset() : 0);

}

template <class MSG>
void set_limit_(const api::Limit &lim,
                MSG &msg)
{
  Mysqlx::Crud::Limit *proto_lim = msg.mutable_limit();

  proto_lim->set_row_count(lim.get_row_count());
  const row_count_t *lim_offset = lim.get_offset();

  if (lim_offset)
    proto_lim->set_offset(*lim_offset);
}

template<bool enable_offset,class MSG,
         class MSG_Params>
void set_limit_(const api::Limit &limit,
                MSG &msg,
                Placeholder_conv_imp &conv,
                MSG_Params &msg_args)
{
  conv.set_offset(2);

  set_limit_<enable_offset>(limit, msg, msg_args);
}


/*
  Helper function template to set `criteria` field within a message of type
  MSG.
*/

template <class MSG> void set_criteria(const api::Expression &api_expr,
                                       MSG &msg, Args_conv &conv)
{
  Mysqlx::Expr::Expr *pb_expr = msg.mutable_criteria();

  Expr_builder eb(*pb_expr, &conv);
  api_expr.process(eb);
}


/*
  Helper function template to set selection parameters given by a
  `Select_spec` object.
*/

template <class MSG>
void set_select(const Select_spec &sel, MSG &msg, Args_conv &conv)
{
  set_db_obj(sel.obj(), msg);

  if (sel.select())
    set_criteria(*sel.select(), msg, conv);

  if (sel.order())
    set_order_by(*sel.order(), msg, conv);

}

// -------------------------------------------------------------------------

/*
  Store single projection information in a Crud::Projection message.

  This builder for a single projection is then used to construct full projection
  builder for Crud::Find message using Array_builder<> template.
*/

struct Projection_builder
  : public Builder_base<Mysqlx::Crud::Projection,
                        api::Projection::Processor::Element_prc>
{
  Expr_builder m_expr_builder;

  void reset(Message &msg, Args_conv *conv = nullptr)
  {
    Builder_base::reset(msg, conv);
    m_expr_builder.reset(*msg.mutable_source(), conv);
  }

  Expr_prc* expr()
  {
    return &m_expr_builder;
  }

  void alias(const string &a)
  {
    m_msg->set_alias(a);
  }
};


/*
  Alternative to Arr_msg_traits<Crud::Find> which is used with Array_builder<>
  tempalte to create a builder which stores single projection specifications
  in repeated `projection` field in the Find message.
*/

struct Proj_msg_traits
{
  typedef Mysqlx::Crud::Find       Array;
  typedef Mysqlx::Crud::Projection Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_projection();
  }
};


// -------------------------------------------------------------------------


/*
  Storing Order_by information inside protocol commands. Messages that
  store Order_by information have a repeated `order` field of type
  Mysqlx::Crud::Order. The Order_builder class defines a builder that can
  fill a single Mysqlx::Crud::Order sub-message given sort key information
  form order expression. From this builder an array builder is created
  which can process a list of order expressions and store order information
  in repeated `order` fields of a command message.
*/

struct Order_builder
  : public Builder_base<Mysqlx::Crud::Order, api::Order_expr::Processor>
{
  Expr_builder m_expr_builder;

  void reset(Message &msg, Args_conv *conv = nullptr)
  {
    Builder_base::reset(msg, conv);
    m_expr_builder.reset(*msg.mutable_expr(), conv);
  }

  Expr_prc* sort_key(api::Sort_direction::value dir)
  {
    m_msg->set_direction(dir == api::Sort_direction::ASC ?
                         Message::ASC : Message::DESC);
    return &m_expr_builder;
  }
};


/*
  Alternative to Arr_msg_traits<MSG> which is used with Array_builder<MSG>
  to crate a builder which stores array elements in a repeated `order` field
  within the message.
*/

template<class MSG>
struct Ord_msg_traits
{
  typedef MSG                 Array;
  typedef Mysqlx::Crud::Order Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_order();
  }
};


/*
  Helper function template to store order by information within message of
  type MSG. It uses Ord_msg_traits<> which assume that order by information
  is stored in a repeated `order` field within the message.
*/

template <class MSG> void set_order_by(const api::Order_by &order_by,
                                       MSG &msg,
                                       Args_conv &conv)
{
  Array_builder<Order_builder, MSG, Ord_msg_traits<MSG> > ord_builder;
  ord_builder.reset(msg, &conv);
  order_by.process(ord_builder);
}


// -------------------------------------------------------------------------



class Any_to_Scalar_builder
  : public cdk::api::Any_processor<api::Scalar_processor>
{
  Scalar_builder m_builder;

public:

  void reset(Mysqlx::Datatypes::Scalar &msg)
  {
    m_builder.reset(msg);
  }

  virtual Scalar_prc* scalar()
  {
    return &m_builder;
  }

  // Report that any value is an array, that is, a list of any expressions.

  virtual List_prc*   arr()
  {
    throw Generic_error("Array not supported on parameters.");
  }

  // Report that any value is a document.

  virtual Doc_prc*    doc()
  {
    throw Generic_error("Document not supported on parameters.");
  }

};



template <class MSG>
class Param_builder
    : public api::Args_map::Processor
{

  MSG &m_msg;
  Placeholder_conv_imp &m_conv;
  Any_to_Scalar_builder m_builder;


public:
  Param_builder(MSG &msg, Placeholder_conv_imp &conv)
    : m_msg(msg)
    , m_conv(conv)
  {}


  Mysqlx::Datatypes::Scalar& get_scalar(Mysqlx::Datatypes::Scalar &arg)
  {
    return arg;
  }

  Mysqlx::Datatypes::Scalar& get_scalar(Mysqlx::Datatypes::Any &arg)
  {
    arg.set_type(Mysqlx::Datatypes::Any_Type_SCALAR);
    return *arg.mutable_scalar();
  }

  virtual Any_prc* key_val(const string &key)
  {
    m_builder.reset(get_scalar(*m_msg.add_args()));

    m_conv.add_placeholder(key);

    return &m_builder;
  }

};



/*
  Helper function template to add parameter (arguments) values to
  a given message of MSG class. At the same time it builds and
  stores name->position map in the map argument.
*/

template <class MSG> void set_args_(const api::Args_map &args, MSG &msg,
                                    Placeholder_conv_imp &map)
{
  Param_builder<MSG> param_builder(msg, map);
  args.process(param_builder);
}



/*
  MessageBuilder implementation
*/

template<msg_type::value T>
void Msg_builder<T>::set_args(const api::Args_map *args)
{
  if (args)
  {
    if (m_stmt_id != 0)
      set_args_(*args, m_prepare_execute, m_conv);
    else
    {
      set_args_(*args, m_msg, m_conv);
    }
  }
}



template<msg_type::value T>
void Msg_builder<T>::set_limit(const api::Limit *limit)
{
  if (limit)
  {
    if (m_stmt_id != 0)
      set_limit_<Prepare_traits<T>::has_offset>(*limit, m_msg, m_conv, m_prepare_execute);
    else
      set_limit_(*limit, m_msg);
  }

}

// -------------------------------------------------------------------------

/*
  Helper function to fill ColumnIdentifier sub-message from information
  given by api::Doc_path object.
 */

void set_doc_path(Mysqlx::Expr::ColumnIdentifier *p_col_id,
                  const api::Doc_path &doc)
{
  for (unsigned pos = 0; pos < doc.length(); ++pos)
  {
    Mysqlx::Expr::DocumentPathItem *dpi = p_col_id->add_document_path();

    switch (doc.get_type(pos))
    {
    case api::Doc_path::MEMBER:
      dpi->set_type(Mysqlx::Expr::DocumentPathItem::MEMBER);
      if (doc.get_name(pos))
        dpi->set_value(*doc.get_name(pos));
      break;

    case api::Doc_path::MEMBER_ASTERISK:
      dpi->set_type(Mysqlx::Expr::DocumentPathItem::MEMBER_ASTERISK);
      break;

    case api::Doc_path::ARRAY_INDEX:
      dpi->set_type(Mysqlx::Expr::DocumentPathItem::ARRAY_INDEX);
      if (doc.get_index(pos))
        dpi->set_index(*doc.get_index(pos));
      break;

    case api::Doc_path::ARRAY_INDEX_ASTERISK:
      dpi->set_type(Mysqlx::Expr::DocumentPathItem::ARRAY_INDEX_ASTERISK);
      break;

    case api::Doc_path::DOUBLE_ASTERISK:
      dpi->set_type(Mysqlx::Expr::DocumentPathItem::DOUBLE_ASTERISK);
      break;

    default: break;
    }
  }
}



// -------------------------------------------------------------------------


/*
  Storing Group_by information inside Find protocol command.
  This command has a repeated `grouping` field of type
  Mysqlx::Expr::Expr. Below we fill it using a builder created
  from Array_builer<> template. Such builder process list of
  expressions and stores each expression inside Find message
  in a submessage appended with Group_by_traits::add_element()
  method (the Group_by_traits structure is passed to the Array_builder<>
  template when the builder is created).
*/

struct Group_by_traits
{
  typedef Mysqlx::Crud::Find    Array;
  typedef Mysqlx::Expr::Expr    Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_grouping();
  }
};

void set_find(Mysqlx::Crud::Find &msg,
              Data_model dm, const Find_spec &fs, Placeholder_conv_imp &conv)
{

  set_data_model(dm, msg);

  set_select(fs, msg, conv);

  if (fs.project())
  {
    Array_builder<Projection_builder, Mysqlx::Crud::Find, Proj_msg_traits>
                 proj_builder;
    proj_builder.reset(msg, &conv);
    fs.project()->process(proj_builder);
  }

  if (fs.group_by())
  {
    Array_builder<Expr_builder, Mysqlx::Crud::Find, Group_by_traits>
      group_by_builder;
    group_by_builder.reset(msg, &conv);
    fs.group_by()->process(group_by_builder);
  }

  if (fs.having())
  {
    Having_builder expr_builder;
    expr_builder.reset(*msg.mutable_grouping_criteria());
    fs.having()->process(expr_builder);
  }

  switch (fs.locking())
  {
    case api::Lock_mode_value::EXCLUSIVE:
      msg.set_locking(Mysqlx::Crud::Find_RowLock_EXCLUSIVE_LOCK);
    break;
    case api::Lock_mode_value::SHARED:
      msg.set_locking(Mysqlx::Crud::Find_RowLock_SHARED_LOCK);
    break;
    case api::Lock_mode_value::NONE:
    default: // do nothing
    break;
  }

  switch (fs.contention())
  {
    case api::Lock_contention_value::NOWAIT:
      msg.set_locking_options(Mysqlx::Crud::Find_RowLockOptions_NOWAIT);
    break;
    case api::Lock_contention_value::SKIP_LOCKED:
      msg.set_locking_options(Mysqlx::Crud::Find_RowLockOptions_SKIP_LOCKED);
    break;
    case api::Lock_contention_value::DEFAULT:
    default: // do nothing
    break;
  }

}


Protocol::Op&
Protocol::snd_Find(Data_model dm,  uint32_t stmt_id, const Find_spec &fs, const api::Args_map *args)
{
  Msg_builder<msg_type::cli_CrudFind> find(get_impl(), stmt_id);

  find.set_limit(fs.limit());
  find.set_args(args);

  set_find(find.msg(), dm, fs, find.conv());

  return find.send();
}




// -------------------------------------------------------------------------


template<>
struct Arr_msg_traits<Mysqlx::Crud::Insert_TypedRow>
{
  typedef Mysqlx::Crud::Insert_TypedRow Array;
  typedef Mysqlx::Expr::Expr            Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_field();
  }
};


/*
  Filling projection information inside Insert message.

  Class Proj_builder fills single Crud::Column sub-message. It is used to
  create full projection builder with Array_builder<> template. This full
  builder processes an api::Columns list and for each element in the list,
  adds new Crud::Column sub-message to the Insert message using
  Proj_traits::add_element(), then fills it with data from the list element
  using Proj_builder.
*/

struct Proj_builder
  : Builder_base<Mysqlx::Crud::Column, Columns::Processor::Element_prc>
{
  using Builder_base<Mysqlx::Crud::Column, Columns::Processor::Element_prc>::m_msg;

  void name(const string &n) { m_msg->set_name(n); }
  void alias(const string &a)  { m_msg->set_alias(a); }

  Path_prc* path()
  {
    // TODO
    THROW("Paths in column projections not implemented");
  }
};

struct Proj_traits
{
  typedef Mysqlx::Crud::Insert  Array;
  typedef Mysqlx::Crud::Column  Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_projection();
  }
};

void set_insert(Mysqlx::Crud::Insert &insert,
                 Data_model dm,
                 api::Db_obj &db_obj,
                 const api::Columns *columns,
                 Row_source &rs,
                 Placeholder_conv_imp& conv,
                 bool upsert)
{

  set_db_obj(db_obj, insert);
  set_data_model(dm, insert);

  if (columns)
  {
    Array_builder<Proj_builder, Mysqlx::Crud::Insert, Proj_traits> proj_builder;
    proj_builder.reset(insert);
    columns->process(proj_builder);
  }

  while (rs.next())
  {
    Mysqlx::Crud::Insert_TypedRow *msg = insert.add_row();

    Array_builder<Expr_builder, Mysqlx::Crud::Insert_TypedRow> row_builder;

    row_builder.reset(*msg, &conv);
    rs.process(row_builder);
  }

  insert.set_upsert(upsert);
}

Protocol::Op&
Protocol::snd_Insert(
    Data_model dm,
    uint32_t stmt_id,
    api::Db_obj &db_obj,
    const api::Columns *columns,
    Row_source &rs,
    const api::Args_map *args,
    bool upsert)
{
  Msg_builder<msg_type::cli_CrudInsert> insert(get_impl(), stmt_id);

  insert.set_args(args);

  set_insert(insert.msg(), dm, db_obj, columns, rs, insert.conv(), upsert);

  return insert.send();
}


// -------------------------------------------------------------------------


class Update_builder
    : public Update_processor
{
private:

  Mysqlx::Crud::UpdateOperation  &m_upd_op;
  Mysqlx::Expr::ColumnIdentifier &m_source;
  Args_conv                      &m_conv;

  scoped_ptr<Expr_builder> m_expr_builder;

public:

  Update_builder(::Mysqlx::Crud::UpdateOperation &upd_op, Args_conv &conv)
    : m_upd_op(upd_op)
    , m_source (*m_upd_op.mutable_source())
    , m_conv(conv)
  {}

  ~Update_builder()
  {
  }

  virtual void target_name(const string &name)
  {
    m_source.set_name(name);
  }

  virtual void target_table(const api::Db_obj &table)
  {
    m_source.set_table_name(table.get_name());
    const string* schema = table.get_schema();
    if (schema)
      m_source.set_schema_name(*schema);
  }

  virtual void target_path(const api::Doc_path &path)
  {
    set_doc_path(&m_source, path);
  }

  Expr_prc* update_op(update_op::value type)
  {
    m_upd_op.set_operation((Mysqlx::Crud::UpdateOperation::UpdateType) type);

    switch(type)
    {
    case update_op::ITEM_REMOVE:
      return nullptr; //Doesn't have value;

    case update_op::SET:
    case update_op::ITEM_SET:
    case update_op::ITEM_MERGE:
    case update_op::ITEM_REPLACE:
    case update_op::ARRAY_INSERT:
    case update_op::ARRAY_APPEND:
    case update_op::MERGE_PATCH:
    default:
      {
        m_expr_builder.reset(new Expr_builder(*m_upd_op.mutable_value(), &m_conv));
        return m_expr_builder.get();
      }
    };
  }

};

void set_update(Mysqlx::Crud::Update &update,
                 Data_model dm,
                 const Select_spec &sel,
                 Update_spec &us,
                 Placeholder_conv_imp &conv)
{

  set_data_model(dm, update);

  set_select(sel, update, conv);

  while (us.next())
  {
    Update_builder prc(*update.add_operation(), conv);
    us.process(prc);
  }
}

Protocol::Op& Protocol::snd_Update(
    Data_model dm,
    uint32_t stmt_id,
    const Select_spec &sel,
    Update_spec &us,
    const api::Args_map *args)
{
  Msg_builder<msg_type::cli_CrudUpdate> update(get_impl(), stmt_id);

  update.set_limit(sel.limit());

  update.set_args(args);

  set_update(update.msg(), dm, sel, us, update.conv());

  return update.send();
}



// -------------------------------------------------------------------------


void set_delete(Mysqlx::Crud::Delete &del,
                Data_model dm,
                const Select_spec &sel,
                Placeholder_conv_imp &conv)
{

  set_data_model(dm, del);


  set_select(sel, del, conv);

}

Protocol::Op&
Protocol::snd_Delete(Data_model dm,
                     uint32_t stmt_id,
                     const Select_spec &sel,
                     const api::Args_map *args)
{
  Msg_builder<msg_type::cli_CrudDelete> del(get_impl(), stmt_id);

  del.set_limit(sel.limit());

  del.set_args(args);

  set_delete(del.msg(),dm, sel, del.conv());

  return del.send();
}


// -------------------------------------------------------------------------

Protocol::Op&
Protocol::snd_PrepareExecute(uint32_t stmt_id,
                             const api::Limit *lim,
                             const api::Args_map *args)
{
  auto& prepare_execute = get_impl().m_prepare_execute;
  auto& conv = get_impl().m_args_conv;

  if (lim || args)
  {
    conv.clear();
    prepare_execute.Clear();
  }

  if (lim)
  {
    set_arg_scalar(*prepare_execute.add_args(), lim->get_row_count());
    set_arg_scalar(*prepare_execute.add_args(),
                   lim->get_offset()? *lim->get_offset() : 0);
  }

  if (args)
  {
    set_args_(*args, prepare_execute, conv);
  }

  prepare_execute.set_stmt_id(stmt_id);

  return get_impl().snd_start(prepare_execute, msg_type::cli_PrepareExecute);
}


Protocol::Op&
Protocol::snd_PrepareDeallocate(uint32_t id)
{
  Mysqlx::Prepare::Deallocate deallocate;
  deallocate.set_stmt_id(id);
  return get_impl().snd_start(deallocate, msg_type::cli_PrepareDealocate);
}


// -------------------------------------------------------------------------


template <class MSG>
void set_view_columns(MSG &msg, const api::Columns &cols)
{
  struct
    : public api::Columns::Processor
    , api::Columns::Processor::Element_prc
  {
    MSG *m_msg;

    // List processor

    void list_begin() {}
    void list_end() {}

    Element_prc* list_el()
    {
      return this;
    }

    // Column_processor

    void name(const string &col)
    {
      m_msg->add_column(col);
    }

    virtual void alias(const string&)
    {
      THROW(
        "Unexpected column alias specification when processing view columns"
      );
    }

    virtual Path_prc* path()
    {
      THROW(
        "Unexpected path specification when processing view columns"
      );
    }
  }
  prc;

  prc.m_msg = &msg;
  cols.process(prc);
}


template <class MSG>
void set_view_options(MSG &msg, api::View_options &opts)
{
  struct : public api::View_options::Processor
  {
    MSG *m_msg;

    void definer(const string &user)
    {
      m_msg->set_definer(user);
    }

    void security(View_security_t security)
    {
      switch (security)
      {
      case cdk::api::View_security::DEFINER:
        m_msg->set_security(Mysqlx::Crud::DEFINER);
        break;
      case cdk::api::View_security::INVOKER:
        m_msg->set_security(Mysqlx::Crud::INVOKER);
        break;
      }
    }

    void algorithm(View_algorithm_t alg)
    {
      switch (alg)
      {
      case cdk::api::View_algorithm::UNDEFINED:
        m_msg->set_algorithm(Mysqlx::Crud::UNDEFINED);
        break;
      case cdk::api::View_algorithm::MERGE:
        m_msg->set_algorithm(Mysqlx::Crud::MERGE);
        break;
      case cdk::api::View_algorithm::TEMPTABLE:
        m_msg->set_algorithm(Mysqlx::Crud::TEMPTABLE);
        break;
      }
    }

    void check(View_check_t check)
    {
      switch (check)
      {
      case cdk::api::View_check::LOCAL:
        m_msg->set_check(Mysqlx::Crud::LOCAL);
        break;
      case cdk::api::View_check::CASCADED:
        m_msg->set_check(Mysqlx::Crud::CASCADED);
        break;
      }
    }
  }
  prc;

  prc.m_msg = &msg;
  opts.process(prc);
}


Protocol::Op&
Protocol::snd_CreateView(
  Data_model dm, const api::Db_obj &obj,
  const Find_spec &query, const api::Columns *cols,
  bool replace,
  api::View_options *opts,
  const api::Args_map *args
)
{
  Mysqlx::Crud::CreateView view;

  set_db_obj(obj, view);
  view.set_replace_existing(replace);

  if (cols)
    set_view_columns(view, *cols);

  if (opts)
    set_view_options(view, *opts);

  Placeholder_conv_imp conv;
  auto stmt = *view.mutable_stmt();
  if (args)
  {
    set_args_(*args, stmt, conv);
  }

  set_find(stmt, dm, query, conv);
  return get_impl().snd_start(view, msg_type::cli_CreateView);
}


Protocol::Op&
Protocol::snd_ModifyView(
  Data_model dm, const api::Db_obj &obj,
  const Find_spec &query, const api::Columns *cols,
  api::View_options *opts,
  const api::Args_map *args
)
{
  Mysqlx::Crud::ModifyView  modify;

  set_db_obj(obj, modify);

  if (cols)
    set_view_columns(modify, *cols);

  if (opts)
    set_view_options(modify, *opts);

  auto stmt = *modify.mutable_stmt();

  Placeholder_conv_imp conv;
  if (args)
  {
    set_args_(*args, stmt, conv);
  }

  set_find(stmt, dm, query, conv);

  return get_impl().snd_start(modify, msg_type::cli_ModifyView);
}


Protocol::Op& Protocol::snd_DropView(const api::Db_obj &obj, bool check_exists)
{
  Mysqlx::Crud::DropView  drop;
  set_db_obj(obj, drop);
  drop.set_if_exists(!check_exists);
  return get_impl().snd_start(drop, msg_type::cli_DropView);
}


}}}  // cdk::protocol::mysqlx
