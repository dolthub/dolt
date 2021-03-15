/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

#ifndef XAPI_INTERNAL_CRUD
#define XAPI_INTERNAL_CRUD

#include "../common/op_impl.h"

/*
  CRUD implementation
  Copy constructor must be disabled for this class
*/

using namespace ::mysqlx::impl::common;
using namespace ::mysqlx;


uint32_t get_type(const Format_info&);


struct mysqlx_row_struct
  : public Mysqlx_diag
  , public Row_impl<>
{
  using Row_impl<>::Row_impl;
};


struct mysqlx_result_struct
  : public Mysqlx_diag
  , Result_impl
{
  using Impl = Result_impl;

  mysqlx_stmt_struct   *m_stmt;
  cdk::Diagnostic_iterator m_warn_it;

public:

  std::list<mysqlx_row_struct>  m_row_set;
  cdk::scoped_ptr<mysqlx_error_struct> m_current_warning;
  std::vector<std::string> m_doc_id_list;
  size_t m_current_id_index = 0;

public:


  mysqlx_result_struct(mysqlx_stmt_struct *sess, Result_init &init)
    : Impl(init), m_stmt(sess)
  {
    next_result();
    check_errors();
  }

  void check_errors()
  {
    // TODO: this iterator will iterate also over errors - is that ok?
    if (!m_reply)
      return;
    m_warn_it = m_reply->get_entries(cdk::api::Severity::WARNING);
    if ( 0 < m_reply->entry_count())
      set_diagnostic(mysqlx_error_struct(m_reply->get_error()));
  }

  mysqlx_error_struct *get_next_warning();

  /*
    Read the next row from the result set and advance the cursor position
  */
  mysqlx_row_struct *read_row()
  {
    const Row_data *data = get_row();
    check_errors();
    if (!data)
      return nullptr;

    m_row_set.emplace_back(*data, m_result_mdata.front());
    return &m_row_set.back();
  }


  const char * read_json(size_t *json_byte_size);

  const char *get_next_generated_id();

};


struct mysqlx_stmt_struct : public Mysqlx_diag
{
private:

  using Impl = Executable_if;
  using Result_impl = mysqlx_result_struct::Impl;

  mysqlx_session_struct &m_session;
  cdk::scoped_ptr<mysqlx_result_struct>   m_result;

public:

  cdk::scoped_ptr<Impl> m_impl;
  mysqlx_op_enum        m_op_type;

  mysqlx_stmt_struct(mysqlx_session_struct *session, mysqlx_op_t op, Impl *impl)
    : m_session(*session), m_impl(impl), m_op_type(op)
  {}

  mysqlx_result_struct* new_result(Result_init &init)
  {
    m_result.reset(new mysqlx_result_struct(this, init));
    return m_result.get();
  }

  void rm_result(mysqlx_result_struct *res)
  {
    if (res != m_result.get())
      return;
    // TODO: notify the current result so that it can cache data etc?
    m_result.reset(NULL);
  }


  mysqlx_result_struct* get_result() { return m_result.get(); }


  /*
    Execute a CRUD statement.
    RETURN: pointer to mysqlx_result_t, which is being allocated each time
            when this function is called. The old result is freed automatically.
            On error the function returns NULL

    NOTE: no need to free the result in the end cdk::scoped_ptr will
          take care of it
  */
  mysqlx_result_struct *exec()
  {
    Mysqlx_diag::clear();
    return new_result(m_impl->execute());
  }

  int sql_bind(va_list &args);
  int sql_bind(cdk::string s);

  int param_bind(va_list &args);

  /*
    Return the operation type OP_SELECT, OP_INSERT, OP_UPDATE, OP_DELETE,
    OP_FIND, OP_ADD, OP_MODIFY, OP_REMOVE, OP_SQL
  */
  mysqlx_op_t op_type() { return m_op_type; }
  mysqlx_session_t &get_session() { return m_session; }

  int set_where(const char *where_expr);
  int set_limit(cdk::row_count_t row_count, cdk::row_count_t offset);
  int set_having(const char *having_expr);

  int add_order_by(va_list &args);
  int add_row(bool get_columns, va_list &args);
  int add_columns(va_list &args);
  int add_document(const char *json_doc);
  int add_multiple_documents(va_list &args);
  int add_projections(va_list &args);
  int add_table_update_values(va_list &args);
  int add_coll_modify_values(va_list &args, mysqlx_modify_op op);
  int add_group_by(va_list &args);

  // Return the session validity state
  bool session_valid();

  void set_row_locking(mysqlx_row_locking_t row_locking,
                       mysqlx_lock_contention_t lock_contention);

  friend class Group_by_list;
};


typedef mysqlx_stmt_struct mysqlx_stmt_t;


/*
  The stmt_traits<> template defines implementation class for different
  CRUD operations identified by mysqlx_op_enum constants.
*/

template <mysqlx_op_enum OP>
struct stmt_traits;


template<>
struct stmt_traits<OP_SQL>
{
  using Impl = Op_sql;
};


template<>
struct stmt_traits<OP_TRX_BEGIN>
{
  using Impl = Op_trx<Trx_op::BEGIN>;
};

template<>
struct stmt_traits<OP_TRX_COMMIT>
{
  using Impl = Op_trx<Trx_op::COMMIT>;
};

template<>
struct stmt_traits<OP_TRX_ROLLBACK>
{
  using Impl = Op_trx<Trx_op::ROLLBACK>;
};

template<>
struct stmt_traits<OP_TRX_SAVEPOINT_SET>
{
  using Impl = Op_trx<Trx_op::SAVEPOINT_SET>;
};

template<>
struct stmt_traits<OP_TRX_SAVEPOINT_RM>
{
  using Impl = Op_trx<Trx_op::SAVEPOINT_REMOVE>;
};


template<>
struct stmt_traits<OP_SELECT>
{
  using Impl = Op_table_select;
};

template<>
struct stmt_traits<OP_INSERT>
{
  using Impl = Op_table_insert<>;
};

template<>
struct stmt_traits<OP_UPDATE>
{
  using Impl = Op_table_update;
};

template<>
struct stmt_traits<OP_DELETE>
{
  using Impl = Op_table_remove;
};


template<>
struct stmt_traits<OP_ADD>
{
  using Impl = Op_collection_add;
};

template<>
struct stmt_traits<OP_REMOVE>
{
  using Impl = Op_collection_remove;
};

template<>
struct stmt_traits<OP_FIND>
{
  using Impl = Op_collection_find;
};

template<>
struct stmt_traits<OP_MODIFY>
{
  using Impl = Op_collection_modify;
};

template<>
struct stmt_traits<OP_SCHEMA_CREATE>
{
  using Impl = Op_create<Object_type::SCHEMA>;
};

template<>
struct stmt_traits<OP_SCHEMA_DROP>
{
  using Impl = Op_drop<Object_type::SCHEMA>;
};

template<>
struct stmt_traits<OP_COLLECTION_DROP>
{
  using Impl = Op_drop<Object_type::COLLECTION>;
};

template<>
struct stmt_traits<OP_LIST_SCHEMAS>
{
  using Impl = Op_list<Object_type::SCHEMA>;
};

template<>
struct stmt_traits<OP_LIST_COLLECTIONS>
{
  using Impl = Op_list<Object_type::COLLECTION>;
};

template<>
struct stmt_traits<OP_LIST_TABLES>
{
  using Impl = Op_list<Object_type::TABLE>;
};

template<>
struct stmt_traits<OP_IDX_CREATE>
{
  using Impl = Op_idx_create;
};


template<>
struct stmt_traits<OP_IDX_DROP>
{
  using Impl = Op_idx_drop;
};


/*
  Return pointer to internal statement implementation after casting it to
  the appropriate implementation class for operation OP (as defined
  by stmt_traits<> template).
*/

template <typename Impl>
inline
auto get_impl(mysqlx_stmt_struct *stmt)
  -> Impl*
{
  assert(stmt && stmt->m_impl);
  // TODO: dynamic_cast<> did not work - rtti not enabled?
  Impl *impl = (Impl*)(stmt->m_impl.get());
  assert(impl);
  return impl;
}

template <mysqlx_op_t OP>
inline
auto get_impl(mysqlx_stmt_struct *stmt)
  -> typename stmt_traits<OP>::Impl*
{
  if (OP != stmt->m_op_type)
    throw Mysqlx_exception("Invalid operation type");
  return get_impl<typename stmt_traits<OP>::Impl>(stmt);
}



#endif
