/*
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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

#include <mysqlx/xapi.h>
#include "mysqlx_cc_internal.h"
#include "../common/op_impl.h"

using std::string;
using namespace mysqlx::common;


Value get_value(int64_t type, va_list &args)
{
  switch (type)
  {
    case MYSQLX_TYPE_NULL:
      return {};
    case MYSQLX_TYPE_SINT:
      return va_arg(args, int64_t);
    case MYSQLX_TYPE_UINT:
      return va_arg(args, uint64_t);
    case MYSQLX_TYPE_FLOAT:
    case MYSQLX_TYPE_DOUBLE:
      // With variable parameters a float value is promoted to a double
      return va_arg(args, double);
    case MYSQLX_TYPE_BOOL:
      // With variable parameters a bool value is promoted to int
      return (bool)(va_arg(args, int) != 0);
    case MYSQLX_TYPE_STRING:
      // TODO: utf8 conversion
      return std::string(va_arg(args, char*));
    case MYSQLX_TYPE_BYTES:
    {
      cdk::byte *cb = va_arg(args, cdk::byte*);
      return { cb, va_arg(args, size_t) };
    }
    case MYSQLX_TYPE_EXPR:
      return Value::Access::mk_expr(va_arg(args, char*));

    default:
      throw_error("Unknown data type in variable argument list.");
      return Value(); // quiet compile warnings
  }
}


Value get_value(va_list &args)
{
  int64_t type = (int64_t)va_arg(args, void*);

  if (0 == type)
    throw std::out_of_range("end of variable argument list");

  return get_value(type, args);
}


/*
  Member function for binding values for parametrized SQL queries.
  This function should only be called by mysqlx_stmt_bind()

  PARAMETERS:
    args - variable list of parameters that follow as
           (mysqlx_data_type_t)type, value, ..., 0
           The list is closed by 0 value for type.

           NOTE: the list is already initialized in mysqlx_stmt_bind(),
                 so no need to call va_start()/va_end() in here.

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: Each new call resets the binds set by the previous call to
        mysqlx_stmt_t::sql_bind()
*/

int mysqlx_stmt_struct::sql_bind(va_list &args)
{
  auto *impl = get_impl<OP_SQL>(this);

  // For variadic parameters mysqlx_data_structype_t is used as value of void* pointer
  int64_t type = (int64_t)va_arg(args, void*);
  do
  {
    impl->add_param(get_value(type, args));
    type = (int64_t)va_arg(args, void*);
    // Spin in the loop until finding the end of the list or on an error
  } while (0 != (int)type);

  return RESULT_OK;
}


int mysqlx_stmt_struct::sql_bind(cdk::string s)
{
  assert(OP_SQL == m_op_type);
  assert(m_impl);

  get_impl<OP_SQL>(this)->add_param(std::string(s));

  return RESULT_OK;
}


/*
  Member function for binding values for parametrized CRUD queries.
  This function should only be called by mysqlx_stmt_bind()

  PARAMETERS:
    args - variable list of parameters.
           param_name, (mysqlx_data_structype_t)type, value, ..., PARAM_END

           The list is closed by PARAM_END value

           NOTE: the list is already initialized in mysqlx_stmt_bind(),
                 so no need to call va_start()/va_end() in here.

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: Each new call resets the binds set by the previous call to
        mysqlx_stmt_struct::param_bind()
*/

int mysqlx_stmt_struct::param_bind(va_list &args)
{
  using BImpl = Bind_if;

  BImpl *impl = get_impl<BImpl>(this);

  // For variadic parameters mysqlx_data_structype_t is used as value of void* pointer
  char *param_name_utf8 = 0;

  while((param_name_utf8 = va_arg(args, char*)) != NULL)
  {
    cdk::string param_name(param_name_utf8);
    impl->add_param(param_name, get_value(args));
  }

  return RESULT_OK;
}


int mysqlx_stmt_struct::add_columns(va_list &args)
{
  if (m_op_type != OP_INSERT)
  {
    m_error.set("Wrong operation type. Only INSERT and ADD are supported.", 0);
    return RESULT_ERROR;
  }

  auto *impl = get_impl<OP_INSERT>(this);

  // TODO: Error if no columns given?
  impl->clear_columns();

  const char *col_name_utf8 = va_arg(args, char*);
  while (col_name_utf8)
  {
    cdk::string col_name(col_name_utf8);
    impl->add_column(col_name);
    col_name_utf8 = va_arg(args, char*);
  }

  return RESULT_OK;

}


/*
  Member function for adding row values CRUD ADD.

  PARAMETERS:
    get_columns - flag indicating if the column information is present
         inside args list
    args - variable list of parameters that contains row data, but
         also can have column names (see get_columns parameter)

           NOTE: the list is already initialized in the upper level,
                 so no need to call va_start()/va_end() in here.

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: Each new call resets the column and row values
*/

int mysqlx_stmt_struct::add_row(bool get_columns, va_list &args)
{
  if (m_op_type != OP_INSERT) // && m_op_type != OP_ADD)
  {
    m_error.set("Wrong operation type. Only INSERT and ADD are supported.", 0);
    return RESULT_ERROR;
  }

  auto *impl = get_impl<OP_INSERT>(this);

  // For variadic parameters mysqlx_data_structype_t is used as value of void* pointer
  int64_t type;
  char *col_name_utf8 = NULL;

  Row_impl<> row;
  cdk::col_count_t col = 0;

  /*
    Getting column name only if the flag is set. Otherwise do not attempt it
    to avoid the stack corruption.
  */
  // TODO: Error if no values are given?

  while((!get_columns || (col_name_utf8 = va_arg(args, char*)) != NULL)
        && ((type = (int64_t)va_arg(args, void*)) != 0))
  {
    if (get_columns)
    {
      cdk::string col_name(col_name_utf8);
      impl->add_column(col_name);
    }

    row.set(col++, get_value(type, args));
  }

  impl->add_row(row);
  return RESULT_OK;
}


int mysqlx_stmt_struct::add_projections(va_list &args)
{
  char *item_utf8 = NULL;
  if (m_op_type != OP_SELECT && m_op_type != OP_FIND)
  {
    m_error.set("Wrong operation type. Only SELECT and FIND are supported.", 0);
    return RESULT_ERROR;
  }

  auto *impl = get_impl<Proj_if>(this);

  // TODO: Error if no projections passed?

  while ((item_utf8 = (char*)va_arg(args, char*)) != NULL )
  {
    cdk::string item(item_utf8);

    if (OP_FIND == m_op_type)
    {
      // For find we expect single item with document expression.
      impl->set_proj(item);
      return RESULT_OK;
    }

    impl->add_proj(item);
  }

  return RESULT_OK;
}


int mysqlx_stmt_struct::add_coll_modify_values(va_list &args, mysqlx_modify_op modify_type)
{
  char *path_utf8 = NULL;
  if (m_op_type != OP_MODIFY)
  {
    set_diagnostic("Wrong operation type. Only MODIFY is supported.", 0);
    return RESULT_ERROR;
  }

  using MImpl = Collection_modify_if;

  MImpl *impl = get_impl<MImpl>(this);

  MImpl::Operation op = MImpl::SET;

  switch (modify_type)
  {
  case MODIFY_SET: op = MImpl::SET; break;
  case MODIFY_UNSET: op = MImpl::UNSET; break;
  case MODIFY_ARRAY_INSERT: op = MImpl::ARRAY_INSERT; break;
  case MODIFY_ARRAY_APPEND: op = MImpl::ARRAY_APPEND; break;
  case MODIFY_ARRAY_DELETE: op = MImpl::ARRAY_DELETE; break;
  case MODIFY_MERGE_PATCH: op = MImpl::MERGE_PATCH; break;
  }

  int rc = RESULT_ERROR;

  while ((path_utf8 = (char*)va_arg(args, char*)) != NULL )
  {
    cdk::string path(path_utf8);

    rc = RESULT_OK;

    if (modify_type == MODIFY_UNSET || modify_type == MODIFY_ARRAY_DELETE)
    {
      impl->add_operation(op, path);
      continue;
    }
    else if (modify_type == MODIFY_MERGE_PATCH)
    {
      /*
        Note: in this case path contains the patch to be applied, which should
        be trated as an expression, not a literal string.
      */
      impl->add_operation(op, "$", Value::Access::mk_expr(path));
      // For merge only one item is expected
      return RESULT_OK;
    }

    impl->add_operation(op, path, get_value(args));
  }

  if (rc == RESULT_ERROR)
    set_diagnostic("No modifications specified for MODIFY operation.", 0);
  return rc;
}


int mysqlx_stmt_struct::add_table_update_values(va_list &args)
{
  char *column_utf8 = NULL;

  if (m_op_type != OP_UPDATE)
  {
    m_error.set("Wrong operation type. Only UPDATE is supported.", 0);
    return RESULT_ERROR;
  }

  using UImpl = Table_update_if;

  UImpl *impl = get_impl<UImpl>(this);

  int rc = RESULT_ERROR;

  while ((column_utf8 = (char*)va_arg(args, char*)) != NULL )
  {
    cdk::string column(column_utf8);
    rc = RESULT_OK;
    impl->add_set(column, get_value(args));
  }

  if (rc == RESULT_ERROR)
    set_diagnostic("No modifications specified for UPDATE operation.", 0);
  return rc;
}


/*
  Set WHERE for statement operation
  PARAMETERS:
    where_expr - character string containing WHERE clause,
                 which will be parsed as required

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: each call to this function replaces previously set WHERE
*/

#define OP_WHERE_LIST(X) \
  X(SELECT) X(DELETE) X(UPDATE) X(FIND) X(MODIFY) X(REMOVE)
#define OP_CASE(X) case OP_##X:

int mysqlx_stmt_struct::set_where(const char *where_expr)
{
  cdk::string expr;

  // passing NULL or empty string means "no restrictions"

  if (!where_expr || !*where_expr)
    return RESULT_OK;

  expr = where_expr;

#define SET_WHERE(X) \
  case OP_##X: get_impl<OP_##X>(this)->set_where(expr); break;

  switch (m_op_type)
  {
    OP_WHERE_LIST(SET_WHERE)

  default:
    throw Mysqlx_exception(MYSQLX_ERROR_OP_NOT_SUPPORTED);
  }
  return RESULT_OK;
}


template <mysqlx_op_t OP>
void set_row_locking_helper(
  typename stmt_traits<OP>::Impl *impl,
  mysqlx_row_locking_enum  row_locking,
    mysqlx_lock_contention_t locking_contention
)
{
  assert(impl);

  if (ROW_LOCK_NONE == row_locking)
    return impl->clear_lock_mode();

  impl->set_lock_mode(Lock_mode(unsigned(row_locking)),
                      Lock_contention(unsigned(locking_contention)));
}


void mysqlx_stmt_struct::set_row_locking(
    mysqlx_row_locking_t row_locking,
    mysqlx_lock_contention_t lock_contention)
{
  switch (m_op_type)
  {
  case OP_SELECT:
    set_row_locking_helper<OP_SELECT>(get_impl<OP_SELECT>(this),
                                      row_locking, lock_contention);
    break;
  case OP_FIND:
    set_row_locking_helper<OP_FIND>(get_impl<OP_FIND>(this),
                                    row_locking, lock_contention);
    break;
  default:
    throw Mysqlx_exception(MYSQLX_ERROR_OP_NOT_SUPPORTED);
  }
}


int mysqlx_stmt_struct::add_group_by(va_list &args)
{
  const char *group_by_utf8;

  while ((group_by_utf8 = va_arg(args, char*)) != NULL)
  {
    switch (m_op_type)
    {
    case OP_SELECT:
    case OP_FIND:
      break;

    default:
      throw Mysqlx_exception(MYSQLX_ERROR_OP_NOT_SUPPORTED);
    }
  }

  using GImpl = Group_by_if;
  GImpl *impl = get_impl<GImpl>(this);
  cdk::string group_by(group_by_utf8);

  impl->add_group_by(group_by);

  return RESULT_OK;
}


/*
  Set HAVING for statement operation
  PARAMETERS:
    having_expr - character string containing HAVING clause,
                  which will be parsed as required

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: each call to this function replaces previously set HAVING
*/

int mysqlx_stmt_struct::set_having(const char *having_expr_utf8)
{
  assert(having_expr_utf8);

  switch (m_op_type)
  {
  case OP_SELECT:
  case OP_FIND:
    break;

  default:
    throw Mysqlx_exception(MYSQLX_ERROR_OP_NOT_SUPPORTED);
  }

  if (!having_expr_utf8 || !*having_expr_utf8)
    throw Mysqlx_exception("Empty having expression");

  using HImpl = Having_if;
  HImpl *impl = get_impl<HImpl>(this);
  cdk::string having_expr(having_expr_utf8);

  impl->set_having(having_expr);

  return RESULT_OK;
}


/*
  Set LIMIT for CRUD operation
  PARAMETERS:
    row_count - the number of result rows to return
    offset - the number of rows to skip before starting counting

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: each call to this function replaces previously set LIMIT
*/
int mysqlx_stmt_struct::set_limit(cdk::row_count_t row_count, cdk::row_count_t offset)
{
  switch (m_op_type)
  {
    OP_WHERE_LIST(OP_CASE)  break;

  default:
    throw Mysqlx_exception(MYSQLX_ERROR_OP_NOT_SUPPORTED);
  }

  using LImpl = Limit_if;
  LImpl *impl = get_impl<LImpl>(this);

  impl->set_limit(row_count);
  if (offset != 0)
    impl->set_offset(offset);

  return RESULT_OK;
}


/*
  Set one item in ORDER BY for CRUD operation
  PARAMETERS:
    order - the character string expression describing ONE item
    direction - sort direction

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: each call to this function adds a new item to ORDER BY list
*/

int mysqlx_stmt_struct::add_order_by(va_list &args)
{
  switch (m_op_type)
  {
    OP_WHERE_LIST(OP_CASE)  break;

  default:
    throw Mysqlx_exception(MYSQLX_ERROR_OP_NOT_SUPPORTED);
  }

  using SImpl = Sort_if;
  SImpl *impl = get_impl<SImpl>(this);

  char *item_utf8 = NULL;
  mysqlx_sort_direction_enum sort_direction;

  do
  {
    item_utf8 = va_arg(args, char*);
    if (item_utf8 && *item_utf8)
    {
      cdk::string item(item_utf8);
      // mysqlx_sort_direction_t is promoted to int
      sort_direction = (mysqlx_sort_direction_enum)va_arg(args, int);
      impl->add_sort(item,
        SORT_ORDER_ASC == sort_direction ? SImpl::ASC : SImpl::DESC
      );
    }
  }
  while (item_utf8 && *item_utf8);

  return RESULT_OK;
}


int mysqlx_stmt_struct::add_document(const char *json_doc)
{
  assert(json_doc && *json_doc);

  if (m_op_type != OP_ADD)
  {
    set_diagnostic("Wrong operation type. Only ADD is supported.", 0);
    return RESULT_ERROR;
  }

  if (!json_doc || !(*json_doc))
    throw Mysqlx_exception("Missing JSON data for ADD operation.");

  auto *impl = get_impl<OP_ADD>(this);

  impl->add_json(json_doc);

  return RESULT_OK;
}


int mysqlx_stmt_struct::add_multiple_documents(va_list &args)
{
  // Note: we report error if no documents were passed
  int rc = RESULT_ERROR;
  const char *json_doc;
  while ((json_doc = va_arg(args, char*)) != NULL)
  {
    rc = add_document(json_doc);
    if (rc != RESULT_OK)
      return RESULT_ERROR;
  }
  if (rc == RESULT_ERROR)
    set_diagnostic("No documents specified for ADD operation.", 0);
  return rc;
}

template <class T>
uint64_t get_count(T &obj)
{
  mysqlx_session_struct &sess = obj.get_session();
  mysqlx_stmt_struct *stmt =
    sess.new_stmt<OP_SELECT>(obj);
  if (!stmt)
    throw_error("Failed to create statement");

  if (RESULT_OK != mysqlx_set_items(stmt, "COUNT(*)", PARAM_END))
    throw_error("Failed to bind parameter");

  return stmt->exec()->read_row()->get(0).get_uint();
}

uint64_t mysqlx_collection_struct::count()
{
  return get_count(*this);
}

uint64_t mysqlx_table_struct::count()
{
  return get_count(*this);
}
