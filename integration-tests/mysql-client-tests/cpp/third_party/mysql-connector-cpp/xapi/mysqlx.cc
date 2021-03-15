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

#include <mysqlx/common.h>
#include <mysqlx/xapi.h>
#include "mysqlx_cc_internal.h"
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <stdexcept>

using namespace mysqlx;

using common::Value;
using common::throw_error;



#define SAFE_EXCEPTION_BEGIN(HANDLE, ERR) try { \
  if (HANDLE == NULL) return ERR;

#define SAFE_EXCEPTION_END(HANDLE, ERR) } \
  catch(const cdk::Error &cdkerr) \
  { \
    HANDLE->set_diagnostic(cdkerr.what(), cdkerr.code().value()); \
    return ERR; \
  } \
  catch(const Mysqlx_exception &mysqlx_ex) \
  { \
    HANDLE->set_diagnostic(mysqlx_ex); \
    return ERR; \
  } \
  catch (const std::exception &ex) \
  { \
    HANDLE->set_diagnostic(ex.what(), 0); \
    return ERR; \
  } \
  catch(...) \
  { \
    HANDLE->set_diagnostic("Unknown error!", MYSQLX_ERR_UNKNOWN); \
    return ERR; \
  }

#define SAFE_EXCEPTION_SILENT_END(ERR) } catch(...) { return ERR; }


#define HANDLE_SESSION_ERROR(OBJ,CODE,MSG) \
    if (error) \
      *error = new mysqlx_dyn_error_struct(MSG, CODE); \
    if (OBJ) { delete OBJ; OBJ = NULL; } \


#define HANDLE_EXCEPTIONS(OBJ) \
  catch(const cdk::Error &e) \
  { HANDLE_SESSION_ERROR(OBJ, e.code().value(), e.what()); } \
  catch(const Mysqlx_exception &e) \
  { HANDLE_SESSION_ERROR(OBJ, e.code(), e.message().c_str()); } \
  catch(const std::exception &e) \
  { HANDLE_SESSION_ERROR(OBJ, 0, e.what()); } \
  catch(...) \
  { HANDLE_SESSION_ERROR(OBJ, 0, "Unknown error"); }


#define PARAM_NULL_EMPTY_CHECK(PARAM, HANDLE, ERR_MSG, ERR) if (!PARAM || !(*PARAM)) \
  { \
    HANDLE->set_diagnostic(ERR_MSG, 0); \
    return ERR; \
  }

#define PARAM_NULL_CHECK(PARAM, HANDLE, ERR_MSG, ERR) if (!PARAM) \
  { \
  HANDLE->set_diagnostic(ERR_MSG, 0); \
  return ERR; \
  }

#define OUT_BUF_CHECK(PARAM, HANDLE, ERR_MSG, ERR) if (!PARAM) \
  { \
  HANDLE->set_diagnostic(ERR_MSG, 0); \
  return ERR; \
  }


static mysqlx_client_struct *
_get_client(const char *conn_str, const char *client_opt,
            mysqlx_error_t **error)
{
  mysqlx_client_struct *cli = NULL;
  try
  {
    cli = new mysqlx_client_struct(conn_str, client_opt);
  }
  HANDLE_EXCEPTIONS(cli)
  return cli;
}

static mysqlx_client_struct *
_get_client(mysqlx_session_options_t *opt,
            mysqlx_error_t **error)
{
  mysqlx_client_struct *cli = NULL;
  try
  {
    if(!opt)
    {
      throw cdk::Error(0, "Client options structure not initialized");
    }
    cli = new mysqlx_client_struct(opt);
  }
  HANDLE_EXCEPTIONS(cli)
  return cli;
}

static mysqlx_session_struct *
_get_session(const char *host, unsigned short port, const char *user,
             const char *password, const char *database, const char *conn_str,
             mysqlx_error_t **error)
{
  mysqlx_session_struct *sess = NULL;
  try
  {

    if(!conn_str)
    {
      std::string pwd(password ? password : "");
      std::string db(database ? database : "");

      // TODO: the default user has to be determined in a more flexible way
      sess = new mysqlx_session_struct(host ? host : "localhost", port,
                                  user ? user : "root", password ? &pwd : NULL,
                                database ? &db : NULL);
    }
    else
    {
      sess = new mysqlx_session_struct(conn_str);
    }

    if (!sess->is_valid())
    {
      const cdk::Error *err = sess->get_cdk_error();
      if (err)
        throw *err;
    }
  }
  HANDLE_EXCEPTIONS(sess)
  return sess;
}


static mysqlx_session_struct *
_get_session_opt(mysqlx_session_options_t *opt,
                 mysqlx_error_t **error)
{
  mysqlx_session_struct *sess = NULL;
  try
  {

    if(!opt)
    {
      throw cdk::Error(0, "Session options structure not initialized");
    }

    sess = new mysqlx_session_struct(opt);

    if (!sess->is_valid())
    {
      const cdk::Error *err = sess->get_cdk_error();
      if (err)
        throw *err;
    }
  }
  HANDLE_EXCEPTIONS(sess)
  return sess;
}

/*
  Get client using the connection string
*/

mysqlx_client_struct * STDCALL
mysqlx_get_client_from_url(const char *conn_string,
                           const char *client_opts,
                           mysqlx_error_t **error)
{
  return _get_client(conn_string, client_opts, error);
}

/*
  Get client using options
*/

mysqlx_client_struct * STDCALL
mysqlx_get_client_from_options(mysqlx_session_options_t *opt,
                               mysqlx_error_t **error)
{
  return _get_client(opt, error);
}

/*
   Get session from client pool
*/

mysqlx_session_t * STDCALL
mysqlx_get_session_from_client(mysqlx_client_t *cli,
                               mysqlx_error_t **error)
{
  mysqlx_session_t *sess = nullptr;
  try
  {
    if (cli)
    {
      sess = new mysqlx_session_t(cli);
    }
  }
  HANDLE_EXCEPTIONS(sess)
  return sess;
}


/*
   Close client pool
*/
void mysqlx_client_close(mysqlx_client_t *cli)
{
  try {
    delete cli;
  }
  catch (...)
  {
    // Ignore errors that might happen during client/session destruction/close.
  }
}


/*
  Establish the X session using string options provided as function parameters
*/

mysqlx_session_struct * STDCALL
mysqlx_get_session(const char *host, int port, const char *user,
                   const char *password, const char *database,
                   mysqlx_error_t **error)
{
  return _get_session(host, port, user, password, database,
                      NULL, error);
}


/*
  Establish the X session using the connection string
*/

mysqlx_session_struct * STDCALL
mysqlx_get_session_from_url(const char *conn_string,
                            mysqlx_error_t **error)
{
  return _get_session(NULL, 0, NULL, NULL, NULL,
                      conn_string, error);
}


/*
  Establish the X session using the options structure
*/

mysqlx_session_struct * STDCALL
mysqlx_get_session_from_options(mysqlx_session_options_t *opt,
                                mysqlx_error_t **error)
{
  return _get_session_opt(opt, error);
}


/*
  Execute a plain SQL query (supports parameters and placeholders)
  PARAMETERS:
    sess - pointer to the current session
    query - SQL query
    length - length of the query

  RETURN:
    statement handler containing the results and/or error
    NULL is not supposed to be returned even in case of error.
    It is very unlikely for this function to end with an error
    because it does not do any parsing, parameter checking etc.
*/

mysqlx_stmt_struct * STDCALL
mysqlx_sql_new(mysqlx_session_struct *sess, const char *query,
                 uint32_t length)
{
  SAFE_EXCEPTION_BEGIN(sess, NULL)

  return sess->sql_query(query, length);

  SAFE_EXCEPTION_END(sess, NULL)
}


/*
  Function for binding values for parametrized queries.

  PARAMETERS:
    stmt - pointer to CRUD
    ...  - variadic arguments that follow in sequences like
           type1, value1, type2, value2, ..., type_n, value_n, PARAM_END
           (PARAM_END marks the end of parameters list)

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: Each new call resets the binds set by the previous call to
        mysqlx_stmt_struct::sql_bind()
*/

int mysqlx_stmt_bind(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  int res = RESULT_OK;
  va_list args;
  va_start(args, stmt);

  /*
    Processing of SQL parameters and parameters for other statement operations
    is different. Therefore, use two different bind functions depending on
    the operation type.
  */
  if (stmt->op_type() == OP_SQL)
    res = stmt->sql_bind(args);
  else
    res = stmt->param_bind(args);

  va_end(args);
  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int mysqlx_set_insert_row(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  int res = RESULT_OK;
  va_list args;
  va_start(args, stmt);
  // Just row data, no columns in args
  res = stmt->add_row(false, args);
  va_end(args);
  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int mysqlx_set_insert_columns(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  int res = RESULT_OK;
  va_list args;
  va_start(args, stmt);
  res = stmt->add_columns(args);
  va_end(args);
  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL
mysqlx_set_add_document(mysqlx_stmt_struct *stmt, const char *json_doc)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(json_doc, stmt, MYSQLX_ERROR_EMPTY_JSON, RESULT_ERROR)
  return stmt->add_document(json_doc);
  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


mysqlx_stmt_struct * STDCALL
mysqlx_table_select_new(mysqlx_table_struct *table)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  return table->get_session().new_stmt<OP_SELECT>(*table);
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_stmt_struct * STDCALL
mysqlx_table_insert_new(mysqlx_table_struct *table)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  return table->get_session().new_stmt<OP_INSERT>(*table);
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_stmt_struct * STDCALL
mysqlx_table_update_new(mysqlx_table_struct *table)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  return table->get_session().new_stmt<OP_UPDATE>(*table);
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_stmt_struct * STDCALL
mysqlx_table_delete_new(mysqlx_table_struct *table)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  return table->get_session().new_stmt<OP_DELETE>(*table);
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_stmt_struct * STDCALL
mysqlx_collection_add_new(mysqlx_collection_struct *collection)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  return collection->get_session().new_stmt<OP_ADD>(*collection);
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_stmt_struct * STDCALL
mysqlx_collection_modify_new(mysqlx_collection_struct *collection)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  return collection->get_session().new_stmt<OP_MODIFY>(*collection);
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_stmt_struct * STDCALL
mysqlx_collection_remove_new(mysqlx_collection_struct *collection)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  return collection->get_session().new_stmt<OP_REMOVE>(*collection);
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_stmt_struct * STDCALL
mysqlx_collection_find_new(mysqlx_collection_struct *collection)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  return collection->get_session().new_stmt<OP_FIND>(*collection);
  SAFE_EXCEPTION_END(collection, NULL)
}


int STDCALL mysqlx_set_where(mysqlx_stmt_struct *stmt, const char *where_expr)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)
  return stmt->set_where(where_expr);
  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_having(mysqlx_stmt_struct *stmt, const char *having_expr)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)
  return stmt->set_having(having_expr);
  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_group_by(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)
  va_list args;
  int res = RESULT_OK;
  va_start(args, stmt);
  res = stmt->add_group_by(args);
  va_end(args);
  return res;
  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL
mysqlx_set_limit_and_offset(mysqlx_stmt_struct *stmt, uint64_t row_count,
                            uint64_t offset)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)
  return stmt->set_limit(row_count, offset);
  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int mysqlx_set_row_locking(mysqlx_stmt_t *stmt, int locking, int contention)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)
  stmt->set_row_locking((mysqlx_row_locking_t)locking,
                        (mysqlx_lock_contention_t)contention);
  return RESULT_OK;
  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


/*
  Set ORDER BY clause for statement operation
  Operations supported by this function:
    SELECT, FIND, UPDATE, MODIFY, DELETE, REMOVE

  Calling it for INSERT or ADD will result in an error

  PARAMETERS:
    stmt - pointer to statement structure
    ... - variable parameters list consisting of (expression, direction) pairs
          terminated by PARAM_END:

          expr_1, direction_1, ..., expr_n, direction_n, PARAM_END
          (PARAM_END marks the end of parameters list)
          Each expression computes value used to sort
          the rows/documents in ascending or descending order,
          as determined by direction constant
          (list the direction enum names).

  RETURN:
    RESULT_OK - on success
    RESULT_ERROR - on error

  NOTE: this function is not supposed to be used directly.
        For SELECT operation the user code should use
        mysqlx_set_select_xxxx() macros that map the
        corresponding mysqlx_set_xxxx() functions.
        This way the unsupported operations will not be used.

        Eeach call of this function replaces previously set ORDER BY
*/

int STDCALL mysqlx_set_order_by(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_order_by(args);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_items(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_projections(args);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_find_projection(
  mysqlx_stmt_struct *stmt, const char *proj
)
{
  /*
    The call to mysqlx_set_items will take care of exceptions
    and operations validity
  */
  return mysqlx_set_items(stmt, proj, PARAM_END);
}


/*
  Execute a statement.
  PARAMETERS:
    stmt - pointer to statement structure

  RETURN:  A MYSQL_RESULT handle that can be used to access results
           of the operation. Returned handle is valid until the CRUD
           handle is freed (when session is closed or explicitly with
           mysqlx_free()) or until another call to mysqlx_execute()
           on the same statement handle is made. It is also possible to close
           a RESULT hanlde and free all resources used by it earlier with
           mysqlx_result_free() call.

           On error NULL is returned.
*/

mysqlx_result_struct * STDCALL mysqlx_execute(mysqlx_stmt_struct *stmt)
{
  SAFE_EXCEPTION_BEGIN(stmt, NULL)

  if (!stmt || !stmt->session_valid() || stmt->get_error())
    return NULL;

  return stmt->exec();

  SAFE_EXCEPTION_END(stmt, NULL)
}


int STDCALL mysqlx_set_update_values(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_table_update_values(args);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_modify_set(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_coll_modify_values(args, MODIFY_SET);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_modify_unset(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_coll_modify_values(args, MODIFY_UNSET);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_modify_array_insert(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_coll_modify_values(args, MODIFY_ARRAY_INSERT);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_modify_array_append(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_coll_modify_values(args, MODIFY_ARRAY_APPEND);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


int STDCALL mysqlx_set_modify_array_delete(mysqlx_stmt_struct *stmt, ...)
{
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_list args;
  int res = RESULT_OK;

  va_start(args, stmt);
  res = stmt->add_coll_modify_values(args, MODIFY_ARRAY_DELETE);
  va_end(args);

  return res;

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
}


static int
_mysqlx_set_modify_patch(mysqlx_stmt_struct *stmt, ...)
{
  int res = RESULT_OK;
  va_list args;
  SAFE_EXCEPTION_BEGIN(stmt, RESULT_ERROR)

  va_start(args, stmt);
  res = stmt->add_coll_modify_values(args, MODIFY_MERGE_PATCH);
  va_end(args);

  SAFE_EXCEPTION_END(stmt, RESULT_ERROR)
    return res;
}

int STDCALL
mysqlx_set_modify_patch(mysqlx_stmt_struct *stmt,
                        const char *patch_spec)
{
  return _mysqlx_set_modify_patch(stmt, patch_spec);
}


/*
  Fetch one row from the result and advance to the next row
  PARAMETERS:
    res - pointer to the result structure

  RETURN: pointer to mysqlx_row_struct or NULL if no more rows left
*/

mysqlx_row_struct * STDCALL mysqlx_row_fetch_one(mysqlx_result_struct *res)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->read_row();
  SAFE_EXCEPTION_END(res, NULL)
}


//mysqlx_doc_struct * STDCALL mysqlx_doc_fetch_one(mysqlx_result_struct *res)
//{
//  SAFE_EXCEPTION_BEGIN(res, NULL)
//  return res->read_doc();
//  SAFE_EXCEPTION_END(res, NULL)
//}


const char * STDCALL mysqlx_json_fetch_one(mysqlx_result_struct *res, size_t *length)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->read_json(length);
  SAFE_EXCEPTION_END(res, NULL)
}


int STDCALL _store_result(mysqlx_result_struct *result, size_t *num,
                          bool no_data_error)
{
  SAFE_EXCEPTION_BEGIN(result, RESULT_ERROR)
    if (no_data_error && !result->has_data())
      throw Mysqlx_exception("Attempt to store data for result without a data set");

  cdk::row_count_t row_num = result->count();
  if (num)
    *num = row_num;
  return RESULT_OK;
  SAFE_EXCEPTION_END(result, RESULT_ERROR)
}


int STDCALL
mysqlx_store_result(mysqlx_result_struct *result, size_t *num)
{
  return _store_result(result, num, true);
}


int STDCALL
mysqlx_get_count(mysqlx_result_struct *result, size_t *num)
{
  return _store_result(result, num, false);
}


/*
  Accessing row fields
  -------------------------------------------------------------------------
*/

using impl::common::bytes;


#define CHECK_COLUMN_RANGE(COL, ROW) if (COL >= ROW->col_count()) \
{ \
  ROW->set_diagnostic(MYSQLX_ERROR_INDEX_OUT_OF_RANGE_MSG, \
                      MYSQLX_ERROR_INDEX_OUT_OF_RANGE); \
  return RESULT_ERROR; \
}


int STDCALL mysqlx_get_bytes(
  mysqlx_row_struct* row,
  uint32_t col, uint64_t offset,
  void *buf, size_t *buf_len
)
{
  SAFE_EXCEPTION_BEGIN(row, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(buf_len, row, MYSQLX_ERROR_OUTPUT_BUFFER_ZERO, RESULT_ERROR)
  OUT_BUF_CHECK(buf, row, MYSQLX_ERROR_OUTPUT_BUFFER_NULL, RESULT_ERROR)

  CHECK_COLUMN_RANGE(col, row)
  bytes b = row->get_bytes(col);
  int rc = RESULT_OK;

  if (b.size() == 0)
    return RESULT_NULL;

  if (offset >= b.size())
  {
    // Report 0 bytes written in the buffer and do nothing else
    *buf_len = 0;
    return RESULT_OK;
  }

  if (b.size() - offset < *buf_len)
    *buf_len = b.size() - (size_t)offset;
  else
    rc = RESULT_MORE_DATA;

  memcpy(buf, b.begin() + offset, *buf_len);
  return rc;

  SAFE_EXCEPTION_END(row, RESULT_ERROR)
}



int STDCALL mysqlx_get_uint(mysqlx_row_struct* row, uint32_t col, uint64_t *val)
{
  SAFE_EXCEPTION_BEGIN(row, RESULT_ERROR)
    OUT_BUF_CHECK(val, row, MYSQLX_ERROR_OUTPUT_BUFFER_NULL, RESULT_ERROR)

  CHECK_COLUMN_RANGE(col, row)

  Value &v = row->get(col);

  if (v.is_null())
    return RESULT_NULL;

  *val = v.get_uint();
  return RESULT_OK;

  SAFE_EXCEPTION_END(row, RESULT_ERROR)
}


int STDCALL mysqlx_get_sint(mysqlx_row_struct* row, uint32_t col, int64_t *val)
{
  SAFE_EXCEPTION_BEGIN(row, RESULT_ERROR)
  OUT_BUF_CHECK(val, row, MYSQLX_ERROR_OUTPUT_BUFFER_NULL, RESULT_ERROR)
  CHECK_COLUMN_RANGE(col, row)

  Value &v = row->get(col);
  if (v.is_null())
    return RESULT_NULL;

  *val = v.get_sint();
  return RESULT_OK;

  SAFE_EXCEPTION_END(row, RESULT_ERROR)
}


int STDCALL mysqlx_get_float(mysqlx_row_struct* row, uint32_t col, float *val)
{
  SAFE_EXCEPTION_BEGIN(row, RESULT_ERROR)
  OUT_BUF_CHECK(val, row, MYSQLX_ERROR_OUTPUT_BUFFER_NULL, RESULT_ERROR)
  CHECK_COLUMN_RANGE(col, row)

  Value &v = row->get(col);
  if (v.is_null())
    return RESULT_NULL;

  if (Value::FLOAT == v.get_type())
  {
    *val = v.get_float();
  }
  else
  {
    double vd = v.get_double();
    if (
      vd > std::numeric_limits<float>::max()
      || vd < std::numeric_limits<float>::lowest()
    )
      throw Mysqlx_exception("Numeric overflow");
    *val = static_cast<float>(vd);
  }
  return RESULT_OK;

  SAFE_EXCEPTION_END(row, RESULT_ERROR)
}


int STDCALL mysqlx_get_double(mysqlx_row_struct* row, uint32_t col, double *val)
{
  SAFE_EXCEPTION_BEGIN(row, RESULT_ERROR)
  OUT_BUF_CHECK(val, row, MYSQLX_ERROR_OUTPUT_BUFFER_NULL, RESULT_ERROR)
  CHECK_COLUMN_RANGE(col, row)

  Value &v = row->get(col);
  if (v.is_null())
    return RESULT_NULL;

  *val = v.get_double();
  return RESULT_OK;

  SAFE_EXCEPTION_END(row, RESULT_ERROR)
}


/*
  Get the number of columns in the result
  PARAMETERS:
    res - pointer to the result structure

  RETURN: the number of columns
*/

uint32_t STDCALL mysqlx_column_get_count(mysqlx_result_struct *res)
{
  SAFE_EXCEPTION_BEGIN(res, 0)
  return res->get_col_count();
  SAFE_EXCEPTION_END(res, 0)
}


/*
  Get column name
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: character string containing column name
*/

const char * STDCALL mysqlx_column_get_name(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->get_column(pos).m_label.c_str();
  SAFE_EXCEPTION_END(res, NULL)
}


/*
  Get column original name
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: character string containing column original name
*/

const char * STDCALL mysqlx_column_get_original_name(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->get_column(pos).m_name.c_str();
  SAFE_EXCEPTION_END(res, NULL)
}


/*
  Get column table name
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: character string containing column table name
*/

const char * STDCALL mysqlx_column_get_table(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->get_column(pos).m_table_label.c_str();
  SAFE_EXCEPTION_END(res, NULL)
}


/*
  Get column original table name
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: character string containing column original table name
*/

const char * STDCALL mysqlx_column_get_original_table(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->get_column(pos).m_table_name.c_str();
  SAFE_EXCEPTION_END(res, NULL)
}


/*
  Get column schema name
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: character string containing column schema name
*/

const char * STDCALL mysqlx_column_get_schema(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->get_column(pos).m_schema_name.c_str();
  SAFE_EXCEPTION_END(res, NULL)
}


/*
  Get column catalog name
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: character string containing column catalog name
*/

const char * STDCALL mysqlx_column_get_catalog(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, NULL)
  return res->get_column(pos).m_catalog.c_str();
  SAFE_EXCEPTION_END(res, NULL)
}


/*
  Get column type identifier
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: 16-bit unsigned int number with the column type identifier
*/

uint16_t STDCALL mysqlx_column_get_type(mysqlx_result_struct *res, uint32_t pos)
{
  SAFE_EXCEPTION_BEGIN(res, MYSQLX_TYPE_UNDEFINED)
  return get_type(res->get_column(pos));
  SAFE_EXCEPTION_END(res, MYSQLX_TYPE_UNDEFINED)
}


/*
  Get column collation number
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: 16-bit unsigned int number with the column collation number
*/

uint16_t STDCALL mysqlx_column_get_collation(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, MYSQLX_COLLATION_UNDEFINED)
  return res->get_column(pos).m_collation;
  SAFE_EXCEPTION_END(res, MYSQLX_COLLATION_UNDEFINED)
}


/*
  Get column length
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: 32-bit unsigned int number indicating the maximum data length
*/

uint32_t STDCALL mysqlx_column_get_length(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, 0)
  return res->get_column(pos).m_length;
  SAFE_EXCEPTION_END(res, 0)
}


/*
  Get column precision
  PARAMETERS:
    res - pointer to the result structure
    pos - zero-based column number

  RETURN: 16-bit unsigned int number of digits after the decimal point
*/

uint16_t STDCALL mysqlx_column_get_precision(
  mysqlx_result_struct *res, uint32_t pos
)
{
  SAFE_EXCEPTION_BEGIN(res, 0)
  return res->get_column(pos).m_decimals;
  SAFE_EXCEPTION_END(res, 0)
}


/*
  Get number of rows affected by the last operation
  PARAMETERS:
    res - pointer to the result structure

  RETURN: 64-bit unsigned int number containing the number of affected rows
*/

uint64_t STDCALL mysqlx_get_affected_count(mysqlx_result_struct *res)
{
  SAFE_EXCEPTION_BEGIN(res, 0)
  return res->get_affected_rows();
  SAFE_EXCEPTION_END(res, 0)
}


/*
  Free the objects such as statement, session options,
  error and result.
*/

void STDCALL mysqlx_free(void *objs)
{
  if (objs)
  {
    Mysqlx_diag_base *obj = (Mysqlx_diag_base*)objs;
    if (typeid(*obj) == typeid(mysqlx_stmt_struct))
    {
      mysqlx_stmt_struct *stmt = (mysqlx_stmt_struct*)obj;
      stmt->get_session().rm_stmt(stmt);
    }
    else if (typeid(*obj) == typeid(mysqlx_session_options_struct))
    {
      mysqlx_free_options((mysqlx_session_options_struct*)obj);
    }
    else if (typeid(*obj) == typeid(mysqlx_result_struct))
    {
      mysqlx_result_free((mysqlx_result_struct*)obj);
    }
    else if (typeid(*obj) == typeid(mysqlx_dyn_error_struct) ||
             typeid(*obj) == typeid(mysqlx_collection_options_struct))
    {
      delete obj;
    }
  }
}


int STDCALL mysqlx_next_result(mysqlx_result_struct *res)
{
  SAFE_EXCEPTION_BEGIN(res, RESULT_ERROR)
  return res->next_result() ? RESULT_OK : RESULT_NULL;
  SAFE_EXCEPTION_END(res, RESULT_ERROR)
}


/*
  Free the result explicitly, otherwise it will be done automatically
  when statement handler is destroyed
*/

void STDCALL mysqlx_result_free(mysqlx_result_struct *res)
{
  if (res && res->m_stmt)
  {
    auto *stmt = res->m_stmt;
    try {
      stmt->rm_result(res);
    }
    catch (...) {}
  }
}


/*
  Closing the session.
  This function must be called by the user to prevent memory leaks.
*/

void STDCALL mysqlx_session_close(mysqlx_session_struct *sess)
{
  if (sess)
  {
    try {
      delete sess;
    }
    catch (...)
    {
      // Ignore errors that might happen during session destruction.
    }
  }
}


int STDCALL
mysqlx_schema_create(mysqlx_session_struct *sess, const char *schema)
{
  SAFE_EXCEPTION_BEGIN(sess, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(schema, sess, MYSQLX_ERROR_MISSING_SCHEMA_NAME_MSG, RESULT_ERROR)
  sess->create_schema(schema);
  return RESULT_OK;
  SAFE_EXCEPTION_END(sess, RESULT_ERROR)
}


int STDCALL
mysqlx_schema_drop(mysqlx_session_struct *sess, const char *schema)
{
  SAFE_EXCEPTION_BEGIN(sess, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(schema, sess, MYSQLX_ERROR_MISSING_SCHEMA_NAME_MSG, RESULT_ERROR)
  sess->drop_schema(schema);
  return RESULT_OK;
  SAFE_EXCEPTION_END(sess, RESULT_ERROR)
}


int STDCALL
mysqlx_collection_create(mysqlx_schema_struct *schema, const char *collection)
{
  SAFE_EXCEPTION_BEGIN(schema, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(collection, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  schema->create_collection(collection, true);
  return RESULT_OK;
  SAFE_EXCEPTION_END(schema, RESULT_ERROR)
}

mysqlx_collection_options_t * STDCALL
mysqlx_collection_options_new()
{
  return new mysqlx_collection_options_struct();
}

template<mysqlx_collection_opt_enum OPT>
void
set_collection_opt(mysqlx_collection_options_t&, va_list&)
{
  throw Mysqlx_exception("Unexpected collection option");
}

template<>
void
set_collection_opt<MYSQLX_OPT_COLLECTION_REUSE>(
    mysqlx_collection_options_t &options,
    va_list &args)
{
  if(options.m_usage.test(mysqlx_collection_options_t::REUSE))
    throw Mysqlx_exception("Option reuse already set.");
  options.m_usage.set(mysqlx_collection_options_t::REUSE);
  options.m_reuse = va_arg(args, unsigned int);
}

template<>
void
set_collection_opt<MYSQLX_OPT_COLLECTION_VALIDATION>(
    mysqlx_collection_options_t &options,
    va_list &args)
{
  if(options.m_usage.test(mysqlx_collection_options_t::VALIDATION) ||
     options.m_usage.test(mysqlx_collection_options_t::VALIDATION_LEVEL) ||
     options.m_usage.test(mysqlx_collection_options_t::VALIDATION_SCHEMA))
    throw Mysqlx_exception("Collection validation already set.");

  options.m_usage.set(mysqlx_collection_options_t::VALIDATION);

  options.m_validation = va_arg(args, char*);
}

template <mysqlx_collection_validation_opt_enum OPT>
void
set_collection_validation_opt(mysqlx_collection_options_t&, va_list&)
{
  throw Mysqlx_exception("Unexpected collection validation option");
}



template <>
void
set_collection_validation_opt<MYSQLX_OPT_COLLECTION_VALIDATION_LEVEL>(
    mysqlx_collection_options_t &options, va_list &args)
{

  if(options.m_usage.test(mysqlx_collection_options_t::VALIDATION) ||
     options.m_usage.test(mysqlx_collection_options_t::VALIDATION_LEVEL))
    throw Mysqlx_exception("Validation level already set.");

  options.m_usage.set(mysqlx_collection_options_t::VALIDATION_LEVEL);


#define SCHEMA_VALIDATION_LEVEL_CASE(X,Y)\
  case MYSQLX_OPT_COLLECTION_VALIDATION_LEVEL_##X:\
    options.m_validation_level = #X;\
  break;

  switch (va_arg(args, int)) {
  COLLECTION_VALIDATION_LEVEL(SCHEMA_VALIDATION_LEVEL_CASE)
  }
}

template<>
void
set_collection_validation_opt<MYSQLX_OPT_COLLECTION_VALIDATION_SCHEMA>(
    mysqlx_collection_options_t &options, va_list &args)
{

  if(options.m_usage.test(mysqlx_collection_options_t::VALIDATION) ||
     options.m_usage.test(mysqlx_collection_options_t::VALIDATION_SCHEMA))
    throw Mysqlx_exception("Validation schema already set.");

  options.m_usage.set(mysqlx_collection_options_t::VALIDATION_SCHEMA);

  options.m_validation_schema = va_arg(args, const char*);
}

int STDCALL
mysqlx_collection_options_set(mysqlx_collection_options_t * options,...)
{
  SAFE_EXCEPTION_BEGIN(options, RESULT_ERROR)
  va_list args;
  mysqlx_collection_options_t tmp_options(*options);
  int type;
  try {

    va_start(args, options);


    while (0 != (type = va_arg(args, int)))
    {
      switch (type)
      {
#define COLLECTION_OPTIONS_OPTION_SET(X,Y)\
      case MYSQLX_OPT_COLLECTION_##X:\
        set_collection_opt<MYSQLX_OPT_COLLECTION_##X>(tmp_options, args); \
      break;

      COLLECTION_OPTIONS_OPTION(COLLECTION_OPTIONS_OPTION_SET)

#define COLLECTION_VALIDATION_OPTION_SET(X,Y)\
      case MYSQLX_OPT_COLLECTION_VALIDATION_##X:\
        set_collection_validation_opt<MYSQLX_OPT_COLLECTION_VALIDATION_##X>(tmp_options, args);\
      break;

      COLLECTION_VALIDATION_OPTION(COLLECTION_VALIDATION_OPTION_SET)
        default:
          throw Mysqlx_exception("Unrecognized option");
      }
    }

    va_end(args);

  }
  catch (...)
  {
    va_end(args);
    throw;
  }
  *options = tmp_options;
  return RESULT_OK;
  SAFE_EXCEPTION_END(options, RESULT_ERROR)
}


int STDCALL
mysqlx_collection_create_with_options(mysqlx_schema_t *schema,
                                      const char *collection,
                                      mysqlx_collection_options_t *options)
{
  SAFE_EXCEPTION_BEGIN(schema, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(collection, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  PARAM_NULL_CHECK(options, schema, MYSQLX_ERROR_MISSING_COLLECTION_OPT_MSG, RESULT_ERROR)

  if( options->m_usage.test(mysqlx_collection_options_t::VALIDATION) )
  {
    schema->create_collection(collection,
                              options->m_reuse,
                              options->m_validation);
  }
  else
  {
    schema->create_collection(collection,
                              options->m_reuse,
                              options->m_validation_level,
                              options->m_validation_schema);
  }

  return RESULT_OK;
  SAFE_EXCEPTION_END(schema, RESULT_ERROR)
}

int STDCALL
mysqlx_collection_create_with_json_options(mysqlx_schema_t *schema,
                                           const char *collection,
                                           const char* json_options)
{
  SAFE_EXCEPTION_BEGIN(schema, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(collection, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(json_options, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  schema->create_collection(collection,
                            std::string(json_options));
  return RESULT_OK;
  SAFE_EXCEPTION_END(schema, RESULT_ERROR)
}

int STDCALL
mysqlx_collection_modify_with_options(mysqlx_schema_t *schema,
                                      const char *collection,
                                      mysqlx_collection_options_t *options)
{
  SAFE_EXCEPTION_BEGIN(schema, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(collection, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  PARAM_NULL_CHECK(options, schema, MYSQLX_ERROR_MISSING_COLLECTION_OPT_MSG, RESULT_ERROR)

  if(options->m_reuse)
    throw_error("Can't use OPT_COLLECTION_REUSE mysqlx_collection_modify_with_options");

  if(options->m_validation.empty())
  {
    schema->modify_collection(collection,
                              options->m_validation_level,
                              options->m_validation_schema);
  }
  else
  {
    schema->modify_collection(collection,
                              options->m_validation, true);
  }
  return RESULT_OK;
  SAFE_EXCEPTION_END(schema, RESULT_ERROR)
}

int STDCALL
mysqlx_collection_modify_with_json_options(mysqlx_schema_t *schema,
                                           const char* collection,
                                           const char* json_options)
{
  SAFE_EXCEPTION_BEGIN(schema, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(collection, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(json_options, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  schema->modify_collection(collection,
                            std::string(json_options));
  return RESULT_OK;
  SAFE_EXCEPTION_END(schema, RESULT_ERROR)
}


int STDCALL
mysqlx_collection_drop(mysqlx_schema_struct *schema, const char *collection)
{
  SAFE_EXCEPTION_BEGIN(schema, RESULT_ERROR)
  PARAM_NULL_EMPTY_CHECK(collection, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)
  schema->drop_collection(collection);
  return RESULT_OK;
  SAFE_EXCEPTION_END(schema, RESULT_ERROR)
}


/*
  STMT will be unavailable outside the function, set session error
  to the upper level object and return
*/
#define SET_ERROR_FROM_STMT(OBJ, STMT, R) do { \
   mysqlx_error_struct *err = STMT->get_error(); \
   if (err) \
      OBJ->set_diagnostic(err->message(), err->error_num()); \
    else \
      OBJ->set_diagnostic("Unknown error!", 0); \
    return R; \
  } while (0)


mysqlx_result_struct * STDCALL mysqlx_sql(mysqlx_session_struct *sess,
                                        const char *query,
                                        size_t query_len)
{
  SAFE_EXCEPTION_BEGIN(sess, NULL)

  mysqlx_stmt_struct *stmt = sess->sql_query(query, query_len);
  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(sess, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(sess, NULL)
}


mysqlx_result_struct * STDCALL mysqlx_sql_param(mysqlx_session_struct *sess,
                                        const char *query,
                                        size_t query_len, ...)
{
  SAFE_EXCEPTION_BEGIN(sess, NULL)
  int rc = RESULT_OK;
  mysqlx_stmt_struct *stmt;

  if ((stmt = sess->sql_query(query, query_len)) == NULL)
    return NULL;

  va_list args;
  va_start(args, query_len);
  rc = stmt->sql_bind(args);
  va_end(args);

  if (rc != RESULT_OK)
    SET_ERROR_FROM_STMT(sess, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(sess, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(sess, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_table_select(mysqlx_table_struct *table, const char *criteria)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  mysqlx_stmt_struct *stmt;

  if ((stmt = mysqlx_table_select_new(table)) == NULL)
    return NULL;

  if (RESULT_OK != stmt->set_where(criteria))
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_table_select_limit(mysqlx_table_struct *table, const char *criteria,
                               uint64_t row_count, uint64_t offset, ...)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  mysqlx_stmt_struct *stmt;
  int rc = RESULT_OK;

  if ((stmt = mysqlx_table_select_new(table)) == NULL)
    return NULL;

  if (RESULT_OK != stmt->set_where(criteria))
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  if (RESULT_OK != stmt->set_limit(row_count, offset))
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  va_list args;
  va_start(args, offset);
  rc= stmt->add_order_by(args);
  va_end(args);

  if (rc != RESULT_OK)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_table_insert(mysqlx_table_struct *table, ...)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  mysqlx_stmt_struct *stmt;
  int rc = RESULT_OK;

  if ((stmt = mysqlx_table_insert_new(table)) == NULL)
    return NULL;

  va_list args;
  va_start(args, table);
  /*
    Parameters are triplets: <column name, value type, value>
  */
  rc= stmt->add_row(true, args);
  va_end(args);

  if (rc != RESULT_OK)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_table_update(mysqlx_table_struct *table,
                        const char *criteria,
                        ...)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  mysqlx_stmt_struct *stmt;
  int rc = RESULT_OK;

  if ((stmt = mysqlx_table_update_new(table)) == NULL)
    return NULL;

  if (RESULT_OK != stmt->set_where(criteria))
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  va_list args;
  va_start(args, criteria);
  /*
    Parameters are triplets: <column name, value type, value>
  */
  rc= stmt->add_table_update_values(args);
  va_end(args);

  if (rc != RESULT_OK)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(table, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_table_delete(mysqlx_table_struct *table, const char *criteria)
{
  SAFE_EXCEPTION_BEGIN(table, NULL)
  mysqlx_stmt_struct *stmt;

  if ((stmt = mysqlx_table_delete_new(table)) == NULL)
    return NULL;

  if (RESULT_OK != stmt->set_where(criteria))
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(table, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(table, NULL)
}


int STDCALL
mysqlx_table_count(mysqlx_table_t *table, uint64_t *count)
{
  SAFE_EXCEPTION_BEGIN(table, RESULT_ERROR)
  PARAM_NULL_CHECK(count, table, MYSQLX_ERROR_OUTPUT_VARIABLE_NULL, RESULT_ERROR);
  *count = table->count();
  return RESULT_OK;
  SAFE_EXCEPTION_END(table, RESULT_ERROR)
}


int STDCALL
mysqlx_collection_count(mysqlx_collection_t *collection, uint64_t *count)
{
  SAFE_EXCEPTION_BEGIN(collection, RESULT_ERROR)
  PARAM_NULL_CHECK(count, collection, MYSQLX_ERROR_OUTPUT_VARIABLE_NULL, RESULT_ERROR);
  *count = collection->count();
  return RESULT_OK;
  SAFE_EXCEPTION_END(collection, RESULT_ERROR)
}

mysqlx_result_struct * STDCALL
mysqlx_collection_find(mysqlx_collection_struct *collection, const char *criteria)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)

  if (!criteria)
    criteria = "true";

  mysqlx_stmt_struct *stmt;

  if ((stmt = mysqlx_collection_find_new(collection)) == NULL)
    return NULL;

  if (RESULT_OK != stmt->set_where(criteria))
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_collection_add(mysqlx_collection_struct *collection, ...)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  mysqlx_stmt_struct *stmt;
  int rc = RESULT_OK;

  if ((stmt = mysqlx_collection_add_new(collection)) == NULL)
    return NULL;

  va_list args;
  va_start(args, collection);
  rc= stmt->add_multiple_documents(args);
  va_end(args);

  if (rc != RESULT_OK)
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_result_struct * STDCALL
_mysqlx_collection_modify_exec(mysqlx_collection_struct *collection,
                               const char *criteria,
                               mysqlx_modify_op modify_op, va_list &args)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  mysqlx_stmt_struct *stmt;
  int rc = RESULT_OK;

  if ((stmt = mysqlx_collection_modify_new(collection)) == NULL)
    return NULL;

  if (!criteria)
    criteria = "true";

  if (RESULT_OK != stmt->set_where(criteria))
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  rc= stmt->add_coll_modify_values(args, modify_op);

  if (rc != RESULT_OK)
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_collection_modify_set(mysqlx_collection_struct *collection,
                             const char *criteria, ...)
{
  mysqlx_result_struct *res;
  va_list args;
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  va_start(args, criteria);
  res = _mysqlx_collection_modify_exec(collection, criteria, MODIFY_SET, args);
  va_end(args);
  return res;
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_collection_modify_unset(mysqlx_collection_struct *collection,
                               const char *criteria, ...)
{
  mysqlx_result_struct *res;
  va_list args;
  SAFE_EXCEPTION_BEGIN(collection, NULL)

  va_start(args, criteria);
  res = _mysqlx_collection_modify_exec(collection, criteria, MODIFY_UNSET, args);
  va_end(args);
  return res;
  SAFE_EXCEPTION_END(collection, NULL)
}

static mysqlx_result_t *
_mysqlx_collection_modify_patch(mysqlx_collection_t *collection,
                               const char *criteria, ...)
{
  mysqlx_result_t *res;
  va_list args;
  SAFE_EXCEPTION_BEGIN(collection, NULL)

  va_start(args, criteria);
  res = _mysqlx_collection_modify_exec(collection, criteria,
                                       MODIFY_MERGE_PATCH, args);
  va_end(args);
  return res;
  SAFE_EXCEPTION_END(collection, NULL)
}

mysqlx_result_t * STDCALL
mysqlx_collection_modify_patch(mysqlx_collection_t *collection,
                               const char *criteria,
                               const char *patch_spec)
{
  return _mysqlx_collection_modify_patch(collection, criteria,
                                         patch_spec);
}


mysqlx_result_struct * STDCALL
mysqlx_collection_remove(mysqlx_collection_struct *collection, const char*criteria)
{
  SAFE_EXCEPTION_BEGIN(collection, NULL)
  mysqlx_stmt_struct *stmt;

  if ((stmt = mysqlx_collection_remove_new(collection)) == NULL)
    return NULL;

  if (RESULT_OK != stmt->set_where(criteria))
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  mysqlx_result_struct *res = mysqlx_execute(stmt);
  if (res == NULL)
    SET_ERROR_FROM_STMT(collection, stmt, NULL);

  return res;
  SAFE_EXCEPTION_END(collection, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_get_tables(mysqlx_schema_struct *schema,
                  const char *table_pattern,
                  int show_views)
{
  SAFE_EXCEPTION_BEGIN(schema, NULL)
  return schema->get_tables(table_pattern, 0 != show_views);
  SAFE_EXCEPTION_END(schema, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_get_collections(mysqlx_schema_struct *schema,
                       const char *col_pattern)
{
  SAFE_EXCEPTION_BEGIN(schema, NULL)
  return schema->get_collections(col_pattern);
  SAFE_EXCEPTION_END(schema, NULL)
}


mysqlx_result_struct * STDCALL
mysqlx_get_schemas(mysqlx_session_struct *sess, const char *schema_pattern)
{
  SAFE_EXCEPTION_BEGIN(sess, NULL)
  return sess->get_schemas(schema_pattern);
  SAFE_EXCEPTION_END(sess, NULL)
}


unsigned int STDCALL
mysqlx_result_warning_count(mysqlx_result_struct *result)
{
  SAFE_EXCEPTION_BEGIN(result, 0)
  return (unsigned int)result->get_warning_count();
  SAFE_EXCEPTION_END(result, 0)
}


mysqlx_error_struct * STDCALL
mysqlx_result_next_warning(mysqlx_result_struct *result)
{
  SAFE_EXCEPTION_BEGIN(result, 0)
  return result->get_next_warning();
  SAFE_EXCEPTION_END(result, 0)
}


uint64_t STDCALL mysqlx_get_auto_increment_value(mysqlx_result_struct *res)
{
  SAFE_EXCEPTION_BEGIN(res, 0)
  return res->get_auto_increment();
  SAFE_EXCEPTION_END(res, 0)
}


int STDCALL mysqlx_transaction_begin(mysqlx_session_struct *sess)
{
  SAFE_EXCEPTION_BEGIN(sess, RESULT_ERROR)
  sess->transaction_begin();
  return RESULT_OK;
  SAFE_EXCEPTION_END(sess, RESULT_ERROR)
}


int STDCALL mysqlx_transaction_commit(mysqlx_session_struct *sess)
{
  SAFE_EXCEPTION_BEGIN(sess, RESULT_ERROR)
  sess->transaction_commit();
  return RESULT_OK;
  SAFE_EXCEPTION_END(sess, RESULT_ERROR)
}


int STDCALL mysqlx_transaction_rollback(mysqlx_session_struct *sess)
{
  SAFE_EXCEPTION_BEGIN(sess, RESULT_ERROR)
  sess->transaction_rollback(nullptr);
  return RESULT_OK;
  SAFE_EXCEPTION_END(sess, RESULT_ERROR)
}


const char* STDCALL
mysqlx_savepoint_set(mysqlx_session_t *sess, const char *name)
{
  SAFE_EXCEPTION_BEGIN(sess, NULL)
  return sess->savepoint_set(name);
  SAFE_EXCEPTION_END(sess, NULL)
}

int STDCALL
mysqlx_savepoint_release(mysqlx_session_t *sess, const char *name)
{
  SAFE_EXCEPTION_BEGIN(sess, RESULT_ERROR)
  sess->savepoint_remove(name);
  return RESULT_OK;
  SAFE_EXCEPTION_END(sess, RESULT_ERROR)
}

 int STDCALL
 mysqlx_rollback_to(mysqlx_session_t *sess, const char *name)
 {
   SAFE_EXCEPTION_BEGIN(sess, RESULT_ERROR)
   if (name == NULL || strlen(name) == 0)
   {
     sess->set_diagnostic("Invalid save point name", 0);
     return RESULT_ERROR;
   }
   sess->transaction_rollback(name);
   return RESULT_OK;
   SAFE_EXCEPTION_END(sess, RESULT_ERROR)
 }


 const char * STDCALL
mysqlx_fetch_generated_id(mysqlx_result_struct *result)
{
  SAFE_EXCEPTION_BEGIN(result, NULL)
  return result->get_next_generated_id();
  SAFE_EXCEPTION_END(result, NULL)
}


int STDCALL
mysqlx_session_valid(mysqlx_session_struct *sess)
{
  SAFE_EXCEPTION_BEGIN(sess, 0)
  return sess->is_valid();
  SAFE_EXCEPTION_END(sess, 0)
}


mysqlx_session_options_t * STDCALL
mysqlx_session_options_new()
{
  return new mysqlx_session_options_struct();
}


void STDCALL
mysqlx_free_options(mysqlx_session_options_t *opt)
{
  if (opt)
    delete opt;
}



/*
  NULL as value of a string option means remove this option from settings.
  But empty string is not valid.
*/

template<mysqlx_opt_type_enum OPT>
void check_option(const char *val)
{
  if (val && !*val)
    throw Mysqlx_exception("Invalid empty string as value of option ");
}


/*
  In case of HOST and SOCKET settings, which accumulate, it is not possible
  to remove them by passing NULL.

  TODO: Consider if these checks are not better done in Settings_impl
*/

template<>
void check_option<MYSQLX_OPT_HOST>(const char *val)
{
  if (!val || !*val)
    throw Mysqlx_exception(MYSQLX_ERROR_MISSING_HOST_NAME);
}


template<>
void check_option<MYSQLX_OPT_SOCKET>(const char *val)
{
  if (!val || !*val)
    throw Mysqlx_exception(MYSQLX_ERROR_MISSING_SOCKET_NAME);
}


/*
  Since USER setting is compulsory, we do not allow removing it, one can only
  overwrite with a new value.
*/

template<>
void check_option<MYSQLX_OPT_USER>(const char *val)
{
  if (!val || !*val)
    throw Mysqlx_exception("Empty user name");
}


/*
  It is OK to give empty string as a password.
*/

template<>
void check_option<MYSQLX_OPT_PWD>(const char *)
{}


int STDCALL
mysqlx_session_option_set(mysqlx_session_options_struct *opt, ...)
{
  // Clear diagnostic information
  opt->Mysqlx_diag::clear();

  SAFE_EXCEPTION_BEGIN(opt, RESULT_ERROR)
  va_list args;
  int type;
  uint64_t  uint_data = 0;
  const char *char_data = NULL;

  using Option = mysqlx_session_options_struct::Session_option_impl;
  using COption = mysqlx_session_options_struct::Client_option_impl;

  mysqlx_session_options_struct::Setter set(*opt);

  try {

    va_start(args, opt);

    while (0 != (type = va_arg(args, int)))
    {
      switch (type)
      {

#define OPT_SET_str(X,N) \
        case N: \
        { char_data = va_arg(args, char*); \
          check_option<MYSQLX_OPT_##X>(char_data); \
          if (!char_data) \
            set.key_val(Option::X)->scalar()->null(); \
          else \
            set.key_val(Option::X)->scalar()->str(char_data); }; \
        break;

#define OPT_SET_num(X,N) \
        case N: \
        { \
          uint_data = va_arg(args, unsigned); \
          set.key_val(Option::X)->scalar()->num(uint_data); }; \
        break;

#define OPT_SET_any(X,N)  OPT_SET_num(X,N)

#define OPT_SET_bool(X,N) \
        case N: \
        { \
          uint_data = va_arg(args, unsigned); \
          set.key_val(Option::X)->scalar()->num(uint_data); }; \
        break;

        SESSION_OPTION_LIST(OPT_SET)

#define CLIENT_OPT_SET_bool(X,N) \
            case -N: \
            { uint_data = va_arg(args, int); \
              set.key_val(COption::X)->scalar()->num(uint_data); }; \
            break;

#define CLIENT_OPT_SET_num(X,N) \
        case -N: \
        { uint_data = va_arg(args, uint64_t); \
          set.key_val(COption::X)->scalar()->num(uint_data); }; \
        break;

        CLIENT_OPTION_LIST(CLIENT_OPT_SET)

        default:
          throw Mysqlx_exception("Unrecognized option");
      }
    }

    va_end(args);

  }
  catch (...)
  {
    va_end(args);
    throw;
  }

  set.commit();

  return RESULT_OK;
  SAFE_EXCEPTION_END(opt, RESULT_ERROR)
}


#define CHECK_OUTPUT_BUF(V, T) V = va_arg(args, T); \
if (V == NULL) \
{ \
   opt->set_diagnostic(MYSQLX_ERROR_OUTPUT_BUFFER_NULL, 0); \
   rc = RESULT_ERROR; \
   break; \
}


/*
  TODO: This function needs to be able to return information about hosts and
        corresponding parameters in the muliple host configurations.
*/

int STDCALL
mysqlx_session_option_get(
  mysqlx_session_options_struct *opt,
  int type,
  ...
)
{
  using Option = mysqlx_session_options_struct::Session_option_impl;

  SAFE_EXCEPTION_BEGIN(opt, RESULT_ERROR)

  if (!opt->has_option(mysqlx_opt_type_enum(type)))
  {
    opt->set_diagnostic("Option ... is not set", 0);
    return RESULT_ERROR;
  }

  int rc = RESULT_OK;
  unsigned int *uint_data = 0;
  char *char_data = NULL;

  va_list args;
  va_start(args, type);

  // Note: assumes the same enum values as used in Settings_impl::Option

  switch(type)
  {

#define OPT_GET_str(X,N) \
  case N: \
    CHECK_OUTPUT_BUF(char_data, char*) \
    strcpy(char_data, opt->get(Option::X).get_string().c_str()); \
    break;

#define OPT_GET_num(X,N) \
  case N: \
    { CHECK_OUTPUT_BUF(uint_data, unsigned int*) \
      auto val = opt->get(Option::X).get_uint(); \
      ASSERT_NUM_LIMITS(unsigned, val); \
      *uint_data = (unsigned)val; }; break;

#define OPT_GET_any(X,N) OPT_GET_num(X,N)

#define OPT_GET_bool(X,N) OPT_GET_num(X,N)

    SESSION_OPTION_LIST(OPT_GET)
    default:
      opt->set_diagnostic("Invalid option value", 0);
      rc = RESULT_ERROR;
  }
  va_end(args);

  return rc;
  SAFE_EXCEPTION_END(opt, RESULT_ERROR)
}


mysqlx_schema_struct * STDCALL
mysqlx_get_schema(mysqlx_session_struct *sess, const char *schema_name,
                  unsigned int check)
{
  SAFE_EXCEPTION_BEGIN(sess, NULL)
  PARAM_NULL_EMPTY_CHECK(schema_name, sess, MYSQLX_ERROR_MISSING_SCHEMA_NAME_MSG, NULL)
  return sess->get_schema(schema_name, check > 0);
  SAFE_EXCEPTION_END(sess, NULL)
}


mysqlx_collection_struct * STDCALL
mysqlx_get_collection(mysqlx_schema_struct *schema, const char *col_name,
                      unsigned int check)
{
  SAFE_EXCEPTION_BEGIN(schema, NULL)
  PARAM_NULL_EMPTY_CHECK(col_name, schema, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, NULL)
  return schema->get_collection(col_name, check > 0);
  SAFE_EXCEPTION_END(schema, NULL)
}


mysqlx_table_struct * STDCALL
mysqlx_get_table(mysqlx_schema_struct *schema, const char *tab_name,
                      unsigned int check)
{
  SAFE_EXCEPTION_BEGIN(schema, NULL)
  PARAM_NULL_EMPTY_CHECK(tab_name, schema, MYSQLX_ERROR_MISSING_TABLE_NAME_MSG, NULL)
  return schema->get_table(tab_name, check > 0);
  SAFE_EXCEPTION_END(schema, NULL)
}


mysqlx_error_struct * STDCALL
mysqlx_error(void *obj)
{
  Mysqlx_diag_base *diag = (Mysqlx_diag_base*)obj;
  SAFE_EXCEPTION_BEGIN(diag, NULL)
  return diag->get_error();
  SAFE_EXCEPTION_SILENT_END(NULL)
}


const char * STDCALL
mysqlx_error_message(void *obj)
{
  if (!obj)
    return nullptr;

  mysqlx_error_struct *error = mysqlx_error(obj);
  if (error)
  {
    const char *c = error->message();
    return c;
  }

  return nullptr;
}


unsigned int STDCALL mysqlx_error_num(void *obj)
{
  mysqlx_error_struct *error = mysqlx_error(obj);
  if (error)
    return error->error_num();

  return 0;
}


int STDCALL
mysqlx_collection_create_index(
  mysqlx_collection_struct *coll, const char *name, const char *idx_json
)
{
  SAFE_EXCEPTION_BEGIN(coll, RESULT_ERROR)
    PARAM_NULL_EMPTY_CHECK(name, coll, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)

  coll->create_index(name, idx_json);
  return RESULT_OK;
  SAFE_EXCEPTION_END(coll, RESULT_ERROR)
}

int STDCALL
mysqlx_collection_drop_index(
  mysqlx_collection_struct *coll, const char *name
)
{
  SAFE_EXCEPTION_BEGIN(coll, RESULT_ERROR)
    PARAM_NULL_EMPTY_CHECK(name, coll, MYSQLX_ERROR_MISSING_COLLECTION_NAME_MSG, RESULT_ERROR)

  coll->drop_index(name);
  return RESULT_OK;
  SAFE_EXCEPTION_END(coll, RESULT_ERROR)
}

#ifdef _WIN32
BOOL WINAPI DllMain(
  _In_ HINSTANCE,
  _In_ DWORD,
  _In_ LPVOID
)
{
  return true;
}
#endif

// TODO: add implementations for other mysqlx_xxxxxx functions
