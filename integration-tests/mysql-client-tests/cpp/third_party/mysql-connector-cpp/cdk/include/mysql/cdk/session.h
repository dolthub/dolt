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

#ifndef CDK_SESSION_H
#define CDK_SESSION_H

#include "api/session.h"
#include "api/transaction.h"
#include "data_source.h"
#include "reply.h"
#include "common.h"


namespace cdk {


/*
  Session class
  =============
*/


class Session
    : public api::Session
    , public api::Transaction<Traits>
{

protected:
  mysqlx::Session      *m_session;
  const mysqlx::string *m_database;
  api::Connection      *m_connection;

  typedef Reply::Initializer Reply_init;

public:

  typedef api::Session::Diagnostics Diagnostics;

  /// Create session to a data store represented by `ds` object.

  Session(ds::TCPIP &ds,
          const ds::TCPIP::Options &options = ds::TCPIP::Options());

  Session(ds::Multi_source&);

#ifndef _WIN32
  Session(ds::Unix_socket &ds,
          const ds::Unix_socket::Options &options = ds::Unix_socket::Options());
#endif //_WIN32

  ~Session();

  // Core Session operations.

  option_t is_valid() { return m_session->is_valid(); }
  option_t check_valid() { return m_session->check_valid(); }

  option_t has_prepared_statements() {
    return m_session->has_prepared_statements();
  }

  void set_has_prepared_statements(bool x) {
    return m_session->set_has_prepared_statements(x);
  }

  void reset() {
    m_session->reset();
  }

  void close() {
    m_session->close();
    m_connection->close();
  }

  /*
    Transactions
    ------------
  */

  /*
    Start new transaction.

    There can be only one open transaction in the session. If
    a transaction is open then begin() throws error.
  */

  void begin() {
    m_session->begin();
  }

  /*
    Commit open transaction.

    After commiting transaction is closed and another one can
    be started with begin(). Does nothing if no transaction
    is open.
  */

  void commit() {
      m_session->commit();
  }

  /*
    Rollback open transaction.

    After rolling back, the transaction is closed and another one
    can be started with begin(). Does nothing if no transaction is
    open.
  */

  void rollback(const string &savepoint = string()) {
      m_session->rollback(savepoint);
  }

  /*
    SavePoints are created inside transaction! And later, you can rollback the
    transaction to a specific SavePoint.
  */

  void savepoint_set(const string &savepoint)
  {
    if (savepoint.empty())
      throw_error(cdkerrc::bad_savepoint, "Invalid (empty) savepoint name");
    m_session->savepoint_set(savepoint);
  }

  /*
     Simply removes previously added SavePoint. No changes to transaction.
  */
  void savepoint_remove(const string &savepoint)
  {
    if (savepoint.empty())
      throw_error(cdkerrc::bad_savepoint, "Invalid (empty) savepoint name");
    m_session->savepoint_remove(savepoint);
  }

  /*
    Diagnostics
    -----------
    Interface for accessing error/warning/info entries stored in
    a given session object. Note that errors realeated to query
    execution are normally accessed via Reply object. These methods
    access diagnostics which is related to the session itself, not
    the individual queries.
  */

  unsigned int entry_count(Severity::value level=Severity::ERROR)
  {
    return m_session->entry_count(level);
  }

  Iterator& get_entries(Severity::value level=Severity::ERROR)
  {
    return m_session->get_entries(level);
  }

  const Error& get_error()
  {
    return m_session->get_error();
  }

  void clear_errors() { return m_session->clear_errors(); }

  /*
    Data manipulation
    -----------------
    Methods which manipulate data in the data store return vlaue
    of type Reply_init that is used to initialize a Reply instance.
    A Reply object is then used to examine server's reply to a given
    operation, including retreiving and processing result sets
    if any.

    If stmt_id = 0 the statement is executed directly. Otherwise it is first
    prepared (under the given id) and then executed. In that case it can be
    re-executed later using the prepared statement.
  */

  // Statements

  /**
    Execute an SQL query.

    If query contins "?" placeholders, values of these are given by
    `args` list.
  */

  Reply_init sql(uint32_t stmt_id,const string &query, Any_list *args =nullptr)
  {
    return m_session->sql(stmt_id, query, args);
  }

  /**
    Execute xplugin admin command.

    Execute admin command `cmd` with arguments `args`. For example,
    xplugin implements admin command "create_collection" whit 2 arguments:
    name of the collection and name of the schema in which to create.

    TODO: Find documentation for supported admin commands.
  */

  Reply_init admin(const char *cmd, const cdk::Any::Document &args)
  {
    return m_session->admin(cmd, args);
  }

  /*
    CRUD operations
    ---------------
    CRUD operations come in two flavours: operations on collections of
    documents and operations on tables. If an operation returns data,
    this data is in the form of one or more sets of rows (which can be
    processed using a Cursor object).

    Different operations use these common arguments:

    Order_by *order_by -- optional specification of how to order results,
    Limit    *limit    -- optional specification of how to limit the number
                          of returned rows.
    Param_source *param -- optional object which specifies values for named
                          parameters used in expressions that are passed to
                          the operation (such as selection criteria).
  */

  // CRUD for Collections
  // --------------------


  /**
    Add documents to a collection.

    Documents to be inserted are given by a Doc_source object which is
    a sequence of expressions, each expression describing a single document.
    Note that a document can be represented as a JSON blob or as a structured
    document expression. In the latter case this expression can contain named
    parameters -- the values for these parameters are given by the `param`
    argument describing a key-value dictionary.

    If `upsert` flag is set and a document being added has the same id as
    a document already present in the collection, the existing document is
    replaced by the new one. Otherwise, if `upsert` flag is false (the default)
    an error is reported if a document being added conflicts with an exisiting
    document in the collection.

    Note: Server requires that inserted documents contain "_id" field with
    unique document id.
  */

  Reply_init coll_add(const api::Object_ref &coll,
                      Doc_source &docs,
                      const Param_source *param,
                      bool upsert = false)
  {
    return m_session->coll_add(coll, docs, param, upsert);
  }

  /**
    Remove documents from a collection.

    Remove documents which match condition given by expression `expr` (all
    documents if `expr` is nullptr). The range of removed documents can be
    limited by Limit/Order_by specifications.
  */

  Reply_init coll_remove(uint32_t stmt_id,
                         const api::Object_ref &coll,
                         const Expression *expr = nullptr,
                         const Order_by *order_by = nullptr,
                         const Limit *lim = nullptr,
                         const Param_source *param = nullptr)
  {
    return m_session->coll_remove(stmt_id, coll, expr, order_by, lim, param);
  }

  /**
    Find documents in a collection.

    Return all documents which match selection criteria given by expression
    `expr` (all documents if `expr` is nullptr). Documents are returned as rows
    with 2 columns

    - column `doc` of type JSON containing the document,
    - column `_id` of type CHAR(N) containing document's id.

    Note: Docuemnt id is also included in the document as a field with
    name "_id".

    Selected documents can be projected to a different document given by
    document expression `proj`. This document expression describes a document
    in which values of fields are given by expressions that can use fields
    extracted from the source document. This way the source doucment can be
    transformed into a document with different structure. If `proj` is nullptr
    then documents are returned as-is.
  */

  Reply_init coll_find(uint32_t stmt_id,
                       const api::Object_ref &coll,
                       const View_spec *view = nullptr,
                       const Expression *expr = nullptr,
                       const Expression::Document *proj = nullptr,
                       const Order_by *order_by = nullptr,
                       const Expr_list *group_by = nullptr,
                       const Expression *having = nullptr,
                       const Limit *lim = nullptr,
                       const Param_source *param = nullptr,
                       const Lock_mode_value lock_mode = Lock_mode_value::NONE,
                       const Lock_contention_value lock_contention = Lock_contention_value::DEFAULT
                       )
  {
    return m_session->coll_find(stmt_id,coll, view, expr, proj,
                                order_by,group_by, having, lim, param,
                                lock_mode, lock_contention);
  }

  /**
    Update documents in a collection.

    Update documents that match given expression (all documents if `expr` is
    nullptr) according to specification given by `us`. The range of updated
    documents can be limited by Limit/Order_by specifications.

    @see `Update_processor` for information how to specify updates that should
    be applied to each document in the collection.
  */

  Reply_init coll_update(uint32_t stmt_id,
                         const api::Object_ref &table,
                         const Expression *expr,
                         const Update_spec &us,
                         const Order_by *order_by = nullptr,
                         const Limit *lim = nullptr,
                         const Param_source *param = nullptr)
  {
    return m_session->coll_update(stmt_id,
                                  table, expr, us, order_by, lim, param);
  }

  // Table CRUD
  // ----------

  /**
    Select rows from a table.

    Select rows which satisfy criteria given by expression `expr` (or all rows
    if `expr` is nullptr).

    Returned rows can be transformed as specified by `proj` argument.
    Projection specification is a list of expressions, each possibly with an
    alias. Expressions give the values of columns in the resulting row. These
    values can depend on values of fields in the source row.

    @see `api::Projection_processor`
  */

  Reply_init table_select(uint32_t stmt_id,
                          const api::Table_ref &tab,
                          const View_spec *view = nullptr,
                          const Expression *expr = nullptr,
                          const Projection *proj = nullptr,
                          const Order_by *order_by = nullptr,
                          const Expr_list *group_by = nullptr,
                          const Expression *having = nullptr,
                          const Limit* lim = nullptr,
                          const Param_source *param = nullptr,
                          const Lock_mode_value lock_mode = Lock_mode_value::NONE,
                          const Lock_contention_value lock_contention = Lock_contention_value::DEFAULT)
  {
    return m_session->table_select(stmt_id,
                                   tab, view, expr, proj, order_by,
                                   group_by, having, lim, param,
                                   lock_mode, lock_contention);
  }

  /**
    Insert rows into a table.

    Insert rows given by a Row_source object. A Row_source object is a sequence
    of rows where each row is described by a list of expressions, one
    expression per one column in the row.
  */

  Reply_init table_insert(uint32_t stmt_id,
                          const api::Table_ref &tab,
                          Row_source &rows,
                          const api::Columns *cols,
                          const Param_source *param)
  {
    return m_session->table_insert(stmt_id,
                                   tab, rows, cols, param);
  }

  /**
    Delete rows from a table.

    Delete rows which match condition given by expression `expr`. If `expr`
    is nullptr, deletes all rows in the table. The range of removed rows
    can be limited by Limit/Order_by specifications.
  */

  Reply_init table_delete(uint32_t stmt_id,
                          const api::Table_ref &tab,
                          const Expression *expr,
                          const Order_by *order_by,
                          const Limit* lim = nullptr,
                          const Param_source *param = nullptr)
  {
    return m_session->table_delete(stmt_id, tab, expr, order_by, lim, param);
  }


  /**
    Update rows in a table.

    Update rows that match given expression (all rows if `expr` is nullptr)
    according to specification given by `us`. The range of updated rows
    can be limited by Limit/Order_by specifications.


    @see `Update_processor` for information how to specify updates that should
    be applied to each row.
  */

  Reply_init table_update(uint32_t stmt_id,
                          const api::Table_ref &tab,
                          const Expression *expr,
                          const Update_spec &us,
                          const Order_by *order_by,
                          const Limit *lim = nullptr,
                          const Param_source *param = nullptr)
  {
    return m_session->table_update(stmt_id,
                                   tab, expr, us, order_by, lim, param);
  }


  // Views
  // -----

  Reply_init view_drop(const api::Table_ref &view, bool check_existence = false)
  {
    return m_session->view_drop(view, check_existence);
  }


  // Prepared Statments methods
  // --------------------------

  Reply_init prepared_execute(
    uint32_t stmt_id, const Limit* lim, const Param_source *param = nullptr
  )
  {
    return m_session->prepared_execute(stmt_id, lim, param);
  }

  Reply_init prepared_execute(
    uint32_t stmt_id, const cdk::Any_list *list = nullptr
  )
  {
    return m_session->prepared_execute(stmt_id, list);
  }

  Reply_init prepared_deallocate(uint32_t stmt_id)
  {
    return m_session->prepared_deallocate(stmt_id);
  }


  // Async_op interface

public:

  bool is_completed() const { return m_session->is_completed(); }

  /*
    Reports default schema
    returns nullptr if not defined
  */
  const mysqlx::string *get_default_schema()
  {
    return m_database;
  }

  /*
    Note: This does not work correctly yet, because xplugin is not
    correctly reporting current schema changes.
  */

  const string& current_schema() const
  {
    return m_session->get_current_schema();
  }

private:

  bool do_cont() { return m_session->cont(); }
  void do_wait() { m_session->wait(); }
  void do_cancel() { THROW("not supported"); }
  const api::Event_info* get_event_info() const { return m_session->get_event_info(); }

};


} // cdk

#endif // CDK_SESSION_H
