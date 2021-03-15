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

#ifndef MYSQLX_COMMON_OP_IF_H
#define MYSQLX_COMMON_OP_IF_H

/*
  This file defines a hierarchy of abstract interfaces for objects that
  represent database operations. The base interface is the Executable_if for
  any operation that can be executed. Other interfaces in the hierarchy allow
  specifying more details of the operation.

  Note: Header op_impl.h defines implementations of these interfaces used
  in the connector.
*/

#include "api.h"
#include "../common_constants.h"
#include <string>


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)
namespace common {

class Result_init;
class Value;

#define LOCK_MODE(X,N)  X = N,
enum Lock_mode
{
  LOCK_MODE_LIST(LOCK_MODE)
};

#define LOCK_CONTENTION(X,N)  X = N,
enum class Lock_contention
{
  LOCK_CONTENTION_LIST(LOCK_CONTENTION)
};


/*
  Abstract interface for internal implementations of an executable object.

  The execute() method returns a Result_init object which can be
  used to construct a result instance.

  Implementation of an executable object holds a description of the operation
  that should be executed. Executable objects can be copied (for example
  by copy assignment operation) and in this case a new copy of the current
  description of the operation should be created by clone() method. After
  cloning, the 2 executable implementations can be modified and executed
  independently.

  See various Op_xxx classes defined for example in operation.h to see examples
  of executable object implementations. Note that these Op_xxx classes do
  not directly inherit from Executable_if. Instead they use a whole hierarchy
  of implementation classes based on Executable_if. But in the end, each
  implementation of an executable object defines the execute() method that
  executes given operation using all the information collected using other
  methods of the implementation class.
*/

struct Executable_if
{
  /*
    Execute the operation and return reference to object which implements
    Result_init interface. Such object is then used to construct a result
    instance.
  */

  virtual Result_init& execute() = 0;

  virtual Executable_if *clone() const = 0;

  virtual ~Executable_if() {}
};


/*
  The XXX_if classes defined below form a hierarchy of interfaces, based
  on Executable_if, for internal implementations of various crud operations.
  The concrete implementations, like Op_collection_find defined in
  operation.h, implements one of the top interfaces in this hierarchy but
  the hierarchy allows casting the implementation down to the layer
  implementing particular aspect of the operation. For example
  Limit_if interface allows setting limit and offset for returned/affected
  rows/documents, which is common for different CRUD operations.
*/


struct Bind_if : public Executable_if
{
  using string = std::string;

  // Add value for named parameter

  virtual void add_param(const string&, const Value&) = 0;

  // Add value for positional parameter

  virtual void add_param(Value) = 0;

  virtual void clear_params() = 0;
};


struct Limit_if : public Bind_if
{
  virtual void set_offset(unsigned) = 0;
  virtual void clear_offset() = 0;

  virtual void set_limit(unsigned) = 0;
  virtual void clear_limit() = 0;
};


struct Sort_if : public Limit_if
{
  using string = std::string;
  enum direction_t { ASC, DESC };

  virtual void add_sort(const string &expr, direction_t dir) = 0;
  virtual void add_sort(const string&) = 0;
  virtual void clear_sort() = 0;
};


struct Having_if : public Sort_if
{
  using string = std::string;

  virtual void set_having(const string&) = 0;
  virtual void clear_having() = 0;
};


struct Group_by_if : public Having_if
{
  using string = std::string;

  virtual void add_group_by(const string&) = 0;
  virtual void clear_group_by() = 0;
};


struct Proj_if : public Group_by_if
{
  using string = std::string;

  /*
    Add projection specification for a table query. It is an expression with
    optional "AS <alias>" suffix.
  */

  virtual void add_proj(const string&) = 0;

  /*
    Set projection for a document query. It is a JSON-like string but document
    field values are interpreted as expressions.
  */

  virtual void set_proj(const string&) = 0;

  virtual void clear_proj() = 0;
};


template <class Base>
struct Select_if : public Base
{
  using string = std::string;

  // Set expression to select rows/documents.

  virtual void set_where(const string&) = 0;

  // Define lock mode for rows/documents returned by the query.

  virtual void set_lock_mode(Lock_mode, Lock_contention) = 0;
  virtual void clear_lock_mode() = 0;
};


// --------------------------------------------------------------------------


struct Collection_find_if : public Select_if<Proj_if>
{};


/*
  Interface to internal implementations of CRUD add operation.
*/

struct Collection_add_if : public Executable_if
{
  /*
    Note: Current implementation only supports sending
    documents in form of UTF8 JSON strings.
  */

  virtual void add_json(const std::string&) = 0;
  virtual void clear_docs() = 0;
};


struct Collection_remove_if : public Select_if<Sort_if>
{};


/*
  Interface to internal implementations of CRUD modify operation.

  Methods `add_operation` are used to pass to the implementation object
  the modifications requested by the user.
*/

struct Collection_modify_if : public Select_if<Sort_if>
{
  using string = std::string;

  enum Operation
  {
    SET,
    UNSET,
    ARRAY_INSERT,
    ARRAY_APPEND,
    ARRAY_DELETE,
    MERGE_PATCH
  };

  virtual void add_operation(Operation, const string&, const Value&) = 0;
  virtual void add_operation(Operation, const string&) = 0;
  virtual void clear_modifications() = 0;
};


// --------------------------------------------------------------------------


/*
  Interface to be implemented by internal implementations of
  table insert operation.
*/

template <class Row_impl>
struct Table_insert_if : public Executable_if
{
  using string = std::string;

  /*
    Pass to the implementation names of columns specified by
    the user. Columns are passed one-by-one in the order in
    which they were specified.
  */

  virtual void add_column(const string&) = 0;
  virtual void clear_columns() = 0;

  /*
    Pass to the implementation a row that should be inserted
    into the table. Several rows can be passed.

    TODO: use move semantics instead
  */

  virtual void add_row(const Row_impl&) = 0;
  virtual void clear_rows() = 0;
};


/*
  Interface to be implemented by internal implementations
  of table CRUD select operation.

  Method `add_where` is used to report selection criteria
  to the implementation.
*/

struct Table_select_if : public Select_if<Proj_if>
{};


/*
  Interface to be implemented by internal implementations
  of table CRUD remove operation.

  Selection criteria which selects rows to be removed is
  passed to the implementation using `set_where` method.

  Note: setting where condition to empty string removes it.
*/

struct Table_remove_if : public Select_if<Sort_if>
{};


/*
  Interface to be implemented by internal implementations of
  table CRUD update operation. Such update operation sets values
  of fields in a row. Name of the column that should be set and
  expression defining new value are reported to the implementation
  using method `add_set`.
*/

struct Table_update_if : public Table_remove_if
{
  using string = std::string;

  virtual void add_set(const string&, const Value&) = 0;
  virtual void clear_modifications() = 0;
};

}  // internal
MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif
