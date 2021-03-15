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

#ifndef MYSQLX_TABLE_CRUD_H
#define MYSQLX_TABLE_CRUD_H

/**
  @file
  Crud operations on tables.

  Classes declared here represent CRUD operations on a table. They are
  analogous to collection CRUD operation classes defined in collection_crud.h.

  The following classes for table CRUD operations are defined:
  - TableInsert
  - TableRemove
  - TableSelect
  - TableUpdate

  CRUD operation objects can be created directly, or assigned from
  result of DevAPI methods that create such operations:
  ~~~~~~
  TableInsert  insert_op(table);
  TableSelect  select_op = table.select(...).orderBy(...);
  ~~~~~~

  CRUD operation objects have methods which can modify the operation
  before it gets executed. For example `TableInsert::values()`
  appends a row to the list of rows that should be inserted into a table
  by the given TableInsert operation. These methods can be chained
  as allowed by the fluent API grammar.
*/


#include "common.h"
#include "result.h"
#include "executable.h"
#include "crud.h"


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

class Table;

// ---------------------------------------------------------------------------

class TableInsert;

namespace internal {

  struct Table_insert_base
    : public Executable<Result, TableInsert>
  {};

}

/**
  An operation which inserts rows into a table.

  This class defines methods that specify the rows to be inserted into
  the table.

  @todo Check that every row passed to .values() call has
  the same number of values. The column count should match
  the one in insert(c1,...) call. For insert() without column
  list, it should match the number of columns in the table.

  @ingroup devapi_op
*/

class TableInsert
  : public internal::Table_insert_base
  , internal::Table_insert_detail
{

protected:

  template <class... Cols>
  TableInsert(Table &table, const Cols&... cols)
    : TableInsert(table)
  {
    add_columns(get_impl(), cols...);
  }

public:

  // Create operation which inserts rows into given table.

  TableInsert(Table &table)
  {
    try {
      reset(internal::Crud_factory::mk_insert(table));
    }
    CATCH_AND_WRAP
  }

  TableInsert(const internal::Table_insert_base &other)
  {
    internal::Table_insert_base::operator=(other);
  }

  TableInsert(internal::Table_insert_base &&other)
  {
    internal::Table_insert_base::operator=(std::move(other));
  }

  using internal::Table_insert_base::operator=;

  /// Add the given row to the list of rows to be inserted.

  virtual TableInsert& values(const Row &row)
  {
    try {
      add_rows(get_impl(), row);
      return *this;
    }
    CATCH_AND_WRAP
  }

  /**
    Add a single row consisting of the specified values to the list of
    rows to be inserted.
  */

  template<typename... Types>
  TableInsert& values(Types... rest)
  {
    try {
      add_values(get_impl(), rest...);
      return *this;
    }
    CATCH_AND_WRAP
  }

  /**
    Add rows from a container such as vector or list.
  */

  template<typename Container>
  TableInsert& rows(const Container &cont)
  {
    try {
      add_rows(get_impl(), cont);
      return *this;
    }
    CATCH_AND_WRAP
  }

  /**
    Add rows from a range given by two iterators.
  */

  template<typename It>
  TableInsert& rows(const It &begin, const It &end)
  {
    try {
      add_rows(get_impl(), begin, end);
      return *this;
    }
    CATCH_AND_WRAP
  }

  /**
    Add the given list of rows.
  */

  template<typename... Types>
  TableInsert& rows(const Row &first, Types... rest)
  {
    try {
      add_rows(get_impl(), first, rest...);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Table_insert_detail::Impl;

  Impl* get_impl()
  {
    return static_cast<Impl*>(internal::Table_insert_base::get_impl());
  }

  ///@cond IGNORED
  friend Table;
  ///@endcond
};


// ---------------------------------------------------------------------------

class TableSelect;

namespace internal {

  class Op_view_create_alter;

  struct Table_select_cmd
    : public Executable<RowResult, TableSelect>
  {};

  struct Table_select_base
    : public Group_by < Having < Order_by < Limit < Offset< Bind_parameters<
        Set_lock< Table_select_cmd, common::Table_select_if >
      > > > > > >
  {};

}


/**
  An operation which selects rows from a table.

  The class defines various methods, such as `where()`, to specify which rows
  should be returned and in which order.

  For each row the operation can return all fields from the
  row or a set of values defined by projection expressions
  specified when the operation was created.

  @ingroup devapi_op
*/

class TableSelect
  : public internal::Table_select_base
  , internal::Table_select_detail
{

  using Operation = Table_select_base;

public:

  TableSelect(Table &table)
  {
    try {
      reset(internal::Crud_factory::mk_select(table));
    }
    CATCH_AND_WRAP
  }

  template <typename...PROJ>
  TableSelect(Table &table, const PROJ&... proj)
    : TableSelect(table)
  {
    try {
      add_proj(get_impl(), proj...);
    }
    CATCH_AND_WRAP
  }

  TableSelect(const internal::Table_select_cmd &other)
  {
    internal::Table_select_cmd::operator=(other);
  }

  TableSelect(internal::Table_select_cmd &&other)
  {
    internal::Table_select_cmd::operator=(std::move(other));
  }

  using internal::Table_select_cmd::operator=;

  /**
    Specify row selection criteria.

    The criteria is specified as a Boolean expression string.
  */

  Operation& where(const string& expr)
  {
    try {
      get_impl()->set_where(expr);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Table_select_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(internal::Table_select_base::get_impl());
  }

  ///@cond IGNORED
  friend Table;
  friend internal::Op_view_create_alter;
  ///@endcond
};


// ---------------------------------------------------------------------------

class TableUpdate;

namespace internal {

  struct Table_update_cmd
    : public Executable<Result, TableUpdate>
  {};

  struct Table_update_base
    : public Order_by< Limit< Bind_parameters< Table_update_cmd > > >
  {};

}


/**
  An operation which updates rows stored in a table.

  Methods of this clas specify modifications to be applied to each row as well
  as the set of rows that should be modified.

  @ingroup devapi_op
*/

class TableUpdate
  : public internal::Table_update_base
{
  using Operation = internal::Table_update_base;

  TableUpdate(Table& table)
  {
    try {
      reset(internal::Crud_factory::mk_update(table));
    }
    CATCH_AND_WRAP
  }

public:

  TableUpdate(Table &table, const string &expr)
    : TableUpdate(table)
  {
    where(expr);
  }

  TableUpdate(const internal::Table_update_cmd &other)
  {
    internal::Table_update_cmd::operator=(other);
  }

  TableUpdate(internal::Table_update_cmd &&other)
  {
    internal::Table_update_cmd::operator=(std::move(other));
  }

  using internal::Table_update_cmd::operator=;

  /**
    Set the given field in a row to the given value.

    The value can be either a direct literal or an expression given
    as `expr(<string>)`, to be evaluated in the server.
  */

  TableUpdate& set(const string& field, const Value &val)
  {
    try {
      get_impl()->add_set(field, (const common::Value&)val);
      return *this;
    }
    CATCH_AND_WRAP
  }

  /**
    Specify selection criteria for rows that should be updated.
  */

  Operation& where(const string& expr)
  {
    try {
      get_impl()->set_where(expr);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Table_update_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(internal::Table_update_base::get_impl());
  }

  ///@cond IGNORED
  friend Table;
  ///@endcond
};


// ---------------------------------------------------------------------------

class TableRemove;

namespace internal {

  struct Table_remove_cmd
    : public Executable<Result, TableRemove>
  {};

  struct Table_remove_base
    : Order_by< Limit< Bind_parameters< Table_remove_cmd > > >
  {};

}


/**
  An operation which removes rows from a table.

  The class defines methods to specify which rows should be removed.

  @ingroup devapi_op
*/

class TableRemove
  : public internal::Table_remove_base
{
  using Operation = internal::Table_remove_base;

  TableRemove(Table& table)
  {
    try {
      reset(internal::Crud_factory::mk_remove(table));
    }
    CATCH_AND_WRAP
  }

public:

  TableRemove(Table &table, const string &expr)
    : TableRemove(table)
  {
    where(expr);
  }

  TableRemove(const internal::Table_remove_cmd &other)
  {
    internal::Table_remove_cmd::operator=(other);
  }

  TableRemove(internal::Table_remove_cmd &&other)
  {
    internal::Table_remove_cmd::operator=(std::move(other));
  }

  using internal::Table_remove_cmd::operator=;

  /**
    Specify selection criteria for rows to be removed.
  */

  Operation& where(const string &expr)
  {
    try {
      get_impl()->set_where(expr);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Table_remove_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(internal::Table_remove_base::get_impl());
  }

  ///@cond IGNORED
  friend Table;
  ///@endcond
};


MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif
