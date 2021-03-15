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

#ifndef MYSQLX_CRUD_H
#define MYSQLX_CRUD_H

/**
  @file
  Common templates used to define CRUD operation classes.
*/


#include "common.h"
#include "detail/crud.h"


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

class Session;
class Collection;
class Table;

namespace internal {

/*
  Factory for constructing concrete implementations of various CRUD
  operations. All these implementations implement the base Executable_if
  interface.

  Note: The caller of mk_xxx() method takes ownership of the returned
  implementation object.
*/

struct PUBLIC_API Crud_factory
{
  using Impl = common::Executable_if;

  static Impl* mk_add(Collection &coll);
  static Impl* mk_remove(Collection &coll, const string &expr);
  static Impl* mk_find(Collection &coll);
  static Impl* mk_find(Collection &coll, const string &expr);
  static Impl* mk_modify(Collection &coll, const string &expr);

  static Impl* mk_insert(Table &tbl);
  static Impl* mk_select(Table &tbl);
  static Impl* mk_update(Table &tbl);
  static Impl* mk_remove(Table &tbl);

  static Impl* mk_sql(Session &sess, const string &sql);
};


}  // internal


/*
  Different CRUD operation classes derive from `Executable` which defines
  the `execute()` method that executes given operation. Derived classes
  define additional methods that can modify the operation before it gets
  executed.

  The hierarchy of classes reflects the grammar that defines the order in which
  fluent API calls can be done. It is built using templates, such as Offset<>
  below, which add one API call on top of base class which defines remaining
  API calls that can be called later. For example, type

     Limit< Offset< Executable<...> > >

  represents an operation for which first .limit() can be called, followed by
  .offset() and then finally .execute(). See classes like
  Collection_find_base in collection_crud.h for more examples.

  Each template assumes that its base class defines method 'get_impl()' which
  returns a pointer to the internal implementation object. It also assumes that
  this implementation is of appropriate type and can be casted to
  the appropriate interface type. For example Limit<> template assumes
  that the implementation type can be casted to Limit_if type.
*/


/**
  @brief The LockContention  enum defines constants for defining
  the row locking contention for `Set_lock::lockExclusive()`
  and `Set_lock::lockShared()` methods.
  @see https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html#innodb-locking-reads-nowait-skip-locked
*/

enum_class LockContention
{
#define DEVAPI_LOCK_CONTENTION_ENUM(X,N)  X = N,

  LOCK_CONTENTION_LIST(DEVAPI_LOCK_CONTENTION_ENUM)
};

namespace internal {

/**
  Template for defining fluent api for CRUD operations.
*/

template <class Base>
class Offset
  : public Base
{
  using Operation = Base;

public:

  /**
    Skip the given number of items (rows or documents) before starting
    to perform the operation.
  */

  Operation& offset(unsigned rows)
  {
    try {
      get_impl()->set_offset(rows);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Limit_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base>
class Limit
  : public Base
{
  using Operation = Base;

public:

  /**
    %Limit the operation to the given number of items (rows or documents).
  */

  Operation& limit(unsigned items)
  {
    try {
      get_impl()->set_limit(items);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Limit_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base>
class Sort
  : public Base
  , Sort_detail
{
  using Operation = Base;

public:

  /**
    Specify ordering of documents in a query results.

    Arguments are one or more strings of the form `"<expr> <dir>"` where
    `<expr>` gives the value to sort on and `<dir>` is a sorting direction
    `ASC` or `DESC`.
  */

  template <typename...Type>
  Operation& sort(Type... spec)
  {
    try {
      get_impl()->clear_sort();
      add_sort(get_impl(), spec...);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Sort_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base>
class Order_by
  : public Base
  , Sort_detail
{
  using Operation = Base;

public:

  /**
    Specify ordering of rows in a query results.

    Arguments are one or more strings of the form `"<expr> <dir>"` where
    `<expr>` gives the value to sort on and `<dir>` is a sorting direction
    `ASC` or `DESC`.
  */

  template <typename...Type>
  Operation& orderBy(Type... spec)
  {
    try {
      get_impl()->clear_sort();
      add_sort(get_impl(), spec...);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Sort_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base>
class Having
  : public Base
{
  using Operation = Base;

public:

  /**
    Specify filter over grouped results of a query.

    The argument is a Boolean expression which can use aggregation functions.
  */

  Operation& having(const string& having_spec)
  {
    try {
      get_impl()->set_having(having_spec);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Having_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base>
class Group_by
  : public Base
  , Group_by_detail
{
  using Operation = Base;

public:

  /**
    Specify grouping of items in a query result.

    Arguments are a one or more expressions. Documents/rows for which
    expressions evaluate to the same value are grouped together.
  */

  template <typename... Expr>
  Operation& groupBy(Expr... group_by_spec)
  {
    try {
      get_impl()->clear_group_by();
      do_group_by(get_impl(), group_by_spec...);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Group_by_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base>
class Bind_placeholders
  : public Base
  , Bind_detail
{
  using BindOperation = Bind_placeholders;

public:

  /**
    Specify values for '?' placeholders in a query.

    One or more values can be specified in a single call to bind(). A query
    can be executed only if values for all placeholders have been specified.
  */

  template <typename... Types>
  BindOperation& bind(Types&&... vals)
  {
    try {
      add_params(get_impl(), std::forward<Types>(vals)...);
      return *this;
    }
    CATCH_AND_WRAP
  }

protected:

  using Impl = common::Bind_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base>
class Bind_parameters
  : public Base
{
  using BindOperation = Bind_parameters;
  using Operation = Base;

public:

  /**
    Bind parameter with given name to the given value.

    A statement or query can be executed only if all named parameters used by
    it are bound to values.
  */

  BindOperation& bind(const string &parameter, const Value &val)
  {
    //TODO: Protocol supports Document and Array... but common::Values doesn't!
    if (Value::DOCUMENT == val.getType())
      throw_error("Can not bind a parameter to a document");

    if (Value::ARRAY == val.getType())
      throw_error("Can not bind a parameter to an array");

    try {
      get_impl()->add_param(parameter, (const common::Value&)val);
      return *this;
    }
    CATCH_AND_WRAP
  }

  /**
    Bind parameters to values given by a map from parameter
    names to their values.
  */

  template <class Map>
  Operation& bind(const Map &args)
  {
    for (const auto &keyval : args)
    {
      bind(keyval.first, keyval.second);
    }
    return *this;
  }

protected:

  using Impl = common::Bind_if;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


/// @copydoc Offset

template <class Base, class IMPL>
class Set_lock
  : public Base
{
  using Operation = Base;

public:

  /**
    Set a shared mode lock on any rows/documents that are read.

    Other sessions can read, but not modify locked rows/documents.
  */

  Operation&
  lockShared(LockContention contention= LockContention::DEFAULT)
  {
    get_impl()->set_lock_mode(common::Lock_mode::SHARED,
                              common::Lock_contention((unsigned)contention));
    return *this;
  }

  /**
    Set an exclusive mode lock on any rows/documents that are read.

    Other sessions are blocked from modifying, locking, or reading the data
    in certain transaction isolation levels. The lock is released
    when the transaction is committed or rolled back.
  */

  Operation&
  lockExclusive(LockContention contention = LockContention::DEFAULT)
  {
    get_impl()->set_lock_mode(common::Lock_mode::EXCLUSIVE,
                              common::Lock_contention((unsigned)contention));
    return *this;
  }

protected:

  using Impl = IMPL;

  Impl* get_impl()
  {
    return static_cast<Impl*>(Base::get_impl());
  }
};


}   // internal
MYSQLX_ABI_END(2,0)
} // mysqlx

#endif
