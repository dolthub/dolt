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

#ifndef MYSQLX_DETAIL_CRUD_H
#define MYSQLX_DETAIL_CRUD_H

/**
  @file
  Details for public API classes representing CRUD operations.
*/


#include "../common.h"
#include "../executable.h"


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

namespace internal {


struct PUBLIC_API Bind_detail
{
protected:

  using Impl = common::Bind_if;
  using Args_prc = Args_processor<Bind_detail, Impl*>;

  static void process_one(Impl *impl, const Value &val)
  {
    impl->add_param((const common::Value&)val);
  }

  template <typename... T>
  static void add_params(Impl *impl, T&&... vals)
  {
    Args_prc::process_args(impl, std::forward<T>(vals)...);
  }

  friend Args_prc;
};


struct PUBLIC_API Sort_detail
{
protected:

  using Impl = common::Sort_if;
  using Args_prc = Args_processor<Sort_detail, Impl*>;

  static void process_one(Impl *impl, const string &ord_spec)
  {
    impl->add_sort(ord_spec);
  }

  template <typename... T>
  static void add_sort(Impl *impl, T... args)
  {
    Args_prc::process_args(impl, args...);
  }

  friend Args_prc;
};


struct PUBLIC_API Group_by_detail
{
protected:

  using Impl = common::Group_by_if;
  using Args_prc = Args_processor<Group_by_detail, Impl*>;

  static void process_one(Impl *impl, const string &spec)
  {
    impl->add_group_by(spec);
  }

  template <typename... T>
  static void do_group_by(Impl *impl, T... args)
  {
    Args_prc::process_args(impl, args...);
  }

  friend Args_prc;
};


struct PUBLIC_API Proj_detail
{
protected:

  using Impl = common::Proj_if;
  using Args_prc = Args_processor<Proj_detail, Impl*>;

  static void process_one(Impl *impl, const string &spec)
  {
    impl->add_proj(spec);
  }

  template <typename... T>
  static void add_proj(Impl *impl, T... proj_spec)
  {
    Args_prc::process_args(impl, proj_spec...);
  }

  friend Args_prc;
};


struct PUBLIC_API Collection_add_detail
{
protected:

  using Impl = common::Collection_add_if;
  using Args_prc = Args_processor<Collection_add_detail, Impl*>;

  static void process_one(Impl *impl, const string &json)
  {
    impl->add_json(json);
  }

  static void process_one(Impl *impl, const DbDoc &doc)
  {
    // TODO: Do it better when we support sending structured
    // document descriptions to the server.

    std::ostringstream buf;
    buf << doc;
    // Note: utf8 conversion using mysqlx::string.
    impl->add_json(mysqlx::string(buf.str()));
  }

  template <typename... T>
  static void do_add(Impl *impl, T... args)
  {
    Args_prc::process_args(impl, args...);
  }

  friend Args_prc;
};


struct PUBLIC_API Collection_find_detail
{
protected:

  using Impl = common::Proj_if;
  using Args_prc = Args_processor<Collection_find_detail, Impl*>;

  static void process_one(Impl *impl, const string &proj)
  {
    impl->add_proj(proj);
  }


  static void do_fields(Impl *impl, const Expression &proj)
  {
    impl->set_proj(proj.get<string>());
  }

  /*
    Note: If e is an expression (of type Expression) then only
    .fields(e) is valid - the multi-argument variant .fields(e,...)
    should be disabled.
  */

  template <
    typename T, typename... R,
    typename std::enable_if<!std::is_same<T, Expression>::value>::type* = nullptr
  >
    static void do_fields(Impl *impl, T first, R... rest)
  {
    Args_prc::process_args(impl, first, rest...);
  }

  friend Args_prc;
};


struct PUBLIC_API Table_insert_detail
{
protected:

  using Row_impl = internal::Row_detail::Impl;
  using Impl = common::Table_insert_if<Row_impl>;

  /*
    Helper methods which pass column/row information to the
    internal implementation object.
  */

  struct Add_column
  {
    static void process_one(Impl *impl, const string &col)
    {
      impl->add_column(col);
    }
  };

  struct Add_value
  {
    using Impl = std::pair<Row, unsigned>;

    static void process_one(Impl *impl, const mysqlx::Value &val)
    {
      impl->first.set((impl->second)++, val);
    }
  };

  struct Add_row
  {
    static void process_one(Impl *impl, const Row &row)
    {
      impl->add_row(*row.m_impl);
    }
  };

  template <typename... T>
  static void add_columns(Impl *impl, T... args)
  {
    Args_processor<Add_column, Impl*>::process_args(impl, args...);
  }

  template <typename... T>
  static void add_rows(Impl *impl, T... args)
  {
    Args_processor<Add_row, Impl*>::process_args(impl, args...);
  }

  template <typename... T>
  static void add_values(Impl *impl, T... args)
  {
    Add_value::Impl row{ {}, 0 };
    Args_processor<Add_value>::process_args(&row, args...);
    Add_row::process_one(impl, row.first);
  }

  friend Args_processor<Add_column, Impl*>;
  friend Args_processor<Add_row, Impl*>;
  friend Args_processor<Add_value, Impl*>;

};


using Table_select_detail = Proj_detail;

}  // internal

MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif
