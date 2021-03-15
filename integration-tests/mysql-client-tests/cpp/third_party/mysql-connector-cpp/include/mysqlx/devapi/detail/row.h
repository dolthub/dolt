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

#ifndef MYSQLX_DETAIL_ROW_H
#define MYSQLX_DETAIL_ROW_H

/**
  @file
  Details for Row class.
*/


#include "../common.h"
#include "../document.h"

#include <memory>


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

class Columns;

namespace internal {

template <class COLS> class Row_result_detail;
struct Table_insert_detail;


class PUBLIC_API Row_detail
{
protected:

  class INTERNAL Impl;
  DLL_WARNINGS_PUSH
  std::shared_ptr<Impl>  m_impl;
  DLL_WARNINGS_POP

  Row_detail() = default;

  Row_detail(std::shared_ptr<Impl> &&impl)
  {
    m_impl = std::move(impl);
  }

  col_count_t col_count() const;
  bytes       get_bytes(col_count_t) const;
  Value&      get_val(col_count_t);

  void clear()
  {
    m_impl.reset();
  }

  Impl& get_impl();

  const Impl& get_impl() const
  {
    return const_cast<Row_detail*>(this)->get_impl();
  }

  void ensure_impl();

  using Args_prc = Args_processor<Row_detail, std::pair<Impl*, col_count_t>*>;

  template<typename... Types>
  void set_values(col_count_t pos, Types... args)
  {
    ensure_impl();
    assert(m_impl);
    std::pair<Impl*, col_count_t> data{ m_impl.get(), pos };
    Args_prc::process_args(&data, args...);
  }

  static void process_one(std::pair<Impl*, col_count_t>*, const Value &val);

  friend Table_insert_detail;
  friend Row_result_detail<Columns>;
  friend Args_prc;
};


}  // internal namespace
MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif
