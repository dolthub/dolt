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

#ifndef MYSQLX_DETAIL_RESULT_H
#define MYSQLX_DETAIL_RESULT_H

/**
  @file
  Details for public API result classes.
*/


#include "../common.h"
#include "../error.h"
#include "../document.h"
#include "../row.h"
#include "../collations.h"

#include <deque>


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

class RowResult;
class Column;
class Columns;
class Session;

namespace common {

class Result_init;
class Column_info;
class Result_impl;

}  // common


namespace internal {


struct Session_detail;


class PUBLIC_API Result_detail
{
  // Disable copy semantics for result classes.

  Result_detail(const Result_detail&) = delete;
  Result_detail& operator=(const Result_detail&) = delete;

public:

  using Impl = common::Result_impl;

protected:

  Result_detail(common::Result_init&);

  // Note: move semantics is implemented by move assignment operator.

  Result_detail(Result_detail &&other)
  {
    operator=(std::move(other));
  }

  Result_detail& operator=(Result_detail&&);

  Result_detail() = default;
  virtual ~Result_detail();

  Impl& get_impl();

  const Impl& get_impl() const
  {
    return const_cast<Result_detail*>(this)->get_impl();
  }

  void check_result() const;

  uint64_t get_affected_rows() const;
  uint64_t get_auto_increment() const;

  using DocIdList = internal::List_initializer<const std::vector<std::string>&>;

  DocIdList get_generated_ids() const;

  // Handling multi-results

  bool has_data() const;

  // Note: needs to be called before accessing the first result set.
  bool next_result();

protected:

  Impl  *m_impl = nullptr;
  bool m_owns_impl = false;

  /*
    Source for WarningList initializer.
  */

  struct Warning_src
  {
    using Value = Warning;

    Result_detail &m_res;

    Warning_src(Result_detail &res)
      : m_res(res)
    {}

    size_t size() const
    {
      return m_res.get_warning_count();
    }

    Warning operator[](size_t pos)
    {
      return m_res.get_warning(pos);
    }
  };

public:

  using WarningList = internal::List_initializer<Array_source<Warning_src>>;

protected:

  unsigned    get_warning_count() const;
  Warning     get_warning(size_t pos);

  WarningList get_warnings()
  {
    get_warning_count();
    return { *this };
  }

public:

  friend Session_detail;
  friend List_initializer<Result_detail>;
};


/*
  This class keeps a reference to column information stored in a
  common::Column_info<> instance. The meta-data is exposed in format expected
  by X DevAPI meta-data access methods. In particualr the CDK type and encoding
  format information is translated to X DevAPI type information. For example,
  a CDK column of type FLOAT can be reported as DevAPI type FLOAT, DOUBLE
  or DECIMAL, depending on the encoding format that was reported by CDK. This
  translation happens in Column::getType() method.

  Additional encoding information is exposed via other methods such as
  is_signed().
*/

class PUBLIC_API Column_detail
  : virtual common::Printable
{
protected:

  using Impl = common::Column_info;

  const Impl *m_impl = nullptr;

  Column_detail(const Impl *impl)
    : m_impl(impl)
  {}

  const Impl& get_impl() const
  {
    assert(m_impl);
    return *m_impl;
  }

  string get_name() const;
  string get_label() const;
  string get_schema_name() const;
  string get_table_name() const;
  string get_table_label() const;

  // Note: should return values of mysqlx::Type enum constants

  unsigned get_type() const;

  CharacterSet get_charset() const;
  const CollationInfo& get_collation() const;

  unsigned long get_length() const;
  unsigned short get_decimals() const;

  bool is_signed() const;
  bool is_padded() const;

  void print(std::ostream&) const override;

protected:

  Column_detail() = default;
  Column_detail(const Column_detail&) = default;
  Column_detail(Column_detail&&) = default;

  Column_detail& operator=(const Column_detail&) = default;

public:

  friend Impl;
  friend Result_detail;
  friend RowResult;

  struct INTERNAL Access;
  friend Access;
};


/*
  A wrapper around column meta-data class COL that adds copy semantics
  and default ctor. This is required by Columns_detail class which uses
  an STL container to store data for several columns.
*/

template <class COL>
struct Column_storage
  : public COL
{
  Column_storage(const typename COL::Impl *impl)
    : COL(impl)
  {}

  // Note: these members are needed to use it with std::deque<>

  Column_storage() = default;
  Column_storage(const Column_storage&) = default;
  Column_storage& operator=(const Column_storage&) = default;
};


template <class COLS> class Row_result_detail;


/*
  Class holding meta-data information for all columns in a result.

  Template parameter COL is a class used to store information about a single
  column. It is made into template parameter because full definition of
  the actuall mysqlx::Column class is not available in this header.

  Note: Because this class is implemented using std::deque<>, we wrap COL
  class with the Column_storage<> wrapper to provide copy semantics and default
  ctor required by this STL container.
*/

template <class COL>
class Columns_detail
  : public std::deque<Column_storage<COL>>
{
  Columns_detail(const Columns_detail&) = delete;

protected:

  Columns_detail() = default;
  Columns_detail(Columns_detail&&) = default;
  Columns_detail& operator=(Columns_detail&&) = default;

  void init(const internal::Result_detail::Impl&);

  ///@cond IGNORE
  friend internal::Row_result_detail<Columns>;
  ///@endcond
};


/*
  COLS is a class used to store information about result columns. It is made
  into template parameter because the actual mysqlx::Columns class, with
  the public API for accessing column information, is defined in the top-level
  header devapi/result.h.

  The COLS class should have move semantics to enable move-semantics for result
  objects.
*/

template <class COLS>
class Row_result_detail
  : public Result_detail
{
public:

  using iterator = Iterator<Row_result_detail, Row>;
  using RowList = List_initializer<Row_result_detail&>;
  using Columns = COLS;

  iterator begin()
  {
    return iterator(*this);
  }

  iterator end() const
  {
    return iterator();
  }

private:

  // Row iterator implementation

  Row   m_row;

  using Value = Row;

  void iterator_start() {}

  bool iterator_next();

  Value iterator_get()
  {
    return m_row;
  }


protected:

  Row_result_detail() = default;
  Row_result_detail(common::Result_init&);

  Row_result_detail(Row_result_detail&&) = default;
  Row_result_detail& operator=(Row_result_detail&&) = default;

  RowList get_rows()
  {
    /*
      Construct RowList instance passing reference to this Row_result_detail
      object which acts as a source for the list initializer.
    */
    return *this;
  }

  row_count_t row_count();

  Row get_row()
  {
    if (!iterator_next())
      return Row();
    return iterator_get();
  }

private:

  // Storage for result column information.

  Columns  m_cols;

protected:

  col_count_t     col_count() const;
  const Column&   get_column(col_count_t) const;
  const Columns&  get_columns() const;

  bool next_result()
  {
    bool rc = Result_detail::next_result();
    if (rc)
      m_cols.init(get_impl());
    return rc;
  }

  friend iterator;
  friend RowResult;
  friend Columns;
};


// Document based results
// ----------------------

class PUBLIC_API Doc_result_detail
  : public Result_detail
{
public:

  using iterator = Iterator<Doc_result_detail, DbDoc>;
  using DocList = List_initializer<Doc_result_detail&>;

  iterator begin()
  {
    return iterator(*this);
  }

  iterator end() const
  {
    return iterator();
  }

private:

  // iterator implementation

  DbDoc m_cur_doc;

  void iterator_start() {}

  bool iterator_next();

  DbDoc iterator_get()
  {
    return m_cur_doc;
  }

protected:

  Doc_result_detail() = default;

  Doc_result_detail(common::Result_init &init)
    : Result_detail(init)
  {}

  DbDoc get_doc()
  {
    if (!iterator_next())
      return DbDoc();
    return iterator_get();
  }

  uint64_t count();

  DocList get_docs()
  {
    return *this;
  }

  friend Impl;
  friend iterator;
};


}  // internal namespace
MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif
