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

#ifndef MYSQLX_COMMON_RESULT_INT_H
#define MYSQLX_COMMON_RESULT_INT_H


#include "common.h"
#include "session.h"
#include "value.h"

#include <mysql/cdk.h>
#include <mysql/cdk/converters.h>
#include <expr_parser.h>


PUSH_SYS_WARNINGS
#include <queue>
POP_SYS_WARNINGS


namespace mysqlx {
namespace impl {
namespace common {

// TODO: Use std::variant when available
using cdk::foundation::variant;


/*
  Handling result meta-data information
  =====================================

  Meta-data for result columns is provided by CDK cursor object which implements
  cdk::Meta_data interface. This information is read from cursor into
  an instance of Meta_data class in Result_impl::get_meta_data() method which
  is called from Result_impl_base::next_result() method when meta-data
  is eceived from the server. Created Meta_data instance is shared between
  result object and Row instances representing individual rows fetched from
  the result. Inside Row instance, meta-data is used to decode values from raw
  bytes.

  Textual meta-data information, such as column names, can be stored either as
  a wide string or utf8 encoded. For that reason the Meta_data and Column_info
  classes are in fact templates parametrized by class STR used to store string
  data (either std::string or some wide string class).

  Meta_data class contains a map from column positions to instances
  of Column_info class. Each Column_info instance can store meta-data
  information for a single column. The Column_info instances are created in
  the Meta_data constructor which reads meta-data information from
  cdk::Meta_data interface and adds Column_info objects to the map using add()
  methods.

  The CDK meta-data for a single column consists of:

  - CDK type constant (cdk::Type_info)
  - encoding format information (cdk::Format_info)
  - additional column information (cdk::Column_info)

  The first two items describe the type and encoding of the values that appear
  in the result in the corresponding column. Additional column information
  is mainly the column name etc.

  Classes Format_descr<TTT> and Format_info are used to store type and encoding
  format information for each column. For CDK type TTT, class Format_descr<TTT>
  stores encoding format descriptor (instance of cdk::Format<TTT> class) and
  encoder/decoder for the values (instance of cdk::Codec<TTT> class)
  (Note: for some types TTT there is no codec or no format descriptor). Class
  Format_info is a variant class that can store Format_descr<TTT> values
  for different TTT.

  Class Column_info extends Format_info with additional storage for
  the cdk::Column_info data (column name etc).

  Using meta-data to decode result values
  ---------------------------------------

  Meta-data information stored in m_mdata member of Result_impl_base class
  is used to interpret raw bytes returned by CDK in the reply to a query.
  This interpretation is done by Row_impl class which takes (shared pointer
  to) Meta_data instance as its constructor parameter and stores it in its
  m_mdata member. Encoding format information for a given column is extracted
  from this Meta_data instance in Row_impl::get() which then passes it
  to Row_impl::conver_at() and eventually to the static Value::Access::mk()
  functions which construct value from raw bytes using the format encoding
  information.

  Row_impl is actually a template parametrized by the exact VAL class used
  to decode and store data received from the server. Normally this should be
  the common::Value class, but a specialization can be used which handles
  either data conversion or data storage in some special way. For example,
  X DevAPI uses a specialization to handle storage of structured data such
  as documents and arrays.

  The common::Value::Access::mk() functions are defined in terms of convert()
  function overloads defined in result.cc. They look at the encoding format
  information and use the encoder instance inside Format_descr<> object to
  convert raw bytes into a value of appropriate type.
*/


/*
  Encapsulates CDK encoding format information and a raw bytes decoder
  for values of given CDK type T. For some types either the decoder or the
  format information are redundant and thus not stored inside Format_descr<T>.
*/

template <cdk::Type_info T>
class Format_descr
{
public:

  cdk::Format<T> m_format;
  cdk::Codec<T>  m_codec;

  Format_descr(const cdk::Format_info &fi)
    : m_format(fi), m_codec(fi)
  {}
};


/*
  Format_descr<T> specializations for different types.
*/


template <>
struct Format_descr<cdk::TYPE_DOCUMENT>
{
  cdk::Format<cdk::TYPE_DOCUMENT> m_format;
  cdk::Codec<cdk::TYPE_DOCUMENT>  m_codec;

  Format_descr(const cdk::Format_info &fi)
    : m_format(fi)
  {}
};


/*
  Note: we do not decode temporal values yet, thus there is
  no codec in Format_descr class.
*/

template<>
struct Format_descr<cdk::TYPE_DATETIME>
{
  cdk::Format<cdk::TYPE_DATETIME> m_format;

  Format_descr(const cdk::Format_info &fi)
    : m_format(fi)
  {}
};


template <>
struct Format_descr<cdk::TYPE_BYTES>
{
  cdk::Format<cdk::TYPE_BYTES> m_format;

  Format_descr(const cdk::Format_info &fi)
    : m_format(fi)
  {}
};


/*
  Note: For GEOMETRY and XML types we do not decode the values.
  Also, CDK does not provide any encoding format information -
  GEOMETRY uses some unspecified MySQL internal representation
  format and XML format is well known.
*/

template<>
struct Format_descr<cdk::TYPE_GEOMETRY>
{
  Format_descr(const cdk::Format_info &)
  {}
};

template<>
struct Format_descr<cdk::TYPE_XML>
{
  Format_descr(const cdk::Format_info &)
  {}
};


/*
  Structure Format_info holds information about the type
  of a column (m_type) and about its encoding format in
  Format_descr<T> structure. Since C++ type of Format_descr<T>
  is different for each T, a variant object is used to store
  the appropriate Format_descr<T> value.
*/

struct Format_info
{
  typedef variant <
    Format_descr<cdk::TYPE_STRING>,
    Format_descr<cdk::TYPE_INTEGER>,
    Format_descr<cdk::TYPE_FLOAT>,
    Format_descr<cdk::TYPE_DOCUMENT>,
    Format_descr<cdk::TYPE_BYTES>,
    Format_descr<cdk::TYPE_DATETIME>,
    Format_descr<cdk::TYPE_GEOMETRY>,
    Format_descr<cdk::TYPE_XML>
  > Format_info_storage;

  cdk::Type_info      m_type;
  Format_info_storage m_fmt;

  template <cdk::Type_info T>
  explicit Format_info(const Format_descr<T> &fd)
    : m_type(T), m_fmt(fd)
  {}

  explicit
  Format_info(cdk::Type_info type, const Format_descr<cdk::TYPE_BYTES> &fmt)
    : m_type(type), m_fmt(fmt)
  {}

  template <cdk::Type_info T>
  Format_descr<T>& get() const
  {
    /*
      Note: we cast away constness here, because using a codec can
      modify it, and thus the Format_descr<T> must be mutable.
    */
    return const_cast<Format_descr<T>&>(
      m_fmt.get<Format_descr<T>>()
    );
  }

};


}}  // impl::common


MYSQLX_ABI_BEGIN(2,0)
namespace common {

using impl::common::Format_info;
using impl::common::Format_descr;


/*
  Storage for single column meta-data.

  This extends Fromat_info with members used to store other column meta-data
  such as its name etc.

  Note: All textual data is stored as utf8 encoded strings.

  Note: Column_info must be defined inside ABI namespace to preserve ABI
  compatibility (name of this class is used in public API)
*/

class Column_info
  : public Format_info
{
public:

  using string = std::string;

  string m_name;
  string m_label;
  string m_table_name;
  string m_table_label;
  string m_schema_name;
  string m_catalog;

  unsigned long  m_length;
  unsigned short m_decimals;
  cdk::collation_id_t m_collation;
  bool           m_padded = false;

  Column_info(const Column_info&) = default;

  /*
    Create Column_info instance for a column of type T that uses given
    encoding format. The rest of column meta-data should be filled in using
    store_info() method.
  */

  template <cdk::Type_info T>
  explicit Column_info(const Format_descr<T> &fmt)
    : Format_info(fmt)
  {}

  /*
    Create Clumn_info instance for a column which stores values of the given
    type as raw bytes. The fi parameter describes the raw bytes storage
    characteristics such as padding.
  */

  explicit Column_info(cdk::Type_info type, const cdk::Format_info &fi)
    : Format_info(type, Format_descr<cdk::TYPE_BYTES>(fi))
  {}

  /*
    After creating Column_info instance this method should be called to
    store information taken from cdk::Column_info interface.
  */

  void store_info(const cdk::Column_info &ci)
  {
    m_name = ci.orig_name();
    m_label = ci.name();

    if (ci.table())
    {
      m_table_name = ci.table()->orig_name();
      m_table_label = ci.table()->name();

      if (ci.table()->schema())
      {
        m_schema_name = ci.table()->schema()->name();
        if (ci.table()->schema()->catalog())
          m_catalog = ci.table()->schema()->catalog()->name();
      }
    }

    m_collation = ci.collation();
    m_length = ci.length();
    ASSERT_NUM_LIMITS(short unsigned, ci.decimals());
    m_decimals = static_cast<short unsigned>(ci.decimals());

    if (cdk::TYPE_BYTES == m_type)
    {
      uint64_t pad_width = get<cdk::TYPE_BYTES>().m_format.pad_width();
      if (0 < pad_width)
      {
        m_padded = true;
        assert(m_length == pad_width);
      }
    }
  }

};

}  // common
MYSQLX_ABI_END(2,0)


namespace impl {
namespace common {

using MYSQLX_ABI(2,0)::common::Column_info;


/*
  Meta_data<STR> holds type and format information for all columns in
  a result. This information is stored in Column_info<STR> objects. The
  string type STR is used to internally store textual meta-data information.
*/

struct Meta_data
{
protected:

  cdk::col_count_t  m_col_count = 0;
  std::map<cdk::col_count_t, Column_info> m_cols;

public:

  /*
    Create Meta_data instance and fill it using meta-data information
    read from the cdk::Meta_data interface.
  */

  Meta_data(cdk::Meta_data&);

  virtual ~Meta_data() {}


  col_count_t col_count() const { return m_col_count; }

  /*
    Return meta-data information for the column at the given position.
  */

  const Column_info& get_column(cdk::col_count_t pos) const
  {
    return m_cols.at(pos);
  }

  const Format_info& get_format(cdk::col_count_t pos) const
  {
    return m_cols.at(pos);
  }

  cdk::Type_info get_type(cdk::col_count_t pos) const
  {
    return get_format(pos).m_type;
  }

private:

  /*
    Add to this Meta_data instance information about column
    at position `pos`. The type and format information is given
    by cdk::Format_info object, additional column meta-data by
    cdk::Column_info object.
  */

  template<cdk::Type_info T>
  void add(
    cdk::col_count_t pos,
    const cdk::Column_info &ci,
    const cdk::Format_info &fi
  )
  {
    m_cols.emplace(pos, Column_info(Format_descr<T>(fi)));
    m_cols.at(pos).store_info(ci);
  }

  /*
    Add raw column information (whose values are presented as
    raw bytes).
  */

  void add_raw(
    cdk::col_count_t pos,
    const cdk::Column_info &ci,
    cdk::Type_info type,
    const cdk::Format_info &fi
  )
  {
    m_cols.emplace(pos, Column_info(type, fi));
    m_cols.at(pos).store_info(ci);
  }

};

using Shared_meta_data = std::shared_ptr<Meta_data>;


/*
  Create Meta_data instance using information provided by
  cdk::Meta_data interface.

  This costructor calls appropriate add() methods to add Column_info
  instances to the m_cols map.
*/

inline
Meta_data::Meta_data(cdk::Meta_data &md)
{
  m_col_count = md.col_count();

  for (col_count_t pos = 0; pos < m_col_count; ++pos)
  {
    cdk::Type_info ti = md.type(pos);
    const cdk::Format_info &fi = md.format(pos);
    const cdk::Column_info &ci = md.col_info(pos);

    switch (ti)
    {
    case cdk::TYPE_STRING:    add<cdk::TYPE_STRING>(pos, ci, fi);   break;
    case cdk::TYPE_INTEGER:   add<cdk::TYPE_INTEGER>(pos, ci, fi);  break;
    case cdk::TYPE_FLOAT:     add<cdk::TYPE_FLOAT>(pos, ci, fi);    break;
    case cdk::TYPE_DOCUMENT:  add<cdk::TYPE_DOCUMENT>(pos, ci, fi); break;
    case cdk::TYPE_DATETIME:  add<cdk::TYPE_DATETIME>(pos, ci, fi); break;
    case cdk::TYPE_GEOMETRY:  add<cdk::TYPE_GEOMETRY>(pos, ci, fi); break;
    case cdk::TYPE_XML:       add<cdk::TYPE_XML>(pos, ci, fi); break;
    default:
      add_raw(pos, ci, ti, fi);
      break;
    }
  }
}


/*
  Handling result data
  ====================
*/


/*
  Convenience wrapper around std container that is used
  to store incoming raw bytes sequence.

  TODO: More efficient implementation.
*/

class Buffer
{
  std::vector<byte> m_impl;

public:

  void append(cdk::bytes data)
  {
    m_impl.insert(m_impl.end(), data.begin(), data.end());
  }

  size_t size() const { return m_impl.size(); }

  cdk::bytes data() const
  {
    return cdk::bytes((byte*)m_impl.data(), m_impl.size());
  }
};


/*
  Data structure used to hold raw row data. It holds a Buffer with
  raw bytes for each non-null field of a row.
*/

typedef std::map<col_count_t, Buffer> Row_data;


/*
  Implementation for a single Row instance. It holds a copy of
  raw data and a shared pointer to row set meta-data.

  It is possible to create an empty Row_impl instance and populate it using
  set() method. Such Row_impl instance does not correspond to a row received
  from the server, but represents a row that is defined by the user (for
  example, to be later inserted into a table).

  This template is parametrized by VAL class, such as commmon::Value used
  to convert and store result data. Converted values are stored as instances
  of VAL class in m_vals map and method get() returns references to these
  instances.

  Note: VAL class must define static method template used for converting raw
  bytes into values:

    template <cdk::Type_info T>
    Value Value::Access::mk(cdk::bytes data, mysqlx::Format_descr<T>&)
*/

template <class VAL = Value>
class Row_impl
{
public:

  using Value = VAL;
  using bytes = cdk::bytes;

  Row_impl() {};

  // Note: row data is copied into Row_impl instance

  Row_impl(const Row_data &data, const Shared_meta_data &md)
    : m_data(data), m_mdata(md)
  {}

protected:

  Row_data          m_data;
  Shared_meta_data  m_mdata;
  std::map<col_count_t, Value>    m_vals;
  col_count_t                     m_col_count = 0;

public:

  void clear()
  {
    m_data.clear();
    m_vals.clear();
    m_mdata.reset();
  }

  col_count_t col_count() const
  {
    return m_mdata ? m_mdata->col_count() : m_col_count;
  }

  bytes get_bytes(col_count_t pos) const
  {
    if (m_mdata && pos >= m_mdata->col_count())
      throw std::out_of_range("row column");

    try {
      return m_data.at(pos).data();
    }
    catch (const std::out_of_range&)
    {
      // empty bytes indicate null value
      return bytes();
    }
  }

  /*
    Get value of field at given position after converting to Value.
    @throws std::out_of_range if given column does not exist in the row.
  */

  Value& get(col_count_t pos)
  {
    if (m_mdata && pos >= m_mdata->col_count())
      throw std::out_of_range("row column");

    try {
      return m_vals.at(pos);
    }
    catch (const std::out_of_range&)
    {
      if (!m_mdata)
        throw;
      const Format_info &fi = m_mdata->get_format(pos);
      convert_at(pos, fi);
      return m_vals.at(pos);
    }
  }

  void set(col_count_t pos, const Value &val)
  {
    m_vals.emplace(pos, val);
    if (pos >= m_col_count)
      m_col_count = pos + 1;
  }

private:

  void convert_at(col_count_t pos, const Format_info &fi)
  {
    Buffer *raw = nullptr;

    try {
      raw = &m_data.at(pos);
    }
    catch (const std::out_of_range&)
    {}

    if (!raw || 0 == raw->size())
    {
      // Null value
      m_vals.emplace(pos, Value());
      return;
    }

    /*
      Call static function VAL::Access:mk() to construct VAL instance from
      raw bytes and put it into m_vals map. Aprropriate encoding format
      information is extracted from fi.
    */

#define CONVERT(T) case cdk::TYPE_##T: \
    m_vals.emplace(pos, \
      VAL::Access::mk(raw->data(), fi.get<cdk::TYPE_##T>()) \
    ); \
    break;

    switch (fi.m_type)
    {
      CDK_TYPE_LIST(CONVERT)
    }
  }

};


/*
  Given encoding format information, convert raw bytes to the corresponding
  value.
*/

Value convert(cdk::bytes, Format_descr<cdk::TYPE_STRING>&);
Value convert(cdk::bytes, Format_descr<cdk::TYPE_INTEGER>&);
Value convert(cdk::bytes, Format_descr<cdk::TYPE_FLOAT>&);
Value convert(cdk::bytes, Format_descr<cdk::TYPE_DOCUMENT>&);
Value convert(cdk::bytes, Format_descr<cdk::TYPE_DATETIME>&);

/*
  Generic template used when no type-specific specialization is defined.
  It builds a value holding the raw bytes.
*/

template <cdk::Type_info T>
Value convert(cdk::bytes data, Format_descr<T>&)
{
  /*
    Note: Trailing '\0' byte is used for NULL value detection and is not
    part of the data
  */
  return{ data.begin(), data.size() - 1 };
}

}}  // impl::common


MYSQLX_ABI_BEGIN(2, 0)
namespace common {

/*
  This static function is called by Row_impl to build column value from raw
  bytes.
*/

template<cdk::Type_info T>
inline
Value Value::Access::mk(cdk::bytes data, impl::common::Format_descr<T> &format)
{
  // Use convert() to convert raw bytes into a value

  Value val{ convert(data, format) };

  /*
    Store raw representation in m_str if possible (for RAW value this is
    already done by the constructor).
  */

  switch (val.get_type())
  {
  case Value::RAW:
  case Value::STRING:
  case Value::VNULL:
    break;

  default:
    /*
      Note: Trailing '\0' byte is used for NULL value detection and is not
      part of the data
    */
    val.m_str.assign(data.begin(), data.end() - 1);
    break;
  }

  return val;
}

}  // common
MYSQLX_ABI_END(2, 0)


/*
  Implementation of result object
  ===============================

  Result object of class Result_impl<STR> gives access to a reply sent
  by the server in response to a query. It gives information about the reply
  such as whether it contains row data or affected items counts etc. If reply
  contains row data, result object stores and gives access to meta-data about
  this row data. It also handles reading rows from the reply. A result object
  can handle server replies which contain multiple results.

  Template parameter STR is a string class used for storing textual meta-data
  - the get_column() method presents column meta-data as Column_info<STR>
  instances.

  A result object is created from a Result_init instance which contains
  information about the session and CDK reply object used to access server
  reply. Method Executable_if::execute() implemented by objects representing
  queries returns such a Result_init object that is then used to initialize
  a result instance.
*/

MYSQLX_ABI_BEGIN(2,0)
namespace common {

using impl::common::Shared_meta_data;
using impl::common::Row_data;
using impl::common::Column_info;

/*
  An abstract interface used to initialize result of an operation.

  An object implementing this interface can be used to construct a result
  object (see ctor of Result_impl_base).

  Note: This class must be defined inside ABI namespace to preserve ABI
  compatibility (its name is used in public API)
*/

class Result_init
{
public:

  virtual Shared_session_impl get_session() = 0;

  /*
    Return pointer to the cdk reply object which gives access to results
    returned by the server. The caller of this method takes ownership of
    the reply object.
  */

  virtual cdk::Reply*      get_reply() = 0;

  /*
    A hook that can perform additional initialization of the result object
    being constructed from a Result_init instance.
  */

  virtual void init_result(Result_impl&) {} // GCOV_EXCL_LINE
};


/*
  Base class for Result_impl<STR> with all members that are not dependent
  on the STR template parameter.

  Given a server reply to a command, it processes the reply giving access to
  the result data and meta-data.

  Note: This class must be defined inside ABI namespace to preserve ABI
  compatibility (its name is used in public API)
*/

class Result_impl
  : public cdk::Row_processor
  , public cdk::api::Diagnostics
{
public:

  Result_impl(Result_init &init);

  virtual ~Result_impl();

  /*
    Prepare for reading (the next) result.

    This method should be called first, before any other methods which access
    the result. After consuming one result, it should be called again to see
    if more results are pending (which can happen in case of a multi-result
    reply from the server).

    Returns true if (the next) result is ready for reading. Otherwise there
    are no more results and server reply has been entirely consumed. Note that
    the result does not need to contain row data - this can be determined by
    has_data() method.
  */

  bool next_result();

  /*
    Returns true if the current result has (more) rows to be fetched.
  */

  bool has_data() const;

  /*
    Return reference to result meta-data. Note that returned meta-data can
    be used together with Row_data from get_row() to build a Row_impl
    instance.
  */

  const Shared_meta_data& get_mdata() const;

  /*
    Fetches next row from the result, if any. Returns NULL if there are no
    more rows. Throws exception if this result has no data.

    Note: Rows are cached internally and read in larger batches.
  */

  const Row_data *get_row();

  // Store all remaining rows in the internal cache.

  void store();

  // Store all results to cache

  void store_all_results();

  /*
    Return the number of rows remaining in the result (the rows that have been
    already fetched with get_row() are not counted).

    This has a side effect of storing all rows in the chache.
  */

  row_count_t count();

  /*
    Discard the reply. TODO: Implement it when needed.
  */
  //void discard();

  /*
    Methods to access result information
  */

  col_count_t get_col_count() const;
  cdk::row_count_t get_affected_rows() const;
  cdk::row_count_t get_auto_increment() const;
  unsigned get_warning_count() const;

  /*
    Client-side filtering of row data. Function m_row_filter is applied
    for each received row to determine if it should be skipped.
  */

  using Row_filter_t = std::function<bool(const Row_data&)>;
  Row_filter_t m_row_filter = [](const Row_data&) { return true; };

  // Get generated document id information.

  const std::vector<std::string>& get_generated_ids() const;

  /*
    Get column information.

    Returns reference to Column_info<STR> object with information about
    the column. This Column_info<STR> object sits inside and is owned by
    this Result_impl object.
  */

  const Column_info& get_column(col_count_t pos) const
  {
    if (m_result_mdata.empty() || !m_result_mdata.front())
      THROW("No result set");
    return m_result_mdata.front()->get_column(pos);
  }

protected:

  Result_impl(const Result_impl&) = delete;


  /*
    Note: Session implementation must exists as long as this result
    implementation exists. Thus we keep shared pointer to it to make sure
    that session implementation is not destroyed when session object itself
    is deleted.
  */

  Shared_session_impl  m_sess;


  // -- Result meta-data

  /*
    This flag is set to true after next_result(), when the result has been
    prepared.
  */

  bool m_inited = false;

  // Note: meta-data can be shared with Row instances

  std::queue<Shared_meta_data>  m_result_mdata;

  /*
    Method fetch_meta_data() creates a new Meta_data_base instance with
    the information taken from a CDK cursor object (or any other object
    implementing cdk::Meta_data interface). This is called from next_result()
    when meta-data is available. The returned object is stored in m_mdata which
    takes its ownership.
  */

  //Meta_data_base* fetch_meta_data(cdk::Meta_data&) = 0;


  // -- Result data

  /*
    This flag is true if there are pending rows that server sends and that
    should be consumed.
  */

  bool m_pending_rows = false;

  /*
    Note: Not using auto-ptrs for m_reply and m_cursor to ensure correct
    order of deleting them (see dtor).
  */

  cdk::Reply  *m_reply;
  cdk::Cursor *m_cursor = nullptr;

  using Row_cache = std::forward_list<Row_data>;

  // Each queue elements represents a resultset.

  std::queue<Row_cache> m_result_cache;
  std::queue<row_count_t> m_result_cache_size;
  Row_cache::iterator m_cache_it;

  /*
    Ensure some rows are loaded into the cache. If cache is not empty, it
    returns true right away. Otherwise it loads rows into the cache. If
    preftetch_size is non-zero then at most that many rows are loaded.

    Returns true if some rows are present in the cache.

    This will cache the current resultset to the back of the queue
  */

  bool load_cache(row_count_t prefetch_size = 0);

  // Jumps to new resultset without poping the cache element

  bool read_next_result();

  // Called after resultset has been get by user

  void pop_row_cache()
  {
    if(!m_result_mdata.empty())
      m_result_mdata.pop();
    if (!m_result_cache.empty())
    {
      /*
        At this point m_cache_it can point inside the last entry of
        m_result_cache that is just to be removed. Such dangling iterator
        can cause issues when later it is assigned to a new value (as
        compiler thinks it is still pointing inside old container and might
        want to do some cleanups). To avoid dangling iterator, we reset it
        to something neutral here.

        Note: This "fix" works under assumption that "one past the end"
        iterator is compatible between different containers, which seems to
        be the case for all compilers we use.

        TODO: A better solution would be to use std::option<> type for
        m_cache_it.
      */

      m_cache_it = m_result_cache.back().end();
      m_result_cache.pop();
    }
    if (!m_result_cache_size.empty())
      m_result_cache_size.pop();
  }

  // Called on each resultset to be read.

  void push_row_cache();

public:

  // -- Diagnostic information

  // Return number of diagnostic entries with given error level (defaults to ERROR).
  unsigned int entry_count(Severity::value level = Severity::ERROR) override
  {
    if (!m_reply)
      THROW("Attempt to get warning count for empty result");

    return m_reply->entry_count(level);
  }

  // Get an iterator to iterate over diagnostic entries with level above or equal to given one
  // (for example, if level is WARNING then iterates over all warnings and errors).
  // By default returns iterator over errors only. The Error_iterator interface extends
  // Iterator interface with single Error_iterator::error() method that returns the current error entry from the sequence.
  Iterator& get_entries(Severity::value level = Severity::ERROR) override
  {
    if (!m_reply)
      THROW("Attempt to get warning count for empty result");

    return m_reply->get_entries(level);
  }

  // Convenience method to return first error entry (if any).
  // Equivalent to get_erros().error(). Note that this method can throw exception if there is no error available.
  const cdk::Error& get_error() override
  {
    if (!m_reply)
      THROW("Attempt to get warning count for empty result");

    return m_reply->get_error();
  }

private:

  // Row_processor

  Row_data    m_row;

  bool row_begin(row_count_t) override
  {
    m_row.clear();
    return true;
  }

  void row_end(row_count_t) override;

  size_t field_begin(col_count_t pos, size_t) override;
  void   field_end(col_count_t) override {}
  void   field_null(col_count_t) override {}
  size_t field_data(col_count_t pos, bytes) override;
  void   end_of_data() override;

};


inline
col_count_t Result_impl::get_col_count() const
{
  if (m_result_mdata.empty())
    THROW("No result set");
  return m_result_mdata.front()->col_count();
}

inline
cdk::row_count_t Result_impl::get_affected_rows() const
{
  if (!m_reply)
    THROW("Attempt to get affected rows count on empty result");
  return m_reply->affected_rows();
}

inline
cdk::row_count_t Result_impl::get_auto_increment() const
{
  if (!m_reply)
    THROW("Attempt to get auto increment value on empty result");
  return m_reply->last_insert_id();
}

inline
unsigned Result_impl::get_warning_count() const
{
  auto self = const_cast<Result_impl*>(this);
  self->store_all_results();
  return self->entry_count(cdk::api::Severity::WARNING);
}

inline
const std::vector<std::string>& Result_impl::get_generated_ids() const
{
  if (!m_reply)
    THROW("Attempt to get generated ids for empty result");
  return m_reply->generated_ids();
}

inline
bool Result_impl::has_data() const
{
  return (!m_result_cache.empty() && !m_result_cache.front().empty()) || m_pending_rows;
}


inline
const Shared_meta_data& Result_impl::get_mdata() const
{
  return m_result_mdata.front();
}


inline
void Result_impl::store()
{
  load_cache();
}

inline
void Result_impl::store_all_results()
{
  do{
  store();
  }while(read_next_result());
}

inline
row_count_t Result_impl::count()
{
  store();
  if (entry_count() > 0)
    get_error().rethrow();
  row_count_t rc = 0;
  if(!m_result_cache_size.empty())
    rc = m_result_cache_size.front();
  return rc;
}


}  // common
MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif
