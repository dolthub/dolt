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

#include <mysql/cdk.h>
#include <mysqlx/xdevapi.h>

#include "impl.h"

#include <vector>
#include <sstream>
#include <iomanip>
#include <cctype>


/*
  Implementation of Result and Row interfaces.
*/

using namespace ::mysqlx::impl::common;
using namespace ::mysqlx::internal;
using namespace ::mysqlx;

using std::endl;


class mysqlx::bytes::Access
{
public:
  static bytes mk(const cdk::bytes &data)
  { return bytes(data.begin(), data.end()); }
};


/*
  Column implementation.
*/


void Column_detail::print(std::ostream &out) const
{
  if (!get_impl().m_schema_name.empty())
    out << "`" << get_impl().m_schema_name << "`.";
  string table_name = get_table_label();
  if (!table_name.empty())
    out << "`" << table_name << "`.";
  out << "`" << get_label() <<"`";
}


/*
  Method getType() translates CDK type/format info into
  DevAPI type information.

  Note: Expected to return values of Type enum constants.
*/

Type get_api_type(cdk::Type_info, const Format_info &);

unsigned Column_detail::get_type() const
{
  return unsigned(get_api_type(get_impl().m_type, get_impl()));
}


Type get_api_type(cdk::Type_info type, const Format_info &fmt)
{
  switch (type)
  {
  case cdk::TYPE_BYTES:
    return Type::BYTES;

  case cdk::TYPE_DOCUMENT:
    return Type::JSON;

  case cdk::TYPE_STRING:
  {
    const Format_descr<cdk::TYPE_STRING> &fd
      = fmt.get<cdk::TYPE_STRING>();
    if (fd.m_format.is_enum())
      return Type::ENUM;
    if (fd.m_format.is_set())
      return Type::SET;
    return Type::STRING;
  }

  case cdk::TYPE_INTEGER:
  {
    const Format_descr<cdk::TYPE_INTEGER> &fd
      = fmt.get<cdk::TYPE_INTEGER>();

    size_t f_len = fd.m_format.length();

    if (f_len < 5)
      return Type::TINYINT;

    if (f_len < 8)
      return Type::SMALLINT;

    if (f_len < 10)
      return Type::MEDIUMINT;

    if (f_len < 20)
      return Type::INT;

    return Type::BIGINT;
  }

  case cdk::TYPE_FLOAT:
  {
    const Format_descr<cdk::TYPE_FLOAT> &fd
      = fmt.get<cdk::TYPE_FLOAT>();
    switch (fd.m_format.type())
    {
    case cdk::Format<cdk::TYPE_FLOAT>::DOUBLE:  return Type::DOUBLE;
    case cdk::Format<cdk::TYPE_FLOAT>::FLOAT:   return Type::FLOAT;
    case cdk::Format<cdk::TYPE_FLOAT>::DECIMAL: return Type::DECIMAL;
    default:
      THROW("Unrecognized float value encoding format");
    }
  }

  case cdk::TYPE_DATETIME:
  {
    const Format_descr<cdk::TYPE_DATETIME> &fd
      = fmt.get<cdk::TYPE_DATETIME>();
    switch (fd.m_format.type())
    {
    case cdk::Format<cdk::TYPE_DATETIME>::TIME:
      return Type::TIME;
    case cdk::Format<cdk::TYPE_DATETIME>::TIMESTAMP:
      return Type::TIMESTAMP;
    case cdk::Format<cdk::TYPE_DATETIME>::DATETIME:
      return fd.m_format.has_time() ? Type::DATETIME : Type::DATE;
    default:
      THROW("Unrecognized temporal value encoding format");
    }
  }

  case cdk::TYPE_GEOMETRY:
    return Type::GEOMETRY;

  case cdk::TYPE_XML:
  default: return Type::BYTES;
  }
}


mysqlx::string Column_detail::get_name() const
{
  return get_impl().m_name;
}

mysqlx::string Column_detail::get_label() const
{
  return get_impl().m_label;
}

mysqlx::string Column_detail::get_schema_name() const
{
  return get_impl().m_schema_name;
}

mysqlx::string Column_detail::get_table_name() const
{
  return get_impl().m_table_name;
}

mysqlx::string Column_detail::get_table_label() const
{
  return get_impl().m_table_label;
}

unsigned long Column_detail::get_length() const
{
  return get_impl().m_length;
}

unsigned short Column_detail::get_decimals() const
{
  return get_impl().m_decimals;
}


bool Column_detail::is_signed() const
{
  if (cdk::TYPE_INTEGER != get_impl().m_type)
    return false;

  const Format_descr<cdk::TYPE_INTEGER> &fd
    = get_impl().get<cdk::TYPE_INTEGER>();
  return !fd.m_format.is_unsigned();
}

bool Column_detail::is_padded() const
{
  return get_impl().m_padded;
}


/*
  Handling character set and collation information
  -----------------------------------------------

  This information is obtained from format descriptor for columns of CDK
  STRING type. Format descriptor gives the MySQL collation id as given by
  the server. Function collation_from_charset_id() returns CollationInfo
  constant corresponding to given collation id. This CollationInfo instance
  can be then used to get collation name and the corresponding charcater
  set.
*/

#define CS_SWITCH(CS)  COLLATIONS_##CS(COLL_SWITCH)

#define COLL_SWITCH(CS,ID,COLL,CASE) \
  case ID: return Collation<CharacterSet::CS>::COLL_CONST_NAME(COLL,CASE);

const CollationInfo& collation_from_id(cdk::collation_id_t id)
{
  switch (id)
  {
    CDK_CS_LIST(CS_SWITCH)
  default:
    THROW("Unknown collation id");
  }
}


const CollationInfo& Column_detail::get_collation() const
{
  try {

    switch (get_impl().m_type)
    {
    case cdk::TYPE_BYTES:
      return Collation<CharacterSet::binary>::bin;

    case cdk::TYPE_DOCUMENT:
      return Collation<CharacterSet::utf8>::general_ci;

    case cdk::TYPE_STRING:
    {
      return collation_from_id(get_impl().m_collation);
    }

    case cdk::TYPE_INTEGER:
    case cdk::TYPE_FLOAT:
    case cdk::TYPE_DATETIME:
    default:
      THROW("No collation info for the type");
    }
  }
  CATCH_AND_WRAP
}

CharacterSet Column_detail::get_charset() const
{
  // TODO: Better use cdk encoding format information
  //const Format_descr<cdk::TYPE_STRING> &fd = m_impl->get<cdk::TYPE_STRING>();
  return get_collation().getCharacterSet();
}


/*
  Definitions of the CollationInfo constants describing all known collations
  as defined in mysqlx/collations.h.
*/

struct CollationInfo::Access
{
  enum coll_case {
    case_ci = CollationInfo::case_ci,
    case_ai_ci = case_ci,
    case_as_ci = case_ci,
    case_cs = CollationInfo::case_cs,
    case_as_cs = case_cs,
    case_as_cs_ks = case_cs,
    case_bin = CollationInfo::case_bin
  };

  static CollationInfo mk(
    CharacterSet _cs, unsigned _id, coll_case _case, const char *_name
  )
  {
    CollationInfo ci;
    ci.m_cs = _cs;
    ci.m_id = _id;
    ci.m_case = CollationInfo::coll_case(_case);
    ci.m_name = _name;
    return std::move(ci);
  }
};


/*
  A helper function that reconstructs MySQL collation name from the data
  given by COLLATIONS_XXX() lists. In most cases the collation name is just
  a concatenation of charset name, collation and sensitivity flags - this
  default name is passed as 'name' pre-allocated string. But there are few
  exceptions to the general rule: 'name_bin' is the name to be used for binary
  collations; also, individual components of the name are given to allow
  further customization.
*/

const char*
coll_name(
  std::string cs, std::string coll, std::string sensitivity,
  const char *name, const char *name_bin)
{
  static std::list<std::string> special;

  /*
    For generic UCA collations, such as uca0900, the "uca" prefix is
    not present in the MySQL collation name. For example, for the uca0900
    collation with "ai_ci" sensitivity, the collation name
    is "utf8mb4_0900_ai_ci" but the value of 'name' is "utf8mb4_uca0900_ai_ci",
    so we need to correct this.
  */

  if (coll.substr(0,3) == "uca")
  {
    special.push_back(cs + "_" + coll.substr(3) + "_" + sensitivity);
    return special.back().c_str();
  }

  if (sensitivity == "bin")
  {
    // Note: special exception for "binary" collation (no _bin suffix)
    return cs == "binary" ? "binary" : name_bin;
  }
  else
    return name;
}


#define COLL_DEFS(CS)  COLLATIONS_##CS(COLL_CONST_DEF)

#define COLL_CONST_DEF(CS,ID,COLL,CASE) \
const CollationInfo \
Collation<CharacterSet::CS>::COLL_CONST_NAME(COLL,CASE) = \
  CollationInfo::Access::mk(CharacterSet::CS, ID, \
    CollationInfo::Access::case_##CASE, \
    COLL_NAME(CS,COLL,CASE));

#define COLL_NAME(CS,COLL,CASE) \
  coll_name(#CS, #COLL, #CASE, #CS "_" #COLL "_" #CASE, #CS "_bin")

// Add utf8mb4 alias for bin collation for compatibility

#undef  COLLATIONS_utf8mb4_EXTRA
#define COLLATIONS_utf8mb4_EXTRA \
const CollationInfo Collation<CharacterSet::utf8mb4>::utf8mb4 = \
  Collation<CharacterSet::utf8mb4>::bin;

CDK_CS_LIST(COLL_DEFS)

#undef  COLLATIONS_utf8mb4_EXTRA
#define COLLATIONS_utf8mb4_EXTRA

/*
  Handling result data
  ====================
*/


/*
  Implementation of Row class
  ---------------------------
*/


Row_detail::Impl& Row_detail::get_impl()
{
  if (!m_impl)
    THROW("Attempt to use null Row instance");
  return *m_impl;
}

void Row_detail::ensure_impl()
{
  if (!m_impl)
    m_impl = std::make_shared<Impl>();
}


/*
  Decoding document values from raw bytes representation.

  Note: Conversions for other value types are handled by common::Value
  class.
*/

mysqlx::Value
mysqlx::Value::Access::mk(cdk::bytes data, Format_descr<cdk::TYPE_DOCUMENT>&)
{
  /*
    Note: this assumes that document is represented as json string
    - thanks to this we can take benefit of lazy parsing.

    Otherwise, implementation that would not assume what underlying
    representation is used for documnets should use a Codec to decode
    the raw bytes and build a representation of the documnent to be
    stored in the Value instance.
  */

  // trim initial space

  unsigned i;
  for (i = 0; i < data.size() && std::isspace(*(data.begin() + i)); ++i);

  std::string json(data.begin() + i, data.end()-1);

  if ('{' == *(data.begin() + i))
    return Value::Access::mk_doc(json);

  return Value::Access::mk_from_json(json);
}


mysqlx::col_count_t Row_detail::col_count() const
{
  return get_impl().col_count();
}


mysqlx::bytes Row_detail::get_bytes(mysqlx::col_count_t pos) const
{
  cdk::bytes data = get_impl().m_data.at(pos).data();
  return mysqlx::bytes::Access::mk(data);
}


mysqlx::Value& Row_detail::get_val(mysqlx::col_count_t pos)
{
  return get_impl().get(pos);
}


void Row_detail::process_one(
  std::pair<Impl*, mysqlx::col_count_t> *data, const mysqlx::Value &val
)
{
  Impl *impl = data->first;
  col_count_t pos = (data->second)++;
  impl->set(pos, val);
}


/*
  Result_detail
  =============
*/


Result_detail::Result_detail(Result_init &init)
{
  m_owns_impl = true;
  m_impl = new Impl(init);
}


Result_detail::~Result_detail()
{
  try {
    if (m_owns_impl)
      delete m_impl;
  }
  catch(...) {}
}


auto Result_detail::operator=(Result_detail &&other)
-> Result_detail&
{
  if (m_impl && m_owns_impl)
    delete m_impl;

  m_impl = other.m_impl;

  if (!other.m_owns_impl)
    m_owns_impl = false;
  else
  {
    m_owns_impl = true;
    other.m_owns_impl = false;
  }

  return *this;
}


auto Result_detail::get_impl() -> Impl&
{
  if (!m_impl)
    THROW("Invalid result set");
  return *m_impl;
}


void Result_detail::check_result() const
{
  if (!get_impl().has_data())
    THROW("No result set");
}


unsigned
Result_detail::get_warning_count() const
{
  return get_impl().get_warning_count();
}


auto Result_detail::get_warning(size_t pos) -> Warning
{
  if (!common::check_num_limits<unsigned>(pos))
    throw std::out_of_range("No diagnostic entry at position ...");

  get_warning_count();
  auto &impl = get_impl();
  auto &it = impl.get_entries(cdk::api::Severity::WARNING);
  size_t curr = SIZE_MAX;
  while( curr != pos && it.next())
  {
    curr++;
  }

  if (curr != pos || pos >= get_warning_count() )
    throw std::out_of_range("No diagnostic entry at position ...");

  byte level = Warning::LEVEL_ERROR;

  switch (it.entry().severity())
  {
  case cdk::api::Severity::ERROR:   level = Warning::LEVEL_ERROR; break;
  case cdk::api::Severity::WARNING: level = Warning::LEVEL_WARNING; break;
  case cdk::api::Severity::INFO:    level = Warning::LEVEL_INFO; break;
  }

  // TODO: handle error category

  return Warning_detail(
      level,
      (uint16_t)it.entry().code().value(),
      it.entry().description()
  );
}


auto Result_detail::get_affected_rows() const -> uint64_t
{
  return get_impl().get_affected_rows();
}

auto Result_detail::get_auto_increment() const -> uint64_t
{
  return get_impl().get_auto_increment();
}


auto Result_detail::get_generated_ids() const -> DocIdList
{
  return get_impl().get_generated_ids();
}


bool Result_detail::has_data() const
{
  return get_impl().has_data();
}

bool Result_detail::next_result()
{
  return get_impl().next_result();
}


/*
  RowResult
  =========
*/


template<>
bool Row_result_detail<Columns>::iterator_next()
{
  auto &impl = get_impl();
  const Row_data *row = impl.get_row();

  if (!row)
    return false;

  m_row = internal::Row_detail(
    std::make_shared<internal::Row_detail::Impl>(*row, impl.get_mdata())
  );

  return true;
}


template<>
mysqlx::col_count_t Row_result_detail<Columns>::col_count() const
{
  return get_impl().get_col_count();
}


template<>
Row_result_detail<Columns>::Row_result_detail(Result_init &init)
  : Result_detail(init)
{
  next_result();
}


template<>
auto Row_result_detail<Columns>::get_column(mysqlx::col_count_t pos) const
-> const Column&
{
  return m_cols.at(pos);
}

template<>
auto Row_result_detail<Columns>::get_columns() const
-> const Columns&
{
  return m_cols;
}


template<>
void Columns_detail<Column>::init(const Result_detail::Impl &impl)
{
  clear();
  for (col_count_t pos = 0; pos < impl.get_col_count(); ++pos)
  {
    emplace_back(&impl.get_column(pos));
  }
}


template<>
mysqlx::row_count_t Row_result_detail<Columns>::row_count()
{
  auto cnt = get_impl().count();
  ASSERT_NUM_LIMITS(row_count_t, cnt);
  return (row_count_t)cnt;
}


/*
  DocResult
  =========
*/


bool Doc_result_detail::iterator_next()
{
  auto &impl = get_impl();
  const Row_data *row = impl.get_row();

  if (impl.entry_count())
    impl.get_error().rethrow();

  if (!row)
    return false;

  // @todo Avoid copying of document string.
  cdk::foundation::bytes data = row->at(0).data();
  m_cur_doc = DbDoc(std::string(data.begin(),data.end()-1));
  return true;
}


uint64_t Doc_result_detail::count()
{
  auto cnt = get_impl().count();
  if (get_impl().entry_count() > 0)
    get_impl().get_error().rethrow();
  return cnt;
}
