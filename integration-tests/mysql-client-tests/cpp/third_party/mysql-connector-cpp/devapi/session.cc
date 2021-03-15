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


#include "impl.h"
#include "../common/settings.h"
#include "../common/result.h"

#include <mysql/cdk.h>
#include <uri_parser.h>

#include <mysqlx/xdevapi.h>

PUSH_SYS_WARNINGS
#include <iostream>
#include <sstream>
#include <list>
POP_SYS_WARNINGS



const unsigned max_priority = 100;

using namespace ::mysqlx::impl::common;
using namespace ::mysqlx::internal;
using namespace ::mysqlx;
using std::ostream;


/*
  Session settings
  ================
*/


void process_val(
    cdk::JSON::Processor::Any_prc::Scalar_prc *prc,
    const mysqlx::Value &val
)
{
  using mysqlx::Value;

  assert(prc);

  auto vtype = val.getType();
  switch (vtype)
  {
  // TODO: avoid unnecessary utf8 conversion
  case Value::STRING:  prc->str(val.get<mysqlx::string>());  break;
  case Value::INT64:   prc->num(val.get<int64_t>());    break;
  case Value::UINT64:  prc->num(val.get<uint64_t>());    break;
  case Value::BOOL:    prc->yesno(val.get<bool>());  break;
  case Value::VNULL:   prc->null();                 break;
  case Value::DOCUMENT:
    {
      std::stringstream json;
      json << val.get<DbDoc>();
      prc->str(json.str());
    }
    break;
  default:
    mysqlx::throw_error("Invalid type of session option value");
  }
}

void process(cdk::JSON::Processor::Any_prc *prc, const mysqlx::Value &val)
{
  using mysqlx::Value;
  assert(prc);

  switch (val.getType())
  {
  case Value::DOCUMENT:
    {
      auto *dprc = prc->doc();
      if (!dprc)
        return;

      DbDoc doc = val;

      dprc->doc_begin();


      for (auto f : doc)
      {
        auto *eprc = dprc->key_val(f);
        if (eprc)
          process(eprc, doc[f]);
      }

      dprc->doc_end();
    }
    break;

  case Value::ARRAY:
    {
      auto *aprc = prc->arr();
      if (!aprc)
        return;

      aprc->list_begin();

      for (Value v : val)
      {
        auto *eprc = aprc->list_el();
        if (eprc)
          process(eprc, v);
      }

      aprc->list_end();
    }
    break;

  default:
    process_val(prc->scalar(), val);
    break;
  }
}


template<>
void
Settings_detail<Settings_traits>::do_set(session_opt_list_t &&opts)
{
  Setter set(*this);

  set.doc_begin();

  for (auto &opt_val : opts)
  {
    int   opt = opt_val.first;
    Value &val = opt_val.second;

    process(set.key_val(opt), val);
  }

  set.doc_end();
}


/*
  Note: old code for ABI compatibility. Some versions of gcc complained
  if this was not defined inside namespace (even though it did not complain
  for the do_set() overload defined above)
*/

namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)
namespace internal {

template<>
void
Settings_detail<Settings_traits>::do_set(
  std::list<std::pair<int, common::Value>> &&opts
)
{
  Setter set(*this);

  set.doc_begin();

  for (auto &opt_val : opts)
  {
    int   opt = opt_val.first;
    Value val(opt_val.second);

    process_val(set.key_val(opt)->scalar(), val);
  }

  set.doc_end();
}

}  // internal
MYSQLX_ABI_END(2,0)
}  // mysqlx




/*
  Client implementation
  ======================
*/

Client_detail::Client_detail(common::Settings_impl &settings)
{
  cdk::ds::Multi_source source;
  settings.get_data_source(source);
  m_impl = std::make_shared<Impl>(source);
  m_impl->set_pool_opts(settings);
}


Shared_session_pool&
Client_detail::get_session_pool()
{
  return m_impl;
}


void Client_detail::close()
{
  auto p = get_session_pool();
  if (p)
    p->close();
}


/*
  Session implementation
  ======================
*/

Session_detail::Session_detail(common::Settings_impl &settings)
{
  try {

    cdk::ds::Multi_source source;
    settings.get_data_source(source);
    m_impl = std::make_shared<Impl>(source);

  }
  catch (const cdk::foundation::connection::TLS::Options::TLS_version::Error &e)
  {
    std::stringstream msg;
    msg << "'" << e.get_ver() << "'";
    msg << " not recognized as a valid TLS protocol version";
    msg << " (should be one of TLSv1, TLSv1.1, TLSv1.2, TLSv1.3)";
    throw_error(msg.str().c_str());
  }
  catch(const cdk::Error &e)
  {
    if (e.code() == cdk::cdkerrc::tls_versions)
      throw_error("No supported TLS protocol version found in the 'tls-versions' list");

    else if (e.code() == cdk::cdkerrc::tls_ciphers)
      throw_error("No valid cipher suite found in the 'tls-ciphersuites' list");

    try {
      throw;
    }
    CATCH_AND_WRAP
  }
  CATCH_AND_WRAP
}


Session_detail::Session_detail(Shared_session_pool &pool)
{
  m_impl = std::make_shared<Impl>(pool);
}


cdk::Session& Session_detail::get_cdk_session()
{
  if (!m_impl)
    throw Error("Session closed");

  return *(m_impl->m_sess);
}


void Session_detail::prepare_for_cmd()
{
  assert(m_impl);
  m_impl->prepare_for_cmd();
}


void Session_detail::close()
{
  m_impl->release();
  m_impl.reset();
}



// ---------------------------------------------------------------------

/*
  Transactions.
*/

void Session_detail::start_transaction()
{
  Op_trx<Trx_op::BEGIN> cmd(m_impl);
  cmd.execute();
}


void Session_detail::commit()
{
  Op_trx<Trx_op::COMMIT> cmd(m_impl);
  cmd.execute();
}


void Session_detail::rollback(const mysqlx::string &sp)
{
  Op_trx<Trx_op::ROLLBACK> cmd(m_impl, sp);
  cmd.execute();
}

mysqlx::string Session_detail::savepoint_set(const mysqlx::string &sp)
{
  Op_trx<Trx_op::SAVEPOINT_SET> cmd(m_impl, sp);
  cmd.execute();
  return cmd.get_name();
}

void Session_detail::savepoint_remove(const mysqlx::string &sp)
{
  Op_trx<Trx_op::SAVEPOINT_REMOVE> cmd(m_impl, sp);
  cmd.execute();
}


// ---------------------------------------------------------------------


void Session_detail::create_schema(const mysqlx::string &name, bool reuse)
{
  Schema_ref schema(name);
  create_object<Object_type::SCHEMA>(m_impl, schema, reuse);
}


void Session_detail::drop_schema(const mysqlx::string &name)
{
  Schema_ref schema(name);
  drop_object<Object_type::SCHEMA>(m_impl, schema);
}


mysqlx::string Session_detail::get_default_schema_name()
{
  if (m_impl->m_default_db.empty())
    throw Error("No default schema set for the session");
  return m_impl->m_default_db;
}



/*
  Schema list source.
*/


Query_src::~Query_src()
{
  delete m_res;
}


Session_detail::Name_src::Name_src(
  const Session &sess, const mysqlx::string &pattern
)
  : m_sess(sess)
{
  Op_list<Object_type::SCHEMA> list_op{ sess.m_impl, pattern };
  m_res = new Res_impl(list_op.execute());
}


auto Session_detail::Schema_src::iterator_get() -> Schema
{
  return { const_cast<Session&>(m_sess), Name_src::iterator_get() };
}




/*
  Schema
  ======
*/


void
Schema_detail::create_collection(const mysqlx::string &name,
                                 mysqlx::CollectionOptions options)
{
  Object_ref coll(m_name, name);

  create_object<Object_type::COLLECTION>(m_sess, coll, options.m_data.reuse,
                                         options.m_data.validation.m_data.validation_level,
                                         options.m_data.validation.m_data.validation_schema.get_json());
}

void
Schema_detail::modify_collection(const mysqlx::string &name,
                                 mysqlx::CollectionOptions options)
{

  Object_ref coll(m_name, name);

  if(options.m_data.reuse)
    throw_error("Can't use CollectionOptions::REUSE on collectionModify");

  modify_object<Object_type::COLLECTION>(
        m_sess, coll,
        options.m_data.validation.m_data.validation_level,
        std::string(options.m_data.validation.m_data.validation_schema.get_json()));
}


void Schema_detail::drop_collection(const mysqlx::string& name)
{
  Object_ref coll(m_name, name);
  drop_object<Object_type::COLLECTION>(m_sess, coll);
}



Schema_detail::Name_src::Name_src(
  const Schema &sch,
  Obj_type type,
  const mysqlx::string &pattern
)
  : m_schema(sch)
{
  Schema_ref obj{ sch.getName() };
  const auto &sess = sch.Schema_detail::m_sess;

  switch (type)
  {
  case COLLECTION:
    {
      Op_list<Object_type::COLLECTION> list_op(sess, obj, pattern);
      m_res = new Res_impl(list_op.execute());
    };
    break;

  case TABLE:
    {
      Op_list<Object_type::TABLE> list_op(sess, obj, pattern);
      m_res = new Res_impl(list_op.execute());
    };
    break;
  }
}


Collection
Schema_detail::Collection_src::iterator_get()
{
  return Collection(m_schema, Name_src::iterator_get());
}

Table
Schema_detail::Table_src::iterator_get()
{
  auto *row = static_cast<const Row_data*>(m_row);
  assert(1 < row->size());
  assert(cdk::TYPE_STRING == m_res->get_column(1).m_type);

  cdk::string type;
  m_res->get_column(1).get<cdk::TYPE_STRING>()
    .m_codec.from_bytes(row->at(1).data(), type);

  return Table(m_schema, Name_src::iterator_get(), type == "VIEW");
}


/*
  Implementation of List_init<> source class taking items from
  query results.
*/


bool Query_src::iterator_next()
{
  assert(m_res);
  m_row = m_res->get_row();
  return nullptr != m_row;
}


mysqlx::string Query_src::iterator_get()
{
  assert(m_row);
  auto *row = static_cast<const Row_data*>(m_row);

  const auto &name_col = m_res->get_column(0);
  const auto &data = row->at(0).data();
  cdk::string name;

  // TDOD: Investigate why we get column type other than STRING.
  // This is related to changed default collation in newer servers and logic
  // we have to interpret BYTES columns as STRING for some collations.
  // TODO: use Value to do the conversion?

  switch (name_col.m_type)
  {
  case cdk::TYPE_STRING:
    m_res->get_column(0).get<cdk::TYPE_STRING>()
      .m_codec.from_bytes(data, name);
    break;

  case cdk::TYPE_BYTES:
    /*
      Even if we see name column reported as raw bytes, we assume it is
      like an utf8 string with null byte appended at the end.
    */
    assert(0 < data.size());
    assert(0 == *(data.end()-1));
    name = std::string(data.begin(), data.end()-1);
    break;

  default:
    assert(false);
  }

  return cdk::foundation::ustring(name);
}

// ---------------------------------------------------------------------
// String conversions.
//
// Note: We use cdk::string to perform required conversion.
//

template <typename C>
inline
std::basic_string<C> to_str(const mysqlx::string &other)
{
  return (std::basic_string<C>)cdk::string(other);
}

template <typename C>
inline
void from_str(mysqlx::string &to, const std::basic_string<C> &from)
{
  to = cdk::string(from);
}


std::string mysqlx::string::Impl::to_utf8(const string &other)
{
  return to_str<char>(other);
}

void mysqlx::string::Impl::from_utf8(string &s, const std::string &other)
{
  from_str(s, other);
}

std::u32string mysqlx::string::Impl::to_ucs4(const string &other)
{
  return to_str<char32_t>(other);
}

void mysqlx::string::Impl::from_ucs4(string &s, const std::u32string &other)
{
  from_str(s, other);
}


std::wstring mysqlx::string::Impl::to_wide(const string &other)
{
  return to_str<wchar_t>(other);
}

void mysqlx::string::Impl::from_wide(string &s, const std::wstring &other)
{
  from_str(s, other);
}


// ---------------------------------------------------------------------

