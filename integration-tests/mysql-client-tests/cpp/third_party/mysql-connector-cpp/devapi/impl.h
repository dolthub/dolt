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

#ifndef MYSQLX_IMPL_H
#define MYSQLX_IMPL_H

/*
  Internal implementations for public DevAPI classes.
*/

#include <mysqlx/xdevapi.h>

#include <mysql/cdk.h>
#include <mysql/cdk/converters.h>
#include <expr_parser.h>

#include "../common/result.h"
#include "../common/op_impl.h"

//#include "result_impl.h"

PUSH_SYS_WARNINGS
#include <map>
#include <memory>
#include <stack>
#include <list>
POP_SYS_WARNINGS


namespace mysqlx {


struct Value::Access
{

  /*
    Check if Value is an expression
  */

  static bool is_expr(const Value &val)
  {
    return val.is_expr();
  }

  /*
    Build document value from a JSON string which is
    assumed to describe a document.
  */

  static Value mk_doc(const string &json)
  {
    Value ret;
    ret.m_type = Value::DOC;
    ret.m_doc = DbDoc(json);
    return ret;
  }

  /*
    Build value after parsing given JSON string. Depending
    on the string, the value can be a document, array or
    scalar.
  */

  static Value mk_from_json(const std::string &json);

  template <cdk::Type_info T>
  static Value mk(cdk::bytes data, impl::common::Format_descr<T> &fmt)
  {
    return common::Value::Access::mk(data, fmt);
  }

  static Value mk(cdk::bytes data, impl::common::Format_descr<cdk::TYPE_DOCUMENT>&);

  static cdk::string cdk_str(const Value &val)
  {
    return val.m_ustr;
  }

  static void process(
    parser::Parser_mode::value, const Value&, cdk::Expression::Processor&
  );

};


/*
  This class presents Value object as cdk::Expression, like common::Value_expr
  does for common::Value objects. It differs from common::Value_expr in that it
  supports array and document values.
*/


class Value_expr
  : public cdk::Expression
{
  const Value &m_val;
  parser::Parser_mode::value m_pm;

public:

  Value_expr(const Value &val,
             parser::Parser_mode::value parser_mode)
    : m_val(val), m_pm(parser_mode)
  {}

  void process_if(Processor *prc) const
  {
    if (!prc)
      return;
    process(*prc);
  }

  void process(Processor &prc) const override
  {
    Value::Access::process(m_pm, static_cast<const Value&>(m_val), prc);
  }

};


inline
void Value::Access::process(
  parser::Parser_mode::value pm,
  const Value &val,
  cdk::Expression::Processor &prc
)
{
  using Processor = cdk::Expression::Processor;

  // Handle non scalar values

  switch (val.m_type)
  {
  case Value::DOC:
    {
      mysqlx::DbDoc &doc = const_cast<Value&>(val).m_doc;

#if 0
      /*
        To avoid costly parsing, if document has internal JSON representation
        simply report this document as a JSON string.

        TODO:
        Test Types.joson fails when inserting ARRAY value with JSON based DbDoc
        element. In that case, what we get in the end in the collection table
        is an array where the document is treated as a string: we get value
        like this:

          [ ..., "{...}", ...]

        instead of a value like this:

          [ ..., {...}, ... ]

        Using json_unquote(json) to report the document does not help.
        Currently only parsing the json string and reporting as protocol OBJECT
        works as expected.
      */

      const char *json = doc.get_json();
      if (json)
      {
        common::Object_ref f("json_unquote");

        auto argsprc = safe_prc(prc)->scalar()->call(f);
        if (argsprc)
        {
          argsprc->list_begin();
          argsprc->list_el()->scalar()->val()->str(json);
          argsprc->list_end();
        }
        return;
      }
#endif

      Processor::Doc_prc *dprc = safe_prc(prc)->doc();
      if (!dprc)
        return;
      dprc->doc_begin();
      for (Field fld : doc)
      {
        // TODO: do we need to force interpreting field values as expressions?
        auto fprc = dprc->key_val((string)fld);
        if (fprc)
          process(pm, doc[fld], *fprc);
      }
      dprc->doc_end();
    }
    return;

  case Value::ARR:
    {
      Processor::List_prc *lpr = safe_prc(prc)->arr();
      if (!lpr)
        return;
      lpr->list_begin();
      for (Value el : *val.m_arr)
      {
        auto elprc = lpr->list_el();
        if (elprc)
          process(pm, el, *elprc);
      }
      lpr->list_end();
    }
    return;

  default:
    common::Value::Access::process(pm, val, prc);
    return;
  }
}


// --------------------------------------------------------------------

/*
  These wrappres are just a convenience to be able to construct
  a Schema_ref/Object_ref instance directly from Schema/Collection/Table one.
*/

struct Schema_ref : public impl::common::Schema_ref
{
  using impl::common::Schema_ref::Schema_ref;

  Schema_ref(const Schema &sch)
    : Schema_ref(sch.getName())
  {}
};


struct Object_ref : public impl::common::Object_ref
{
  using impl::common::Object_ref::Object_ref;

  // TODO: Collection/Table without explicit schema?

  Object_ref(const Collection &coll)
    : Object_ref(coll.getSchema().getName(), coll.getName())
  {}

  Object_ref(const Table &tbl)
    : Object_ref(tbl.getSchema().getName(), tbl.getName())
  {}
};


// --------------------------------------------------------------------


/*
  DbDoc implementation which stores document data in std::map.
*/

class DbDoc::Impl
{
  class JSONDoc;

  std::string m_json;

  virtual void print(std::ostream &out) const
  {
    out << get_json();
  }

  virtual void prepare() {} // GCOV_EXCL_LINE

  // Data storage

  typedef std::map<Field, Value> Map;
  Map m_map;

  bool has_field(const Field &fld)
  {
    prepare();
    return m_map.end() != m_map.find(fld);
  }

  const Value& get(const Field &fld) const
  {
    const_cast<Impl*>(this)->prepare();
    return m_map.at(fld);
  }

  virtual const char* get_json() const
  {
    if(!m_json.empty())
      return m_json.c_str();

    auto *self = const_cast<DbDoc::Impl*>(this);

    self->m_json.append("{");
    bool first = true;
    for (auto el : m_map)
    {
      if (!first)
      {
        self->m_json.append(", ");
      }
      else
      {
        first = false;
      }
      self->m_json.append(R"(")").append(el.first).append(R"(": )");

      switch (el.second.get_type())
      {
      case common::Value::STRING:
      case common::Value::USTRING:
      case common::Value::EXPR:
        self->m_json
            .append(R"(")")
            .append(el.second.get<std::string>())
            .append(R"(")");
        break;
      default:
        std::stringstream value_ss;
        value_ss << el.second;
        self->m_json.append(value_ss.str());
        break;
      }
    }
    self->m_json.append("}");

    return m_json.c_str();
  }

  // Iterating over fields of the document

  Map::iterator m_it;

  void reset() { prepare(); m_it = m_map.begin(); }

  const Field& get_current_fld() { return m_it->first; }
  void next() { ++m_it; }
  bool at_end() const { return m_it == m_map.end(); }

  struct Builder;

  friend DocResult;
  friend DbDoc;
  friend RowResult;
  friend Value::Access;

  public:
  virtual ~Impl(){}
};


/*
  DbDoc::Impl specialization which takes document data from
  a JSON string.
*/

class DbDoc::Impl::JSONDoc
  : public DbDoc::Impl
{

  bool m_parsed;

public:

  JSONDoc(const std::string &json)
    : m_parsed(false)
  {
    m_json = json;
  }

  JSONDoc(std::string &&json)
    : m_parsed(false)
  {
    m_json = std::move(json);
  }

  void prepare();

  void print(std::ostream &out) const
  {
    out << get_json();
  }

  const char* get_json() const
  {
    return m_json.c_str();
  }
};


// --------------------------------------------------------------------


/*
  Implementation for a single Row instance.

  The common::Row_impl<> template is used with mysqlx::Value class used for
  internal storage of row field values.
*/

class internal::Row_detail::Impl
  : public impl::common::Row_impl<Value>
{
public:

  using impl::common::Row_impl<Value>::Row_impl;

  friend Row;
  friend Row_detail;
  friend RowResult;
  friend SqlResult;
};


/*
  Internal implementation for Client objects.

  The common::Session_pool class is used.
*/

struct internal::Client_detail::Impl
  : public impl::common::Session_pool
{
  using impl::common::Session_pool::Session_pool;
};


/*
  Internal implementation for Session objects.

  The common::Session_impl class is used.
*/

struct internal::Session_detail::Impl
  : public impl::common::Session_impl
{
  using Result_impl = internal::Result_detail::Impl;

  using impl::common::Session_impl::Session_impl;
};


}  // mysqlx


#ifndef THROW_AS_ASSERT
#undef THROW
#define THROW(MSG) do { mysqlx::throw_error(MSG); throw (MSG); } while(false)
#endif

#endif
