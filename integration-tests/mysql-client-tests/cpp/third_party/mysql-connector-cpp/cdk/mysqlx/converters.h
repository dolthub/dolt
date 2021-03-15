/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

#ifndef CDK_MYSQLX_CONVERTERS_H
#define CDK_MYSQLX_CONVERTERS_H

#include <mysql/cdk/converters.h>
#include <mysql/cdk/protocol/mysqlx/expr.h>
#include <mysql/cdk/foundation/codec.h>


namespace cdk {
namespace mysqlx {

/*
  Converting expressions and scalar values
  ========================================

  Scalars and expressions known to CDK must be translated to scalars and
  expressions understood by the protocol. In most cases translation
  is trivial, but there are exceptions and if CDK is implemented over
  different protocol then this situation can change.

  Below the following converters are defined:

  Any_converter      - convert values of type Any,
  Any_list_converter - convert lists of Any values,
  Expr_converter      - convert full expressions of type Expression,
  Expr_list_converter - convert lists of expressions,
  Doc_converter       - convert documents with keys mapped to expressions.

  First a set of required processor converters is defined.
*/


struct Scalar_prc_converter
  : public Converter<
      Scalar_prc_converter,
      cdk::Value_processor,
      protocol::mysqlx::api::Scalar_processor
    >
{
  virtual void null() { m_proc->null(); }

  virtual void num(int64_t val)   { m_proc->num(val); }
  virtual void num(uint64_t val)  { m_proc->num(val); }
  virtual void num(float val)     { m_proc->num(val); }
  virtual void num(double val)    { m_proc->num(val); }
  virtual void yesno(bool val)    { m_proc->yesno(val); }

  virtual void str(const string &val)
  {
    std::string utf8(val);
    // TODO: pass CS information
    m_proc->str(utf8);
  }

  virtual void value(Type_info type, const Format_info &fi, bytes data)
  {
    /*
      TODO: Eventually we should Look at type/format info and do
      required conversions. Depending on CDK type and format used
      to represent the source value, presenting it to protocol layer
      might require re-coding it using protocol-specific format.

      At the moment we assume that any scalars passed in encoded
      form use the same encoding that is used by protocol and thus
      we can simply pass the raw bytes without any modifications.
    */
    switch (type)
    {
    case cdk::TYPE_INTEGER:
      {
        cdk::Codec<cdk::TYPE_INTEGER> codec(fi);

        int64_t val;
        codec.from_bytes(data, val);

        m_proc->num(val);
      }
      break;
    case cdk::TYPE_FLOAT:
      {
        cdk::Codec<cdk::TYPE_FLOAT> codec(fi);

        double val;
        codec.from_bytes(data, val);

        m_proc->num(val);
      }
      break;
    case cdk::TYPE_STRING:
      {
        cdk::Codec<cdk::TYPE_STRING> codec(fi);

        string val;
        codec.from_bytes(data, val);

        m_proc->str(bytes(val));
      }
      break;
    case cdk::TYPE_DATETIME:
      {
        //TODO: TYPE_DATETIME
      }
      break;
    case cdk::TYPE_BYTES:
      m_proc->octets(data,
                     cdk::protocol::mysqlx::api::Scalar_processor::CT_PLAIN);
      break;
    case cdk::TYPE_DOCUMENT:
      m_proc->octets(data,
                     cdk::protocol::mysqlx::api::Scalar_processor::CT_JSON);
      break;
    case cdk::TYPE_GEOMETRY:
      m_proc->octets(data, cdk::protocol::mysqlx::api::Scalar_processor::CT_GEOMETRY);
      break;
    case cdk::TYPE_XML:
      m_proc->octets(data, cdk::protocol::mysqlx::api::Scalar_processor::CT_XML);
      break;
    }


  }

};


typedef Any_prc_converter<Scalar_prc_converter> Any_converter_base;

typedef Expr_conv_base<Any_converter_base>  Any_converter;
typedef Expr_conv_base<
          List_prc_converter<Any_converter_base>,
          cdk::Any_list,
          protocol::mysqlx::api::Any_list
        >
        Any_list_converter;


// -------------------------------------------------------------------------


struct Doc_path_storage
  : public cdk::Doc_path_storage
  , public protocol::mysqlx::api::Doc_path
{
  typedef protocol::mysqlx::api::Doc_path Proto_path;
  typedef cdk::Doc_path_storage           Storage;

  // Proto_path interface

  bool is_whole_document() const
  {
    return cdk::Doc_path_storage::is_whole_document();
  }

  unsigned length() const
  {
    size_t len = cdk::Doc_path_storage::length();
    assert(len <= std::numeric_limits<unsigned>::max());
    return (unsigned)len;
  }

  Proto_path::Type get_type(unsigned pos) const
  {
    switch (get_el(pos).m_type)
    {
    case Storage::MEMBER:               return Proto_path::MEMBER;
    case Storage::MEMBER_ASTERISK:      return Proto_path::MEMBER_ASTERISK;
    case Storage::ARRAY_INDEX:          return Proto_path::ARRAY_INDEX;
    case Storage::ARRAY_INDEX_ASTERISK: return Proto_path::ARRAY_INDEX_ASTERISK;
    case Storage::DOUBLE_ASTERISK:      return Proto_path::DOUBLE_ASTERISK;
    }

    // Quiet compile warning.
    assert(false);
    return Proto_path::Type(0);
  }

  const cdk::string* get_name(unsigned pos) const
  {
    const Path_el &el = get_el(pos);
    return Storage::MEMBER == el.m_type ? &el.m_name : NULL;
  }

  const uint32_t* get_index(unsigned pos) const
  {
    const Path_el &el = get_el(pos);
    return Storage::ARRAY_INDEX == el.m_type ? &el.m_idx : NULL;
  }

};


struct Expr_prc_converter_base;

typedef Any_prc_converter<Expr_prc_converter_base>  Expr_prc_converter;
typedef List_prc_converter<Expr_prc_converter>      Expr_list_prc_converter;
typedef Doc_prc_converter<Expr_prc_converter_base>  Doc_converter_base;


struct Expr_prc_converter_base
  : public Converter<
             Expr_prc_converter_base,
             cdk::Expr_processor,
             protocol::mysqlx::api::Expr_processor
           >
  , public protocol::mysqlx::api::Db_obj
{

  typedef Prc_from::Value_prc   Value_prc;
  typedef Prc_to::Value_prc     Value_prc_to;
  typedef Prc_from::Args_prc    Args_prc;
  typedef Prc_to::Args_prc      Args_prc_to;

  //using Base::reset;
  //using Base::m_proc;

  virtual ~Expr_prc_converter_base() {}

  Scalar_prc_converter m_scalar_conv;
  Args_prc* get_args_converter(Args_prc_to*);

  // cdk::Expression::Processor

  Value_prc* val()
  {
    Value_prc_to *sp = m_proc->val();
    if (!sp)
      return NULL;
    m_scalar_conv.reset(*sp);
    return &m_scalar_conv;
  }

  Args_prc* op(const char*);
  Args_prc* call(const api::Object_ref&);

  void ref(const api::Column_ref&, const Doc_path*);

  void ref(const Doc_path &path)
  {
    Doc_path_storage dp;
    path.process(dp);
    m_proc->id(dp);
  }

  void param(const string &name) { m_proc->placeholder(name); }
  void param(uint16_t pos)       { m_proc->placeholder(pos); }
  void var(const string &name) { m_proc->var(name); }

  // Db_obj

  string m_name;
  string m_schema;
  bool   m_has_schema;

  void set_db_obj(const Object_ref &obj) const
  {
    Expr_prc_converter_base *self= const_cast<Expr_prc_converter_base*>(this);
    self->m_name= obj.name();
    if (NULL != obj.schema())
    {
      self->m_schema= obj.schema()->name();
      self->m_has_schema= true;
    }
    else
      self->m_has_schema= false;
  }

  const string& get_name() const { return m_name; }
  const string* get_schema() const { return (m_has_schema ? &m_schema : NULL); }

  scoped_ptr<Expr_list_prc_converter> m_args_converter;
};


typedef Expr_conv_base<
          Expr_prc_converter,
          cdk::Expression,
          protocol::mysqlx::api::Expression
        >
        Expr_converter;

typedef Expr_conv_base<
          List_prc_converter< Expr_prc_converter>,
          cdk::Expr_list,
          protocol::mysqlx::api::Expr_list
        >
        Expr_list_converter;

typedef Expr_conv_base<
          Doc_converter_base,
          cdk::Expression::Document,
          protocol::mysqlx::api::Expression::Document
        >
        Doc_converter;

typedef Expr_conv_base<
          Doc_prc_converter<Scalar_prc_converter>,
          cdk::mysqlx::Param_source,
          protocol::mysqlx::api::Args_map
        >
        Param_converter;

inline
Expr_prc_converter_base::Args_prc*
Expr_prc_converter_base::get_args_converter(Args_prc_to *prc)
{
  if (!prc)
    return NULL;
  if (!m_args_converter)
    m_args_converter.reset(new Expr_list_prc_converter());
  m_args_converter->reset(*prc);
  return m_args_converter.get();
}



inline
Expr_prc_converter_base::Args_prc*
Expr_prc_converter_base::op(const char *op_name)
{
  return get_args_converter(m_proc->op(op_name));
}


inline
Expr_prc_converter_base::Args_prc*
Expr_prc_converter_base::call(const api::Object_ref &func)
{
  set_db_obj(func);
  return get_args_converter(m_proc->call(*this));
}


inline
void
Expr_prc_converter_base::ref(const api::Column_ref &col, const Doc_path *path)
{
  if (col.table())
    set_db_obj(*col.table());
  const protocol::mysqlx::api::Db_obj *table= (col.table() ? this : NULL);
  if (NULL != path)
  {
    Doc_path_storage dp;
    path->process(dp);
    m_proc->id(col.name(), table, dp);
  }
  else
    m_proc->id(col.name(), table);
}


// -------------------------------------------------------------------------

/*
  Class JSON_converter defines conversion from JSON documents to generic
  CDK documents of type Expression::Document. Scalar values inside JSON
  document are represented as literal expressions.
*/

struct JSON_to_expr_prc_converter
  : public Converter<
      JSON_to_expr_prc_converter,
      cdk::JSON_processor,
      cdk::Expr_processor
    >
{
  void null()             { m_proc->val()->null();   }
  void num(int64_t val)   { m_proc->val()->num(val); }
  void num(uint64_t val)  { m_proc->val()->num(val); }
  void num(float val)     { m_proc->val()->num(val); }
  void num(double val)    { m_proc->val()->num(val); }
  void yesno(bool val)    { m_proc->val()->yesno(val); }
  void str(const string &val)
  { m_proc->val()->str(val); }
};


typedef Expr_conv_base<
          Doc_prc_converter<JSON_to_expr_prc_converter>,
          JSON,
          Expression::Document
        >
        JSON_converter;

}} // cdk::mysqlx

#endif
