/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

#ifndef CDK_COMMON_H
#define CDK_COMMON_H

#include "api/mdata.h"
#include "api/obj_ref.h"
#include "api/expression.h"
#include "api/document.h"
#include "api/query.h"


namespace cdk {


/*
  CDK value type system
  =====================

  CDK type system is designed to be extensible. One can add values of any type
  to CDK, as long as these values can be encoded and decoded as raw byte
  sequences. CDK itself does not make any assumptions about which value types
  are supported and what encodings they use.

  All types known to CDK are listed in Type_info enumeration containing
  TYPE_TTT constants for each CDK type TTT. The list of types is given by
  CDK_TYPE_LIST() macro, from which the Type_info enum is generated.

  Given type TTT, an object of type Codec<TTT> can be used to encode and decode
  values of type TTT into raw byte sequences. To create a codec, one needs
  a specification of the encoding format, which is given by an instance of
  Format_info class (this is part of meta-data).

  Given Format_info instance that represents encoding for values of type TTT,
  one can create an object of class Format<TTT> to get information about the
  encoding format. Whether any meaningful encoding information is available
  for a type TTT depends on that type.
*/


#define CDK_TYPE_LIST(X) \
  X(INTEGER) \
  X(FLOAT) \
  X(STRING) \
  X(DATETIME) \
  X(BYTES) \
  X(DOCUMENT) \
  X(GEOMETRY) \
  X(XML)


#undef TYPE_ENUM
#define TYPE_ENUM(X) TYPE_##X,

enum Type_info
{
  CDK_TYPE_LIST(TYPE_ENUM)
};


/*
  Class Format<TTT> gives information about encoding format
  for type TTT. Object of class Codec<TTT> can encode/decode
  values of type TTT into byte sequences using given encoding
  format. See codec.h.
*/

template <Type_info TI>
class Format;

template <Type_info TI>
class Codec;


/**
  Abstract interface used to describe encoding format for values
  of some type. Single Format_info instance can describe encoding
  formats for more than one type.

  Implementation of this interface is returned by Meta_data::format()
  method.
*/

class Format_info
{
public:

  /**
    Informs about types whose encoding formats are described by
    this instance.
  */

  virtual bool for_type(Type_info) const =0;

protected:

  /*
    Methods get_info() should be overwritten by specializations to
    store information about this encoding format in appropriate Format<TTT>
    class. Only methods for types TTT that are compatible with given
    encoding format should be overwritten - others will throw error.
  */

#define TYPE_GET_INFO(X) \
  virtual void get_info(Format<TYPE_##X>&) const \
  { THROW("incompatible data encoding format"); }
#define TYPE_FRIENDS(X) friend class Format<TYPE_##X>;

  CDK_TYPE_LIST(TYPE_GET_INFO)
  CDK_TYPE_LIST(TYPE_FRIENDS)
};


/*
  CDK Traits
  ==========
*/

/*
  Note: These types are compatible with the X protocol.
*/

typedef uint64_t row_count_t;
typedef uint32_t col_count_t;
typedef uint64_t collation_id_t;


class Column_info
    : public api::Column_ref
{
public:

  typedef uint32_t length_t;

  virtual length_t length() const =0;
  virtual length_t decimals() const =0;
  virtual collation_id_t collation() const = 0;
};


struct Traits
{
  typedef cdk::row_count_t row_count_t;
  typedef cdk::col_count_t col_count_t;
  typedef void transaction_id_t;
  typedef const string& savepoint_id_t;

  typedef cdk::Type_info   Type_info;
  typedef const cdk::Format_info& Format_info;
  typedef const cdk::Column_info& Column_info;
};



typedef api::Meta_data<Traits> Meta_data;


/*
  Expressions and documents
  =========================

  Below types for representing scalar values, expressions and documents
  are defined. There are three main classes:

  Any - represents a scalar value such as string or number, or a list of
        Any values or a document with keys mapped to Any values.

  Expression - it is like Any value (either scalar, list or document) in
     which base "scalar" value can be an expressions built using operators,
     named parameters, field references etc.

  JSON - represents a document with the same possible types of key values
     as in documents described by JSON strings.

  There are also tpypes XXX_list to represent lists of object of type XXX
  using the Expr_list<> api (see api/expression.h).
*/

/*
  Any values
  ----------
  Any value can be one of:
  - a string,
  - a number,
  - a Boolean,
  - a value of arbitrary type encoded as raw bytes,
  - an array of Any values,
  - a document with keys mapped to Any values.

  TODO: Add other types required by DevAPI.
*/

class Value_processor;
typedef api::Any<Value_processor> Any;
typedef api::Expr_list<Any>       Any_list;

class Value_processor
{
public:

  virtual void null() =0;

  virtual void value(Type_info, const Format_info&, bytes) =0;
  virtual void str(const string&) =0;
  virtual void num(int64_t) =0;
  virtual void num(uint64_t) =0;
  virtual void num(float) =0;
  virtual void num(double) =0;
  virtual void yesno(bool) =0;
};


template<>
struct Safe_prc<Value_processor>
  : Safe_prc_base<Value_processor>
{
  typedef Safe_prc_base<Value_processor>  Base;
  using Base::Processor;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  void null() { return m_prc ? m_prc->null() : (void)NULL; }

  void value(Type_info ti, const Format_info &fi, bytes data)
  { return m_prc ? m_prc->value(ti, fi, data) : (void)NULL; }

  void str(const string &val)
  { return m_prc ? m_prc->str(val) : (void)NULL; }

  void num(int64_t val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void num(uint64_t val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void num(float val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void num(double val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void yesno(bool val)
  { return m_prc ? m_prc->yesno(val) : (void)NULL; }
};


/*
  Expressions
  -----------
  Base expressions are built from literals, variables, named or positional
  parameters, column or document field references using operators and
  functions. Operators are described by their string names, like "+",
  "&&" or "AND". Functions are references to stored routines present
  in the database or to global functions implemented in the server.

  Expression is either a base expression, or list of expressions ar a
  document with keys mapped to expressions.

  TODO: Define operators and their meaning in protocol independent way.
  TODO: Consider removing variables given that we have named parameters.
*/

class Expr_processor;
typedef api::Any<Expr_processor>    Expression;
typedef api::Expr_list<Expression>  Expr_list;

using api::Doc_path;

class Expr_processor
{
public:

  typedef Value_processor      Value_prc;
  typedef cdk::Expr_list       Expr_list;
  typedef Expr_list::Processor Args_prc;
  typedef cdk::api::Object_ref Object_ref;
  typedef cdk::api::Column_ref Column_ref;
  typedef cdk::Doc_path        Doc_path;
  typedef cdk::string          string;

  /*
    Report expression which is a literal value. The callback should return
    a processor to process the value, or NULL to ignore it.
  */
  virtual Value_prc*  val() =0;

  /*
    Callbacks op() and call() report an operator applied to arguments or
    a function call with arguments. In either case, the callback should
    return a list processor to process argument values (or NULL, if argument
    values are to be ignored).
  */

  virtual Args_prc*   op(const char*) =0;
  virtual Args_prc*   call(const Object_ref&) =0;

  // TODO: consider changing ref() so that they return doc path processor

  virtual void ref(const Column_ref&, const Doc_path*) =0;
  virtual void ref(const Doc_path&) =0;

  virtual void param(const string&) =0;
  virtual void param(uint16_t) =0;
  virtual void var(const string&) =0;
};


template<>
struct Safe_prc<Expr_processor>
  : Safe_prc_base<Expr_processor>
{
  typedef Safe_prc_base<Expr_processor>  Base;
  using Base::Processor;

  typedef Processor::Value_prc   Value_prc;
  typedef Processor::Expr_list   Expr_list;
  typedef Processor::Args_prc    Args_prc;
  typedef Processor::Object_ref  Object_ref;
  typedef Processor::Column_ref  Column_ref;
  typedef Processor::Doc_path    Doc_path;
  typedef Processor::string      string;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  Safe_prc<Value_prc>  val()
  { return m_prc ? m_prc->val() : NULL; }

  Safe_prc<Args_prc>   op(const char *name)
  { return m_prc ? m_prc->op(name) : NULL; }

  Safe_prc<Args_prc>   call(const Object_ref &func)
  { return m_prc ? m_prc->call(func) : NULL; }

  void ref(const Column_ref &col, const Doc_path *path)
  { return m_prc ? m_prc->ref(col, path) : (void)NULL; }

  void ref(const Doc_path &path)
  { return m_prc ? m_prc->ref(path) : (void)NULL; }

  void param(const string &name)
  { return m_prc ? m_prc->param(name) : (void)NULL; }

  void param(uint16_t pos)
  { return m_prc ? m_prc->param(pos) : (void)NULL; }

  void var(const string &name)
  { return m_prc ? m_prc->var(name) : (void)NULL; }
};


/*
  JSON documents
  --------------
  These are key-value maps where key value can be either another
  JSON document or one of these value types:
  - a string,
  - a number,
  - a Boolean,
  - an array of above items.

  TODO: Current implementation has a limitation that arrays can
    contain only scalars and arrays, not documents.
*/

class JSON_processor
{
public:

  virtual void null()             =0;
  virtual void str(const string&) =0;
  virtual void num(uint64_t)      =0;
  virtual void num(int64_t)       =0;
  virtual void num(float)         =0;
  virtual void num(double)        =0;
  virtual void yesno(bool)        =0;

  virtual ~JSON_processor() {}
};

typedef api::Doc_base<JSON_processor> JSON;


template<>
struct Safe_prc<JSON_processor>
  : Safe_prc_base<JSON_processor>
{
  typedef Safe_prc_base<JSON_processor> Base;
  using Base::Processor;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  void null()
  { return m_prc ? m_prc->null() : (void)NULL; }

  void str(const string &val)
  { return m_prc ? m_prc->str(val) : (void)NULL; }

  void num(uint64_t val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void num(int64_t val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void num(float val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void num(double val)
  { return m_prc ? m_prc->num(val) : (void)NULL; }

  void yesno(bool val)
  { return m_prc ? m_prc->yesno(val) : (void)NULL; }
};


/*
  Classes for specyfying projections
  ==================================

  Projection object describes projection to a Proj_processor. The projection
  is a list of items, each item consisting of an expression and optional alias.
*/


typedef api::Projection<Expression> Projection;


template<>
struct Safe_prc<Projection::Processor::Element_prc>
  : Safe_prc_base<Projection::Processor::Element_prc>
{
  typedef Safe_prc_base<Projection::Processor::Element_prc> Base;
  using Base::Processor;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  Expression::Processor* expr()
  { return m_prc ? m_prc->expr() : NULL; }

  void alias(const string &a)
  { return m_prc ? m_prc->alias(a) : (void)NULL; }
};


/*
  Row and Document sources
  ========================

  Operations which insert rows or documents into tables/collections expect
  an object which defies the sequence of rows/documents to be inserted.
  The row/document source objects implement the Iterator interface to move
  through the sequence.

  A row source object defines a sequence of rows, where values of row fields
  are given by a list of expressions. A document source object describes
  each document by a single document expression.
*/

class Row_source
  : public Expr_list
  , public foundation::Iterator
{};


class Doc_source
  : public Expression
  , public foundation::Iterator
{};


/*
  Classes for describing statement parameters
  ===========================================
*/

typedef cdk::api::Limit<row_count_t>         Limit;
typedef cdk::api::Order_by<Expression>       Order_by;
typedef cdk::api::Sort_direction             Sort_direction;
typedef cdk::api::Doc_base<Value_processor>  Param_source;
typedef cdk::api::Lock_mode::value           Lock_mode_value;
typedef cdk::api::Lock_contention::value     Lock_contention_value;

using   cdk::api::View_security;
using   cdk::api::View_algorithm;
using   cdk::api::View_check;

typedef cdk::api::View_options               View_options;
typedef cdk::api::View_spec<View_options>    View_spec;

/*
  Classes for describing update operations
  ========================================
*/


class Update_processor
{
public:

  enum {
    NO_OVERWRITE = 0x1,
    NO_INSERT    = 0x2,
  };

  typedef Expression::Processor  Expr_prc;

  // TODO: update docs

  /*
    Specify column whose value is changed by the update operation.

    This callback should be called when updating rows in a table - in this
    case it specifies which field of the row should be updated. If update
    operations specify a path, then it is assumed that the field contains a
    document.

    In case of updating document in a collection, the update operation acts
    on the whole document and column should be not specified - if column()
    is called in this context then we have malformed update operation.
  */

  virtual void column(const api::Column_ref&) =0;

  /*
    Update operation which removes document element specified by a path.
    If `path` is NULL then whole document is removed. In table mode the
    corresponding field in a row becomes NULL.
  */

  virtual void remove(const Doc_path *path) =0;

  /*
    Set value of element given by the path to the value given by
    the expression.

    If path is NULL then document is replaced by the value of the expression.
    In table mode, value of the field is set to the value of the expression.

    Normally, values of existing elements are overwritten by the new value
    and if path specifies non-existing element then new element is added. This
    default behavior can be changed by specifying NO_OVERWRITE and NO_INSERT
    flags.
  */

  virtual Expr_prc* set(const Doc_path*, unsigned flags =0) =0;

  /*
    Insert value into array at position specified by the path.

    The path should point at a position within array element in the document.
    The value is inserted at that position and existing values at and after this
    position are moved to the right.
  */

  virtual Expr_prc* array_insert(const Doc_path*) =0;

  /*
    Append value to an array.

    The path should specify an array element in the document. The value is appended
    at the end of the array.
  */

  virtual Expr_prc* array_append(const Doc_path*) =0;

  /*
    Perform MERGE_PATCH operation on a document.
  */
  virtual Expr_prc* patch() =0;

};


/*
  Update operation describes itself to an Update_processor.
*/

typedef api::Expr_base<Update_processor>  Update_op;


/*
  Update specification is a sequence of update operations.
*/

class Update_spec
  : public Update_op
  , public foundation::Iterator
{};


template<>
struct Safe_prc<Update_processor>
  : Safe_prc_base<Update_processor>
{
  typedef Safe_prc_base<Update_processor>  Base;
  using Base::Processor;

  typedef Processor::Expr_prc  Expr_prc;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  void column(const api::Column_ref &col)
  { return m_prc ? m_prc->column(col) : (void)NULL; }

  void remove(const Doc_path *path)
  { return m_prc ? m_prc->remove(path) : (void)NULL; }

  Safe_prc<Expr_prc> set(const Doc_path *path, unsigned flags =0)
  { return m_prc ? m_prc->set(path, flags) : NULL; }

  Safe_prc<Expr_prc> array_insert(const Doc_path *path)
  { return m_prc ? m_prc->array_insert(path) : NULL; }

  Safe_prc<Expr_prc> array_append(const Doc_path *path)
  { return m_prc ? m_prc->array_append(path) : NULL; }

  Safe_prc<Expr_prc> patch()
  { return m_prc ? m_prc->patch() : NULL; }
};

}  // cdk

#endif
