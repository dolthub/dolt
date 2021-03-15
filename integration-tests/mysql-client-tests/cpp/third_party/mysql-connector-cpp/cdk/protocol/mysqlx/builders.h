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

#ifndef PROTOCOL_MYSQLX_BUILDERS_H
#define PROTOCOL_MYSQLX_BUILDERS_H

#include <mysql/cdk/protocol/mysqlx/expr.h>


namespace cdk {
namespace protocol {
namespace mysqlx {


/*
  Message builders
  ================
  A message builder class acts as expression processor and builds protobuf
  message which represents given expression. Different types of protobuf
  messages are built from different types of expressions - we have a
  builder for Mysqlx::Datatypes::Any from Any value as well as a builder
  for Mysqlx::Expr::Expr from a full Expression. However, both builders
  are generated from the same templates with help of Msg_traits template
  which defines specifics of each protobuf message type.
*/


/*
  Protobuf message traits
  -----------------------
  For one of the supported protobuf message types MSG, there are
  XXX_msg_traits<MSG> structures which define how to store different
  kinds of values in a message of type MSG.

  Scalar_msg_traits<MSG> - for a message that can store plain scalars,
    defines a static function:

    Mysqlx::Datatypes::Scalar& get_scalar(MSG &msg, Scalar::Type type)

    which returns a reference to a sub-messgae of type
    Mysqlx::Datatypes::Scalar inside msg, where scalar value can be stored.
    If needed, msg is marked to indicate that it stores a scalar value.
    The `type` field within returned sub-message is initialized to given type.

  Arr_msg_traits<MSG> - for a message that can store sequence of values.
    This structure defines a sub-message type Msg and a static function:

    Msg& add_element(MSG &arr)

    which adds a new element to the array and returns reference to the new
    sub-message where the next value in the sequence can be stored.

  Obj_msg_tratis<MSG> - for a message that can store objects, that is
    key-value maps. This structure defines sub-message type Msg and
    a static function:

    Msg& add_key(MSG &obj, const string &key)

    which adds a new key-value pair to the object and returns reference to
    the sub-message where the key value can be stored.

  Any_msg_traits<MSG> - for messages that can store three kinds of values:
    scalars in Mysqx::Datatypes::Scalar sub-message, arrays or documents.
    The structure defines sub-message types Scalar, Array and Object for
    storing corresponding types of values and functions:

    Scalar& get_scalar(MSG &msg)  - return a sub-message where scalar value
                                    can be stored.

    Array& get_array(MSG &msg)    - return a sub-message where array of values
                                    can be stored.

    Object& get_object(MSG &msg)  - return a sub-message where a key-value
                                    map can be stored.

    These functions mark the message to indicate what kind of value it
    stores.

*/

template <class MSG> struct Scalar_msg_traits;
template <class MSG> struct Arr_msg_traits;
template <class MSG> struct Obj_msg_traits;
template <class MSG> struct Any_msg_traits;


template <class MSG, class ARR, class OBJ>
struct Msg_traits_common
{
  typedef Mysqlx::Datatypes::Scalar Scalar;
  typedef ARR  Array;
  typedef OBJ  Object;
  typedef MSG  Msg;

  static Msg& add_element(Array &arr)
  {
    return *arr.add_value();
  }

  static Msg& add_key(Object &obj, const std::string &key)
  {
    typename Object::ObjectField *fld = obj.add_fld();
    fld->set_key(key);
    return *fld->mutable_value();
  }
};


// Common message traits for messages in Mysqlx::Datatypes namespace.

struct Data_msg_traits
  : public Msg_traits_common<
      Mysqlx::Datatypes::Any,
      Mysqlx::Datatypes::Array,
      Mysqlx::Datatypes::Object
    >
{
  static Scalar& get_scalar(Msg &msg)
  {
    msg.set_type(Msg::SCALAR);
    Scalar *s = msg.mutable_scalar();
    return *s;
  }

  static Scalar& get_scalar(Scalar &msg, Scalar::Type type)
  {
    msg.set_type(type);
    return msg;
  }

  static Array& get_array(Msg &msg)
  {
    msg.set_type(Msg::ARRAY);
    return *msg.mutable_array();
  }

  static Object& get_object(Msg &msg)
  {
    msg.set_type(Msg::OBJECT);
    return *msg.mutable_obj();
  }

};

template<>
struct Scalar_msg_traits<Mysqlx::Datatypes::Scalar> : public Data_msg_traits
{};

template<>
struct Arr_msg_traits<Mysqlx::Datatypes::Array> : public Data_msg_traits
{};

template<>
struct Obj_msg_traits<Mysqlx::Datatypes::Object> : public Data_msg_traits
{};

template<>
struct Any_msg_traits<Mysqlx::Datatypes::Any> : public Data_msg_traits
{};


// Common message traits for messages in Mysqlx::Expr namespace.

struct Expr_msg_traits
  : public Msg_traits_common<
      Mysqlx::Expr::Expr,
      Mysqlx::Expr::Array,
      Mysqlx::Expr::Object
    >
{
  static Msg& get_scalar(Msg &msg)
  {
    msg.set_type(Msg::LITERAL);
    return msg;
  }

  static Scalar& get_scalar(Msg &msg, Scalar::Type type)
  {
    get_scalar(msg);
    Scalar *s = msg.mutable_literal();
    s->set_type(type);
    return *s;
  }

  static Array& get_array(Msg &msg)
  {
    msg.set_type(Msg::ARRAY);
    return *msg.mutable_array();
  }

  static Object& get_object(Msg &msg)
  {
    msg.set_type(Msg::OBJECT);
    return *msg.mutable_object();
  }
};


template<>
struct Scalar_msg_traits<Mysqlx::Expr::Expr> : public Expr_msg_traits
{};

template<>
struct Arr_msg_traits<Mysqlx::Expr::Array> : public Expr_msg_traits
{};

template<>
struct Obj_msg_traits<Mysqlx::Expr::Object> : public Expr_msg_traits
{};

template<>
struct Any_msg_traits<Mysqlx::Expr::Expr> : public Expr_msg_traits
{};


// -----------------------------------------------------------------------

/*
  Common base for message builders which defines m_msg member that stores
  pointer to the message which is being constructed. Message builder object
  is an expression processor and should be used as follows:

    bld.reset(msg,conv);
    expr.process(bld);

  where bld is a message builder instance, msg is a protobuf message and
  expr is an expression.

  conv is a converter implementing Args_conv interface that is used to map named
  placeholders to positional ones. Before used in a builder, such converter must
  be initialized first, as is done in set_args() method.

*/

class Args_conv
{
public:
  virtual unsigned conv_placeholder(const string &) = 0;
};

template <class MSG, class PRC>
struct Builder_base
  : public PRC
  , cdk::foundation::nocopy
{
  typedef MSG Message;
  typedef PRC Processor;

  Message *m_msg;
  Args_conv *m_args_conv;

  Builder_base() : m_msg(NULL), m_args_conv(NULL)
  {}

  void reset(Message &msg, Args_conv *conv = NULL)
  {
    m_msg = &msg;
    m_args_conv = conv;
  }

  virtual ~Builder_base() {}
};


// -----------------------------------------------------------------------

/*
  Builder templates
  =================

  Templates defined below construct array or object builders from base
  builders for plain values.
*/

/*
  Array_builder<BLD, MSG> template defines a message builder which builds
  an array message of type MSG from a list of expression.
  Base builder of type BLD is used to build each expression in
  the list. Structure Arr_msg_traits<MSG> must be defined for message type MSG
  and the base builder must build sub-messages of the type defined by these
  traits. Alternative traits can be specified if needed.
*/

template <class BLD, class MSG, class Traits = Arr_msg_traits<MSG> >
class Array_builder
  : public Builder_base<
             MSG,
             cdk::api::List_processor<typename BLD::Processor>
           >
{
  typedef Builder_base<
             MSG,
             cdk::api::List_processor<typename BLD::Processor>
           >  Base;

  using Base::m_msg;
  using Base::m_args_conv;

public:

  typedef typename Base::Message   Message;
  typedef typename Base::Processor Processor;
  typedef typename Processor::Element_prc  Element_prc;

protected:

  Element_prc* list_el()
  {
    BLD *bld = get_el_builder();
    bld->reset(Traits::add_element(*m_msg),
               this->m_args_conv);
    return bld;
  }

private:

  scoped_ptr<BLD>   m_el_builder;

public:

  BLD* get_el_builder()
  {
    if (!m_el_builder)
      m_el_builder.reset(new BLD());
    return m_el_builder.get();
  }

};


/*
  Any_builder_base<BLD, MSG> template defines a message builder which builds
  a message that can store plain expression or array of expressions or a
  document, from an Any value that can be one of these things.

  Base builder of type BLD is used to build sub-messages from base (scalar)
  values. Structure Any_msg_traits<MSG> must be defined for message type MSG
  and the base builder must build sub-messages of the Scalar type defined by
  these traits. Builders for arrays and documents are generated from the
  base builder.
*/

template <class BLD, class MSG, class Traits = Obj_msg_traits<MSG> >
class Doc_builder_base;

template <class BLD, class MSG, class Traits = Any_msg_traits<MSG> >
class Any_builder_base
  : public Builder_base<
             MSG,
             cdk::api::Any_processor<typename BLD::Processor>
           >
{
  typedef Builder_base<
            MSG,
            cdk::api::Any_processor<typename BLD::Processor>
          >  Base;

  using Base::m_msg;
  using Base::m_args_conv;

public:

  typedef typename Base::Message   Message;
  typedef typename Base::Processor Processor;

protected:

  typedef typename Traits::Object  Object;
  typedef typename Traits::Array   Array;
  typedef Doc_builder_base<BLD, Object>           Obj_builder;
  typedef Array_builder<Any_builder_base, Array>  Arr_builder;

  typedef typename Processor::Scalar_prc  Scalar_prc;
  typedef typename Processor::Doc_prc     Doc_prc;
  typedef typename Processor::List_prc    List_prc;

  Scalar_prc* scalar()
  {
    BLD *sb = get_scalar_builder();
    sb->reset(Traits::get_scalar(*m_msg),
              this->m_args_conv);
    return sb;
  }

  Doc_prc* doc()
  {
    Obj_builder *ob = get_obj_builder();
    ob->reset(Traits::get_object(*m_msg),
              this->m_args_conv);
    return ob;
  }

  List_prc* arr()
  {
    Arr_builder *ab = get_arr_builder();
    ab->reset(Traits::get_array(*m_msg),
              this->m_args_conv);
    return ab;
  }

private:

  BLD m_scalar_builder;
  Arr_builder m_arr_builder;
  scoped_ptr<Obj_builder> m_obj_builder;

public:

  BLD* get_scalar_builder()
  { return &m_scalar_builder; }

  Arr_builder* get_arr_builder()
  { return &m_arr_builder; }

  Obj_builder* get_obj_builder()
  {
    if (!m_obj_builder)
      m_obj_builder.reset(new Obj_builder());
    return m_obj_builder.get();
  }

};


/*
  Doc_builder_base<BLD, MSG> template defines a message builder which builds
  an object message from a document expression.

  Base builder of type BLD is used to build sub-messages from base (scalar)
  values. Structure Obj_msg_traits<MSG> must be defined for message type MSG
  and the base builder must build sub-messages of the Scalar type defined by
  these traits.

  In the object keys are mapped to any values which can be either base values,
  arrays or sub-documents. The base builder is used only for base values -
  builders for arrays and sub-documents are generated from it.
*/

template <class BLD, class MSG, class Traits> // = Obj_msg_traits<typename BLD::Message> >
class Doc_builder_base
  : public Builder_base<
             MSG,
             cdk::api::Doc_processor< typename BLD::Processor >
           >
{
  typedef Builder_base<
            MSG,
            cdk::api::Doc_processor< typename BLD::Processor >
          >  Base;

  using Base::m_msg;
  using Base::m_args_conv;

public:

  typedef typename Base::Message   Message;
  typedef typename Base::Processor Processor;
  typedef typename Traits::Array   Array;
  typedef typename Traits::Msg     Any;

protected:

  typedef Any_builder_base<BLD, Any>    Any_builder;
  using typename Processor::Any_prc;


  Any_prc* key_val(const string &key)
  {
    Any &val = Traits::add_key(*m_msg,key);
    Any_builder *ab = get_any_builder();
    ab->reset(val, this->m_args_conv);
    return ab;
  }

private:

  Any_builder m_any_builder;

public:

  Any_builder* get_any_builder()
  { return &m_any_builder; }

};


// ----------------------------------------------------------------------


/*
  Scalar and expression builders
  ==============================

  The following builders are defined below using the generic templates:

  Scalar_builder - build Mysqlx::Datatypes::Scalar message from Any::Scalar
                   expression.

  Any_builder    - build Mysqlx::Datatypes::Any message from Any expression.

  Expr_builder   - build Mysqlx::Expr::Expr message from full expression of
                   type Expression.

  Build scalar value given by Any::Processor callbacks and store it in
  protobuf message of type MSG. Msg_traits<MSG>.get_scalar() defines
  where the scalar is stored within the message.
*/


/*
  This scalar builder is used to build either Mysqlx::Datatypes::Scalar
  or Mysqlx::Expr::Expr messages. Both types of messages can store plain
  scalar values.
*/

template <class MSG>
class Scalar_builder_base
  : public Builder_base<MSG, api::Scalar_processor>
{
  typedef Builder_base<MSG, api::Scalar_processor>  Base;

  using Base::m_msg;

  typedef api::Scalar_processor::Octets_content_type Octets_content_type;

protected:

  typedef Mysqlx::Datatypes::Scalar Scalar;
  typedef Scalar::String            String;

  Scalar& get_scalar(Scalar::Type type)
  {
    return Scalar_msg_traits<MSG>::get_scalar(*m_msg, type);
  }

  String& get_string()
  {
    Scalar &sc= get_scalar(Scalar::V_STRING);
    return *sc.mutable_v_string();
  }

protected:

  void null();
  void str(bytes val);
  void str(collation_id_t cs, bytes val);
  void num(int64_t val);
  void num(uint64_t val);
  void num(float val);
  void num(double val);
  void yesno(bool val);
  void octets(bytes val, Octets_content_type type);
};


typedef Scalar_builder_base<Mysqlx::Datatypes::Scalar>  Scalar_builder;

class Any_builder :
  public Any_builder_base<Scalar_builder, Mysqlx::Datatypes::Any>
{
public:

  Any_builder()
  {}

  Any_builder(Mysqlx::Datatypes::Any &msg, Args_conv *conv)
  {
    reset(msg, conv);
  }
};


/*
  Builder for base expressions. Below it is extended to full expressions
  using Any_builder_base<> template.
*/

class Expr_builder_base
  : public Builder_base<Mysqlx::Expr::Expr, api::Expr_processor>
{
public:

  typedef Mysqlx::Expr::Expr Expr;
  typedef Any_builder_base<Expr_builder_base, Expr> Expr_builder;

  using Builder_base<Mysqlx::Expr::Expr, api::Expr_processor>::m_args_conv;

protected:

  Scalar_builder_base<Expr>   m_scalar_builder;
  scoped_ptr<Args_prc> m_args_builder;

  template <class MSG>
  Args_prc* get_args_builder(MSG&);

  Value_prc* val() override
  {
    m_scalar_builder.reset(Expr_msg_traits::get_scalar(*m_msg), this->m_args_conv);
    return &m_scalar_builder;
  }

  Mysqlx::Expr::Operator &set_op(const char *name);
  Args_prc* op(const char *name) override;

  Mysqlx::Expr::FunctionCall &set_call(const api::Db_obj& db_obj);
  Args_prc* call(const api::Db_obj& db_obj) override;

  void var(const string &name)override;
  void id(const string &name, const api::Db_obj *coll) override;
  void id(const string &name, const api::Db_obj *coll,
                  const api::Doc_path &path) override;
  void id(const api::Doc_path &path) override;

  void placeholder() override;
  void placeholder(const string &name) override;
  void placeholder(unsigned pos) override;

};


class Expr_builder
  : public Any_builder_base<Expr_builder_base, Mysqlx::Expr::Expr>
{

public:

  Expr_builder()
  {}

  Expr_builder(Mysqlx::Expr::Expr &msg, Args_conv *conv = NULL)
  {
    reset(msg, conv);
  }

protected:

};


/*
  Builder for base expressions on having statments. Below it is extended to full
  having expressions using Any_builder_base<> template.
*/

class Having_builder_base
  : public Expr_builder_base
{

  bool m_first_id = true;

protected:

  template <class MSG>
  Args_prc* get_args_builder(MSG&);

  Args_prc* op(const char *name) override;
  Args_prc* call(const api::Db_obj& db_obj) override;

//  void id(const string &name, const api::Db_obj *coll) override;
  void id(const string &name, const api::Db_obj *coll,
                  const api::Doc_path &path) override;
  void id(const api::Doc_path &path) override;

};


class Having_builder
  : public Any_builder_base<Having_builder_base, Mysqlx::Expr::Expr>
{

public:

  Having_builder()
  {}

  Having_builder(Mysqlx::Expr::Expr &msg, Args_conv *conv = NULL)
  {
    reset(msg, conv);
  }

protected:

};


/*
  Builder used to store operator or function call arguments inside
  a sub-message of Expr message.
  BUILDER is a builder class to be used to construct individual arguments in the
  list, for example Expr_builder.
*/

template <class MSG, class BUILDER>
struct Args_builder
  : public Builder_base<MSG, api::Expr_list::Processor>
{
  typedef Builder_base<MSG, api::Expr_list::Processor>  Base;

  using Base::m_msg;
  using Base::m_args_conv;
  using Base::reset;

  BUILDER m_arg_builder;

  Args_builder(MSG &msg, Args_conv *conv = NULL)
  {
    reset(msg, conv);
  }

  using typename Builder_base<MSG, api::Expr_list::Processor>::Element_prc;

  Element_prc* list_el()
  {
    m_arg_builder.reset(*m_msg->add_param(), this->m_args_conv);
    return &m_arg_builder;
  }
};


template <class MSG>
inline
Expr_builder_base::Args_prc*
Expr_builder_base::get_args_builder(MSG &msg)
{

  m_args_builder.reset(new Args_builder<MSG,Expr_builder>(msg, this->m_args_conv));
  return m_args_builder.get();
}


// ---------------------------------------------------------------------

/*
  Scalar builder implementation
  =============================
*/

template <class MSG>
inline
void Scalar_builder_base<MSG>::null()
{
  get_scalar(Scalar::V_NULL);
}

template <class MSG>
inline
void Scalar_builder_base<MSG>::str(bytes val)
{
  //TODO: Default charset handling - must be clarified in protocol specs.
  get_string().set_value(val.begin(), val.size());
}

template <class MSG>
inline
void Scalar_builder_base<MSG>::str(collation_id_t cs, bytes val)
{
  String &str= get_string();
  str.set_collation(cs);
  str.set_value(val.begin(), val.size());
}

template <class MSG>
inline
void Scalar_builder_base<MSG>::num(int64_t val)
{
  get_scalar(Scalar::V_SINT).set_v_signed_int(val);
}

template <class MSG>
inline
void Scalar_builder_base<MSG>::num(uint64_t val)
{
  get_scalar(Scalar::V_UINT).set_v_unsigned_int(val);
}

template <class MSG>
inline
void Scalar_builder_base<MSG>::num(float val)
{
  get_scalar(Scalar::V_FLOAT).set_v_float(val);
}

template <class MSG>
inline
void Scalar_builder_base<MSG>::num(double val)
{
  get_scalar(Scalar::V_DOUBLE).set_v_double(val);
}

template <class MSG>
inline
void Scalar_builder_base<MSG>::yesno(bool val)
{
  get_scalar(Scalar::V_BOOL).set_v_bool(val);
}



template <class MSG>
inline
void Scalar_builder_base<MSG>::octets(bytes val, Octets_content_type type)
{
  ::Mysqlx::Datatypes::Scalar_Octets *octets =
      get_scalar(Scalar::V_OCTETS).mutable_v_octets();
  octets->set_value(val.begin(), val.size());
  octets->set_content_type(type);
}


// ---------------------------------------------------------------------

/*
  Plain expression builder implementation
  =======================================
*/

inline
Mysqlx::Expr::Operator&
Expr_builder_base::set_op(const char *name)
{
  m_msg->set_type(Expr::OPERATOR);
  Mysqlx::Expr::Operator *op = m_msg->mutable_operator_();
  op->set_name(name);
  return *op;
}

inline
Expr_builder_base::Args_prc*
Expr_builder_base::op(const char *name)
{
  return get_args_builder(set_op(name));
}


/*
  Callback for FUNC_CALL expression type
*/

inline
Mysqlx::Expr::FunctionCall &
Expr_builder_base::set_call(const api::Db_obj& db_obj)
{
  m_msg->set_type(Expr::FUNC_CALL);
  Mysqlx::Expr::FunctionCall *fc = m_msg->mutable_function_call();
  Mysqlx::Expr::Identifier *id = fc->mutable_name();

  id->set_name(db_obj.get_name());
  const string *schema = db_obj.get_schema();
  if (schema)
    id->set_schema_name(*schema);
  return *fc;
}

inline
Expr_builder_base::Args_prc*
Expr_builder_base::call(const api::Db_obj& db_obj)
{
  return get_args_builder(set_call(db_obj));
}


inline
void Expr_builder_base::var(const string &name)
{
  m_msg->set_type(Mysqlx::Expr::Expr_Type_VARIABLE);
  m_msg->set_variable(name);
}


/*
  Callback for IDENT expression type
*/
inline
void Expr_builder_base::id(const string &name, const api::Db_obj *db_obj)
{
  m_msg->set_type(Expr::IDENT);
  Mysqlx::Expr::ColumnIdentifier *p_col_id = m_msg->mutable_identifier();
  p_col_id->set_name(name);

  if (!db_obj)
    return;

  p_col_id->set_table_name(db_obj->get_name());

  const string *schema= db_obj->get_schema();

  if (!schema)
    return;

  p_col_id->set_schema_name(*schema);
}


/*
  Callback for IDENT expression type with only Doc_path parameter
*/
inline
void Expr_builder_base::id(const api::Doc_path &doc)
{
  m_msg->set_type(Expr::IDENT);

  Mysqlx::Expr::ColumnIdentifier *p_col_id = NULL;

  if (doc.is_whole_document())
  {
    // The path "$" is represented as a member without name
    if (!p_col_id)
      p_col_id = m_msg->mutable_identifier();

    Mysqlx::Expr::DocumentPathItem *dpi = p_col_id->add_document_path();
    dpi->set_type(Mysqlx::Expr::DocumentPathItem::MEMBER);
    return;
  }


  for (unsigned pos = 0; pos < doc.length(); ++pos)
  {
    if (!p_col_id)
      p_col_id = m_msg->mutable_identifier();

    Mysqlx::Expr::DocumentPathItem *dpi = p_col_id->add_document_path();
    dpi->set_type(static_cast<Mysqlx::Expr::DocumentPathItem_Type>(doc.get_type(pos)));


    switch (doc.get_type(pos))
    {
    case api::Doc_path::MEMBER:
      if (doc.get_name(pos))
        dpi->set_value(*doc.get_name(pos));
      break;

    case api::Doc_path::ARRAY_INDEX:
      if (doc.get_index(pos))
        dpi->set_index(*doc.get_index(pos));
      break;

    case api::Doc_path::DOUBLE_ASTERISK:
    case api::Doc_path::ARRAY_INDEX_ASTERISK:
    case api::Doc_path::MEMBER_ASTERISK:
      break;
    }
  }
}


/*
  Callback for IDENT expression type with name and Doc_path parameter
*/
inline
void Expr_builder_base::id(const string &name, const api::Db_obj *db_obj, const api::Doc_path &doc)
{
  id(name, db_obj);
  id(doc);
}


/*
  Callback for PLACEHOLDER expression type
*/
inline
void Expr_builder_base::placeholder()
{
  m_msg->set_type(Expr::PLACEHOLDER);
  // TODO: Does protocol support anonymous placeholders?
}

/*
  Callback for a named PLACEHOLDER expression type with name
*/

inline
void Expr_builder_base::placeholder(const string &name)
{
  if (!m_args_conv)
    throw_error("Expr builder: Calling placeholder without an Args_conv!");
  /*
    throw_error(
          (boost::format("Calling placeholder(%s) without an Args_conv!")
           % name
           ).str());
  */
  placeholder(m_args_conv->conv_placeholder(name));
}

inline
void Expr_builder_base::placeholder(unsigned pos)
{
  placeholder();
  m_msg->set_position(pos);
}

/*
  Having_builder implementation
 */

inline
Expr_builder_base::Args_prc*
Having_builder_base::op(const char *name)
{
  return get_args_builder(set_op(name));
}

inline
Expr_builder_base::Args_prc*
Having_builder_base::call(const api::Db_obj& db_obj)
{
  return get_args_builder(set_call(db_obj));
}


/*
  On table mode, having is reported as alias->$.path so no need to change
  anything
*/

inline
void Having_builder_base::id(const string &name, const api::Db_obj *coll,
                             const api::Doc_path &path)
{
  Expr_builder_base::id(name, coll);
  Expr_builder_base::id(path);
}

/*
  On document mode, having is reported as alias.path so we need to
  report to protocol as alias->$.path[1].
  This means that the first path position has to be a member and the rest
  of path is reported as it used to be.
*/
inline
void Having_builder_base::id(const api::Doc_path &path)
{
  if (!m_first_id)
  {
    Expr_builder_base::id(path);
    m_first_id = true;
    return;
  }

  m_first_id = false;

  if (path.is_whole_document() || path.get_type(0) != api::Doc_path::MEMBER)
    throw_error("Having expression should point to fields alias");


  /*
    Wrapper around Doc_path object which shifts all path elements by one, so
    that a path like "foo.bar.baz" becomes "bar.baz". The first path element is
    returned by projection_alias().
  */

  struct Doc_path_to_table : public api::Doc_path
  {
    Doc_path_to_table(const api::Doc_path &path)
      : m_path(path)
    {}

    const api::Doc_path &m_path;

    string projection_alias()
    {
      if (m_path.length() == 0 || m_path.get_type(0) != MEMBER)
        throw_error("Having should refer to projection alias");
      return *m_path.get_name(0);
    }

    bool is_whole_document() const override
    {
      return m_path.is_whole_document();
    }

    unsigned length() const override
    {
      auto len = m_path.length();
      if (len > 0)
        --len;
      return len;
    }

    Type get_type(unsigned pos) const override
    {
      return m_path.get_type(pos+1);
    }

    const string* get_name(unsigned pos) const override
    {
      return m_path.get_name(pos+1);
    }

    const uint32_t* get_index(unsigned pos) const override
    {
      return m_path.get_index(pos+1);
    }

  };

  Doc_path_to_table dp(path);

  Expr_builder_base::id(dp.projection_alias(), nullptr, dp);
  m_first_id = true;
}

template <class MSG>
inline
Having_builder_base::Args_prc*
Having_builder_base::get_args_builder(MSG &msg)
{

  m_args_builder.reset(new Args_builder<MSG,Having_builder>(msg, this->m_args_conv));
  return m_args_builder.get();
}

/*
   Save Arguments and convert them to numeric order.
*/

class Placeholder_conv_imp
    : public Args_conv
{
  std::map<string, unsigned> m_map;
  unsigned m_offset = 0;
public:

  virtual ~Placeholder_conv_imp() {}

  void clear()
  {
    m_map.clear();
    m_offset = 0;
  }

  void set_offset(unsigned offset)
  {
    m_offset = offset;
  }

  unsigned conv_placeholder(const string &name)
  {
    std::map<string, unsigned>::const_iterator it = m_map.find(name);
    if (it == m_map.end())
      throw_error("Placeholder converter: Placeholder was not defined on args");
      //throw Generic_error((boost::format("Placeholder %s was not defined on args.")
      //                     % name).str());

    return it->second;
  }

  void add_placeholder(const string &name)
  {
    std::map<string, unsigned>::const_iterator it = m_map.find(name);
    if (it != m_map.end())
      throw_error("Placeholder converter: Redefined placeholder");
      //throw Generic_error((boost::format("Redifined placeholder %s.")
      //                     % name).str());
    assert((m_map.size()+m_offset) < std::numeric_limits<unsigned>::max());
    unsigned pos = static_cast<unsigned>(m_map.size()+m_offset);
    m_map[name] = pos;
  }

};


}}} // cdk::protocol::mysqlx

#endif
