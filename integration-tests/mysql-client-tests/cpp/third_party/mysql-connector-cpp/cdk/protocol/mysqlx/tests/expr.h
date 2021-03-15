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

#ifndef MYSQL_CDK_PROTOCOL_MYSQLX_TESTS_EXPR_H
#define MYSQL_CDK_PROTOCOL_MYSQLX_TESTS_EXPR_H


#include <mysql/cdk.h>
#include <mysql/cdk/protocol/mysqlx.h>

#include <gtest/gtest.h>
#include <xplugin_test.h>

/**
  Common infrastructure for mysqlx protocol tests.
*/

/*
  To use getenv() on Windows, which warns that it is not safe
*/
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

namespace cdk {
namespace test {
namespace proto{
namespace expr {

using std::min;

using namespace cdk::protocol::mysqlx;

namespace api {
  using namespace cdk::protocol::mysqlx::api;
}  // api


class Scalar_base : public api::Scalar
{
public:
  virtual ~Scalar_base() {}
  virtual const Scalar_base* clone() const =0;
};

class Any_base : public api::Any
{
public:
  virtual ~Any_base() {}
  virtual const Any_base* clone() const =0;
};

class Args_map : public api::Args_map
{
  typedef std::map<string,const Any_base*> Args;
  Args m_args;

public:

  Args_map() {}
  Args_map(const Args_map&rhs)
  {

    for (Args::const_iterator it = rhs.m_args.begin();
         it != rhs.m_args.end();
         ++it)
    {
        m_args[it->first] = it->second->clone();
    }
  }
  ~Args_map()
  {
      for (Args::iterator it = m_args.begin();
           it != m_args.end();
           ++it)
      delete it->second;
  }

  void process(Processor &ep) const
  {
    ep.doc_begin();
    Args::const_iterator it = m_args.begin();
    for(; it != m_args.end(); ++it)
    {
      it->second->process_if(ep.key_val(it->first));
    }
    ep.doc_end();
  }

  void add(const string &name, const Any_base &expr) { m_args[name] = expr.clone(); }
};

class Expr_base;

template <class E, class B = Expr_base>
class Expr_class : public B
{

protected:

  Expr_class() {}

  template <typename T>
  Expr_class(void *, T arg) : B(arg)
  {}

  typedef Expr_class Base;
  typedef typename B::Processor Processor;
  const B* clone() const
  { return new E(*(E*)this); }
};


class Param_String : public Expr_class<Param_String, Any_base>
{
  std::string  m_val;
  collation_id_t m_cs;
  bool         m_has_cs;

public:

  Param_String(collation_id_t cs, const std::string &val)
    : m_val(val), m_cs(cs), m_has_cs(true)
  {}

  Param_String(const std::string &val)
    : m_val(val), m_has_cs(false)
  {}

  void process(Processor &p) const
  {
    if (m_has_cs)
      safe_prc(p)->scalar()->str(m_cs, bytes((byte*)m_val.data(), m_val.size()));
    else
      safe_prc(p)->scalar()->str(bytes((byte*)m_val.data(), m_val.size()));
  }
};

class Param_Number : public Expr_class<Param_Number, Any_base>
{
  enum { DOUBLE, FLOAT, SINT, UINT } m_type;

  union {
    double   v_double;
    float    v_float;
    int64_t  v_sint;
    sint64_t v_uint;
  } m_val;

public:

  Param_Number(int64_t val)
    : m_type(SINT)
  {
    m_val.v_sint= val;
  }

  Param_Number(uint64_t val)
    : m_type(UINT)
  {
    m_val.v_uint= val;
  }

  Param_Number(float val)
    : m_type(FLOAT)
  {
    m_val.v_float= val;
  }

  Param_Number(double val)
    : m_type(DOUBLE)
  {
    m_val.v_double= val;
  }

  void process(Processor &p) const
  {
    switch (m_type)
    {
    case   UINT: safe_prc(p)->scalar()->num(m_val.v_uint);   break;
    case   SINT: safe_prc(p)->scalar()->num(m_val.v_sint);   break;
    case  FLOAT: safe_prc(p)->scalar()->num(m_val.v_float);  break;
    case DOUBLE: safe_prc(p)->scalar()->num(m_val.v_double); break;
    }
  }
};


class Expr_base : public api::Expression
{
public:
  virtual ~Expr_base() {}
  virtual const Expr_base* clone() const =0;
};


class List : public api::Expr_list
{
  std::vector<const cdk::test::proto::expr::Expr_base*> m_list;

public:

  List() {}
  List(const List&);
  ~List();

  void process(Processor &p) const
  {
    p.list_begin();

    for (std::vector<const cdk::test::proto::expr::Expr_base*>::const_iterator it = m_list.begin();
         it != m_list.end();
         ++it)
    {
      (*it)->process_if(p.list_el());
    }
    p.list_end();
  }

  unsigned count() const { return m_list.size(); }
  const api::Expression& get(unsigned pos) const
  { return *m_list.at(pos); }

  void add(const cdk::test::proto::expr::Expr_base &expr)
  { m_list.push_back(expr.clone()); }
};

inline
List::~List()
{
  for(unsigned i=0; i < m_list.size(); ++i)
    delete m_list[i];
}

inline
List::List(const List &rhs)
{
  for(unsigned i=0; i < rhs.m_list.size(); ++i)
    m_list.push_back(rhs.m_list[i]->clone());
}





class String : public Expr_class<String>
{
  std::string  m_val;
  collation_id_t m_cs;
  bool         m_has_cs;

public:

  String(collation_id_t cs, const std::string &val)
    : m_val(val), m_cs(cs), m_has_cs(true)
  {}

  String(const std::string &val)
    : m_val(val), m_has_cs(false)
  {}

  void process(Processor &p) const
  {
    if (m_has_cs)
      safe_prc(p)->scalar()->val()->str(m_cs, bytes((byte*)m_val.data(), m_val.size()));
    else
      safe_prc(p)->scalar()->val()->str(bytes((byte*)m_val.data(), m_val.size()));
  }
};


class Number : public Expr_class<Number>
{
  enum { DOUBLE, FLOAT, SINT, UINT } m_type;

  union {
    double   v_double;
    float    v_float;
    int64_t  v_sint;
    sint64_t v_uint;
  } m_val;

public:

  Number(int64_t val)
    : m_type(SINT)
  {
    m_val.v_sint= val;
  }

  Number(uint64_t val)
    : m_type(UINT)
  {
    m_val.v_uint= val;
  }

  Number(float val)
    : m_type(FLOAT)
  {
    m_val.v_float= val;
  }

  Number(double val)
    : m_type(DOUBLE)
  {
    m_val.v_double= val;
  }

  void process(Processor &p) const
  {
    switch (m_type)
    {
    case   UINT: p.scalar()->val()->num(m_val.v_uint);   break;
    case   SINT: p.scalar()->val()->num(m_val.v_sint);   break;
    case  FLOAT: p.scalar()->val()->num(m_val.v_float);  break;
    case DOUBLE: p.scalar()->val()->num(m_val.v_double); break;
    }
  }
};


class Field : public Expr_class<Field>
{
protected:

  const string m_name;

public:

  Field(const string name) : m_name(name)
  {}

  void process(Processor &p) const
  { p.scalar()->id(m_name, NULL); }
};


class Parameter : public Expr_class<Parameter, Field>
{
  int m_pos;

public:

  Parameter() : Base(NULL, string()), m_pos(-1)
  {}

  Parameter(unsigned pos) : Base(NULL, string()), m_pos((int)pos)
  {}

  Parameter(const string &name) : Base(NULL, name), m_pos(-1)
  {}

  void process(Processor &p) const
  {
    if (m_pos >= 0)
    {
      p.scalar()->placeholder(m_pos);
      return;
    }

    if (m_name.length() > 0)
    {
      p.scalar()->placeholder(m_name);
      return;
    }

    p.scalar()->placeholder();
  }

};

class Op : public Expr_class<Op>
{
protected:

  const char *m_op;
  cdk::test::proto::expr::List   m_args;

public:

  Op(const char *name) : m_op(name)
  {}

  Op(const char *name, const Expr_base &lhs, const Expr_base &rhs)
    : m_op(name)
  {
    add_arg(lhs);
    add_arg(rhs);
  }

  void add_arg(const Expr_base &arg)
  {
    m_args.add(arg);
  }

  void process(Processor &p) const
  {
    protocol::mysqlx::api::Expr_processor::Args_prc *prc = p.scalar()->op(m_op);
    prc->list_begin();
    m_args.process(*prc);
    prc->list_end();
  }
};


class Call : public Expr_class<Call, Op>
{
public:

  Call(const char *name) : Base(NULL,name)
  {}

  void process(Processor &p) const
  {
    m_args.process(*p.scalar()->call(Db_obj(m_op)));
  }
};






}}}}  // cdk::test::proto::expr

#endif
