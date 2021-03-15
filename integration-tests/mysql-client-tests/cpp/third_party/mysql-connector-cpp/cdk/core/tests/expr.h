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

#ifndef MYSQL_CDK_CORE_TESTS_EXPR_H
#define MYSQL_CDK_CORE_TESTS_EXPR_H


#include "test.h"


namespace cdk {
namespace test {
namespace expr {

using std::min;


class Expr_base : public cdk::Expression
{
public:
  virtual ~Expr_base() {}
  virtual const Expr_base* clone() const =0;
};


class List : public Expr_list
{
  std::vector<const Expr_base*> m_list;

public:

  List() {}
  List(const List&);
  ~List();

  unsigned count() const { return m_list.size(); }
  const Expression& get(unsigned pos) const
  { return *m_list.at(pos); }

  void add(const Expr_base &expr) { m_list.push_back(expr.clone()); }
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


template <class E, class B = Expr_base>
class Expr_class : public B
{

protected:

  Expr_class() {}

  template <typename T>
  Expr_class(void *, T arg) : B(arg)
  {}

  typedef Expr_class Base;
  typedef typename Base::Processor Processor;
  const Expr_base* clone() const
  { return new E(*(E*)this); }
};


class String : public Expr_class<String>
{
  string  m_val;

public:

  String(const string &val)
    : m_val(val)
  {}

  void process(Processor &p) const
  {
    p.str(m_val);
  }
};


class Number : public Expr_class<Number>
{
  enum { DOUBLE, FLOAT, SINT, UINT } m_type;

  union {
    double   v_double;
    float    v_float;
    int64_t  v_sint;
    uint64_t v_uint;
  } m_val;

public:

  Number(int val)
    : m_type(SINT)
  {
    m_val.v_sint= val;
  }

  Number(unsigned val)
    : m_type(UINT)
  {
    m_val.v_uint= val;
  }

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
    case   UINT: p.num(m_val.v_uint);   break;
    case   SINT: p.num(m_val.v_sint);   break;
    case  FLOAT: p.num(m_val.v_float);  break;
    case DOUBLE: p.num(m_val.v_double); break;
    }
  }
};


class Path
  : public Expr_class<Path>
  , public cdk::Doc_path
{
protected:

  std::vector<string> m_path;

public:

  Path()
  {}

  Path(const string &member)
  {
    add(member);
  }

  void process(Processor &p) const
  { p.ref(*this); }

  void add(const string &member)
  {
    m_path.push_back(member);
  }

private:

  unsigned length() const { return m_path.size(); }
  Type get_type(unsigned pos) const { return MEMBER; }
  const string* get_name(unsigned pos) const { return &m_path.at(pos); }
  const uint32_t* get_index(unsigned) const { return NULL; }
};


class Op : public Expr_class<Op>
{
protected:

  const char *m_op;
  List        m_args;

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
  { p.op(m_op, m_args); }
};


class And : public Op
{
public:

  And(const Expr_base &lhs, const Expr_base &rhs)
    : Op("&&", lhs, rhs)
  {}
};

class Call : public Expr_class<Call, Op>
{
public:

  Call(const char *name) : Base(NULL,name)
  {}

  void process(Processor &p) const
  { p.call(Table_ref(m_op), m_args); }
};


}}}  // cdk::test::expr

#endif
