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

#ifndef CDK_API_EXPRESSION_H
#define CDK_API_EXPRESSION_H

#include "../foundation.h"

#include <stddef.h> // for NULL
#include <stdint.h>

namespace cdk {
namespace api {

using foundation::string;


/*
  Expressions
  ===========
  In CDK expressions are understood in a very broad sense. An expression can
  represent a simple scalar value like string or number but it can also be
  a true expression constructed from operators and function calls. Documents,
  that is key-value mappings are also treated as a kind of expression, as they
  fit into the same framework.

  An expression object implements Expr_base<PRC> interface, where type PRC
  determines the kind of expression that this object represents. To get
  description of the expression, or process it in any way, one calls
  Expression<PRC>::process(prc) method on the expression object, where
  prc is a processor implementing interface PRC which defines processor
  callbacks that will be called to describe the expression.

  For example, PRC can have the following callback which would be called for
  expressions of the form E1 + E2:

    void sum(Expr_base<PRC> &lhs, Expr_base<PRC> &rhs)

  Parameters lhs and rhs would describe E1 and E2, respectively.

  This representation of expressions uses visitor pattern which does
  not make any assumptions about internal representation of the expression,
  how the storage of the data is arranged etc.

  Expressions are used in CDK for things like selection criteria in CRUD
  operations or parameter values. See common.h for definitions of different
  expression processors used by CDK, such as Any_processor.
  See also api/document.h for definitions of interfaces used to describe
  documents.
*/


/**
  Base interface for expressions over processor PRC.

  For convenience, the Processor typedef is defined. Expr_base<PRC>::Processor
  is the same as PRC -- the type of the processor that can be used to process
  this kind of expressions.
*/

template <class PRC>
class Expr_base
{
public:

  typedef PRC Processor;

  virtual void process(Processor&) const =0;

  void process_if(Processor *prc) const
  {
    if (prc)
      process(*prc);
  }

  virtual ~Expr_base() {}
};


/**
  Interface for a list of expressions of type EXPR.

  Given expression processor type PRC, List_processor<PRC> is a processor for
  list of expressions over PRC. When processing a list, the following sequence
  of callbacks is made:

  1. prc.list_begin() - called before processing any list elements.

  2. prc.list_el() - called for each element in the list. Method shuld return
     pointer to a PRC processor which is used to process the element. If NULL
     is returned this list element is skipped.

  3. prc.list_end() - called after processing all elements in the list.
*/

template <class PRC>
class List_processor
{
public:

  typedef PRC Element_prc;

  // LCOV_EXCL_START
  virtual void list_begin() {}
  virtual void list_end()   {}
  // LCOV_EXCL_STOP
  virtual Element_prc* list_el() =0;

  virtual ~List_processor() {}
};


template <class EXPR>
class Expr_list
  : public Expr_base< List_processor<typename EXPR::Processor> >
{
public:

  typedef EXPR     Expression;

  virtual ~Expr_list() {}
};


}}  // cdk::api


/*
  Safe processor infrastructure
  =============================

  Processor callback chains like this one:

    prc.list_el()->scalar()->val()->str("foo");

  are dangerous, because they can easily lead to dereferencing a NULL pointer
  if some intermediate callback returns NULL. Writing NULL tests would be
  tedious. Instead, we introduce a safe processor wrapper around a plain
  processor. This wrapper performs NULL pointer checks and ignores callbacks
  if processor is NULL.

  Safe processor wrapper around a given processor is returned by safe_prc()
  function. Above callback chain should be written as follows:

    safe_prc(prc)->scalar()->val()->str("foo");

  Function safe_prc() returns object of type Safe_prc<PRC> where PRC is the
  type of the wrapped processor. Class Safe_prc<PRC> defines custom
  operator->() to perform NULL checks when calling processor callbacks.
*/

namespace cdk {

/*
  Generic template for Safe_prc<PRC> classes. This template must be
  specialized for each processor type used in CDK (otherwise safe_prc()
  might not compile. See below for specialization defined for list
  processors.
*/

template <class PRC> struct Safe_prc;


/*
  Function safe_prc() which returns wrapper for a given processor.
  Both a pointer and a reference to a processor are accepted. A call
  safe_prc(NULL) returns Safe_prc<PRC> instance that wrapps a NULL pointer.
  -- such wrapper should ignore all callbacks.
*/

template <class PRC>
Safe_prc<PRC> safe_prc(PRC &prc)
{
  return &prc;
}

template <class PRC>
Safe_prc<PRC> safe_prc(PRC *prc)
{
  return prc;
}


/*
  Base class for safe processor wrappers.

  Each specialization of Safe_prc<PRC> should derive from this base class.
  It defines the overloaded operator->() that makes wrapper instance behave
  like the wrapped processor. It also defines m_prc member which stores a
  pointer to the wrapped processor. Derived class should define base processor
  callbacks and either forward them to m_prc or ignore them if m_prc is NULL.
*/

template <class PRC>
struct Safe_prc_base
{
  typedef PRC             Processor;

  Processor *m_prc;
  Safe_prc_base(Processor *prc) : m_prc(prc)
  {}

  Safe_prc<PRC>* operator->()
  {
    return (Safe_prc<PRC>*)this;
  }

  operator PRC*()
  {
    return m_prc;
  }
};


/*
  Safe processor wrapper for list processors.

  It defines the same callbacks as List_processor<PRC>. However,
  List_processor<PRC>::list_el() returns a pointer to a sub-processor that
  can process list element. In the wrapper this callback returns wrapped
  element processor so that nothing bad happens if this processor is NULL.

  If wrapped processor is NULL then this wrapper ignores all callbacks.
*/

template <class PRC>
struct Safe_prc< cdk::api::List_processor<PRC> >
  : Safe_prc_base< cdk::api::List_processor<PRC> >
{
  typedef Safe_prc_base< cdk::api::List_processor<PRC> >  Base;
  using typename Base::Processor;
  typedef typename Base::Processor::Element_prc  Element_prc;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  void list_begin()
  { return m_prc ? m_prc->list_begin() : (void)NULL; }

  void list_end()
  { return m_prc ? m_prc->list_end() : (void)NULL; }

  Safe_prc<Element_prc>
  list_el()
  { return m_prc ? m_prc->list_el() : NULL; }

};

}

#endif
