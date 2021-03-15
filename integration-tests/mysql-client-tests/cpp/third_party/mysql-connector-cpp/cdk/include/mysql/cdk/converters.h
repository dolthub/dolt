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

#ifndef CDK_CONVERTERS_H
#define CDK_CONVERTERS_H

#include "foundation.h"  // scoped_ptr<>
#include "api/document.h"

namespace cdk {

/*
  Value conversion
  ================
  Converting values described by generic CDK interfaces to descriptions
  expected by X protocol.

  To define a conversion, first one has to define expression processor
  converter. Given expression processor interfaces FROM and TO, a processor
  converter acts as a processor of type FROM which translates and forwards
  callbacks of the FROM interface to a processor of type TO.

  For example, Scalar_prc_converter defined below, translates callbacks of
  a generic cdk scalar processor of type cdk::Scalar_processor to callbacks
  of protocol processor of type protocol::mysqlx::api::Scalar_processor.
  The two processors are similar, but not identical and, e.g., strings must be
  translated from internal cdk encoding to one of encodings understood by
  the protocol.

  Having appropriate processor converter CONV, generic template
  Expr_conv_base<CONV,FROM,TO> can create a converter of expressions of
  type FROM to expressions of type TO. To convert expression expr_from of
  type FROM to an expression of type TO using expression converter conv,
  one does:

    conv.reset(expr_from);

  Now conv can act as expression of type TO which can be processed using
  an appropriate processor:

    conv.process(prc_to);

  Behind the scenes, the processor converter of type CONV will be used
  to translate callbacks from expr_from into callbacks of prc_to.
*/


/*
  Processor converter traits define types of processors that are being
  converted by a given converter.
*/

template <class CONV>
struct Conv_traits
{
  typedef typename CONV::Prc_from  Prc_from;
  typedef typename CONV::Prc_to    Prc_to;
};


/*
  Base template for defining processor converters. Such converter class
  Conv should be declared as:

    class Conv : public Converter<Conv, FROM, TO>
    {
      ...
    };

  where FROM and TO are types of processors being converted. If not specified,
  these types are read from the Conv_traits<Conv> structure which should
  be defined.

  Base template defines common infrastructure such as the m_proc member
  storing a pointer to the target processor and reset() method.
*/

template <
  class CONV,
  class FROM = typename Conv_traits<CONV>::Prc_from,
  class TO   = typename Conv_traits<CONV>::Prc_to
>
class Converter
  : public FROM
{
public:

  typedef FROM Prc_from;
  typedef TO   Prc_to;

  void reset(Prc_to &prc) const
  {
    const_cast<Converter*>(this)->m_proc= &prc;
  }

protected:

  Prc_to *m_proc;
};


// -------------------------------------------------------------------------

/*
  The Expr_conv_base<CONV, FROM, TO> template which generates converter for
  expressions from a processor converter class CONV. Optionally the types
  FROM and TO of converted expressions can be specified, but processors used
  by these expressions must match the processors that are being converted by
  CONV class.

  To use such expression coverter, one should call reset(expr_from) method
  passing reference to an expression of type FROM. Then the converter can
  be used as an expression of type TO which is result of translating the
  source expression using converter CONV.
*/

template <
  class CONV,
  class FROM = cdk::api::Expr_base< typename Conv_traits<CONV>::Prc_from >,
  class TO   = cdk::api::Expr_base< typename Conv_traits<CONV>::Prc_to >
>
class Expr_conv_base
  : public TO
{
  typedef FROM  Expr_from;
  typedef TO    Expr_to;

  typedef typename Expr_from::Processor  Prc_from;
  typedef typename Expr_to::Processor    Prc_to;

protected:

  const Expr_from *m_expr;
  CONV       m_conv;

public:

  Expr_conv_base() : m_expr(NULL)
  {}

  Expr_conv_base(const Expr_from &expr)
    : m_expr(&expr)
  {}

  Expr_conv_base(const Expr_from *expr)
    : m_expr(expr)
  {}

  void reset(const Expr_from &expr)
  {
    m_expr = &expr;
  }

  bool is_valid() const
  {
    return NULL != m_expr;
  }

  const TO* get() const
  {
    return m_expr ? this : NULL;
  }

  void process(Prc_to &proc) const
  {
    if (!m_expr)
      return;

    Expr_conv_base *self= const_cast<Expr_conv_base*>(this);
    self->m_conv.reset(proc);
    m_expr->process(self->m_conv);
  }
};


// -------------------------------------------------------------------------


/*
  Template List_prc_converter<CONV> produces a converter for list processors
  given base processor converter CONV for list elements.
*/

template <class CONV> class List_prc_converter;

template <class CONV>
struct Conv_traits< List_prc_converter<CONV> >
{
  typedef cdk::api::List_processor< typename Conv_traits<CONV>::Prc_from >
          Prc_from;
  typedef cdk::api::List_processor< typename Conv_traits<CONV>::Prc_to >
          Prc_to;
};

template <class CONV>
class List_prc_converter : public Converter< List_prc_converter<CONV> >
{
  typedef Converter< List_prc_converter<CONV> > Base;

  typedef typename Base::Prc_from Prc_from;
  typedef typename Base::Prc_to   Prc_to;
  using Base::m_proc;

  scoped_ptr<CONV> m_el_converter;
  using typename Prc_from::Element_prc;

  void list_begin() { m_proc->list_begin(); }
  void list_end()   { m_proc->list_end(); }

  Element_prc* list_el()
  {
    typename Prc_to::Element_prc *ep = m_proc->list_el();
    if (!ep)
      return NULL;
    CONV *conv = get_el_converter();
    conv->reset(*ep);
    return conv;
  }

public:

  CONV* get_el_converter();
};


template <class CONV>
inline
CONV* List_prc_converter<CONV>::get_el_converter()
{
  if (!m_el_converter)
    m_el_converter.reset(new CONV());
  return m_el_converter.get();
}


// -------------------------------------------------------------------------


/*
  Template Any_prc_converter<CONV> produces converter for Any expression
  processors given base processor converter CONV for base (scalar)
  expressions.

  Given base converter CONV for scalar, this template adds conversions for
  arrays and documents.
*/

template <class CONV> struct Doc_prc_converter;

template <class CONV> struct Any_prc_converter;

template <class CONV>
struct Conv_traits< Any_prc_converter<CONV> >
{
  typedef cdk::api::Any_processor<typename Conv_traits<CONV>::Prc_from>
          Prc_from;
  typedef cdk::api::Any_processor<typename Conv_traits<CONV>::Prc_to>
          Prc_to;
};


template <class CONV>
struct Any_prc_converter
  : public Converter< Any_prc_converter<CONV> >
{
  typedef Converter< Any_prc_converter<CONV> >  Base;

  typedef typename Base::Prc_from Prc_from;
  typedef typename Base::Prc_to   Prc_to;
  using Base::m_proc;

  typedef CONV                                  Scalar_conv;
  typedef List_prc_converter<Any_prc_converter> List_conv;
  typedef Doc_prc_converter<CONV>               Doc_conv;

  typedef typename Prc_from::Scalar_prc Scalar_prc;
  typedef typename Prc_from::Doc_prc    Doc_prc;
  typedef typename Prc_from::List_prc   List_prc;

  Scalar_prc* scalar()
  {
    typename Prc_to::Scalar_prc *sp = m_proc->scalar();
    if (!sp)
      return NULL;
    Scalar_conv *sc = get_scalar_conv();
    sc->reset(*sp);
    return sc;
  }

  List_prc* arr()
  {
    typename Prc_to::List_prc *lp = m_proc->arr();
    if (!lp)
      return NULL;
    List_conv *lc = get_list_conv();
    lc->reset(*lp);
    return lc;
  }

  Doc_prc* doc()
  {
    typename Prc_to::Doc_prc *dp = m_proc->doc();
    if (!dp)
      return NULL;
    Doc_conv *dc = get_doc_conv();
    dc->reset(*dp);
    return dc;
  }

private:

  Scalar_conv m_scalar_conv;
  List_conv   m_list_conv;
  scoped_ptr<Doc_conv> m_doc_conv;

public:

  Scalar_conv* get_scalar_conv()
  { return &m_scalar_conv; }

  List_conv* get_list_conv()
  { return &m_list_conv; }

  Doc_conv* get_doc_conv()
  {
    if (!m_doc_conv)
      m_doc_conv.reset(new Doc_conv());
    return m_doc_conv.get();
  }
};


// -------------------------------------------------------------------------


/*
  Template Doc_prc_converter<CONV> produces converter for document
  processors, given base processor converter CONV for base (scalar)
  expressions.
*/

template <class CONV>
struct Conv_traits< Doc_prc_converter<CONV> >
{
  typedef cdk::api::Doc_processor<typename Conv_traits<CONV>::Prc_from>
          Prc_from;
  typedef cdk::api::Doc_processor<typename Conv_traits<CONV>::Prc_to>
          Prc_to;
};

template <class CONV>
struct Doc_prc_converter
  : public Converter< Doc_prc_converter<CONV> >
{
  typedef Converter< Doc_prc_converter<CONV> >  Base;

  typedef typename Base::Prc_from Prc_from;
  typedef typename Base::Prc_to   Prc_to;
  using Base::m_proc;

  typedef Any_prc_converter<CONV> Any_conv;
  typedef typename Prc_from::Any_prc Any_prc;

  void doc_begin() { m_proc->doc_begin(); }
  void doc_end()   { m_proc->doc_end(); }

  Any_conv m_any_conv;

  Any_prc* key_val(const string &key)
  {
    typename Prc_to::Any_prc *ap = m_proc->key_val(key);
    if (!ap)
      return NULL;
    m_any_conv.reset(*ap);
    return &m_any_conv;
  }

};


} // cdk

#endif
