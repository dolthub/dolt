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

#ifndef CDK_API_DOCUMENT_H
#define CDK_API_DOCUMENT_H

#include "expression.h"

namespace cdk {
namespace api {


/**
  Documents over processor PRC
  ============================

  Such documents are key-value maps where value of a key can be either an
  expression over PRC (a "scalar" value), list of such expressions, or
  another document over PRC.

  Object that describes a document over PRC should implement the
  Doc_base<PRC> interface. This is Expr_base<> interface over
  special Doc_processor<PRC> processor type that defines callbacks used
  to describe the key-value mapping (see below).

  Given document object d implementing Doc_base<PRC> and a corresponding
  processor dp, executing d.process(dp) will first call dp.doc_begin(),
  then dp.key_val() callbacks to present keys and their values and finally
  dp.doc_end().

  An example document class is cdk::JSON defined in common.h. It is a document
  over processor of type JSON_processor.

  TODO: Example.
*/

template <class PRC>
class Doc_processor;

template <class PRC>
class Doc_base;

template <class PRC>
class Any;


/*
  "Any" Expression is an expression which can be a base expression over
  some processor PRC, an array of such expressions or a document over PRC
  (that is, a key-value map where key values can be again such "any"
  expressions).

  Given expression processor type PRC, Any_processor<PRC> is a processor
  of any values over PRC. The processor callbacks for different kind of
  values return an appropriate processor to process the value or NULL if
  the value should be ignored.
*/

template <class PRC>
class Any_processor
{
public:

  typedef PRC                            Scalar_prc;
  typedef Doc_processor<PRC>             Doc_prc;
  typedef List_processor<Any_processor>  List_prc;


  /*
    Report that any value is a "scalar", that is, a base expression over
    PRC.
  */

  virtual Scalar_prc* scalar() =0;

  // Report that any value is an array, that is, a list of any expressions.

  virtual List_prc*   arr() =0;

  // Report that any value is a document.

  virtual Doc_prc*    doc() =0;

  virtual ~Any_processor() {}
};


template <class PRC>
class
    Any: public Expr_base< Any_processor<PRC> >
{
public:

  typedef Any_processor<PRC>    APRC;

  typedef Expr_base<PRC>   Scalar;
  typedef Doc_base<PRC>    Document;
  typedef Expr_list<Any>   List;

};


/**
  Documents over PRC are expression over Doc_processor<PRC>. If d is a
  document object and dp is a ducument processor then d.process(dp) will
  describe key-value pairs to the processor using Doc_processor<PRC>
  callbacks.
*/

template <class PRC>
class Doc_processor
{
public:

  typedef cdk::api::string  string;
  typedef Any_processor<PRC>       Any_prc;

  /// This is called before any key_val() or key_doc() callback.
  // LCOV_EXCL_START
  virtual void doc_begin() {}

  /// This is called after last key_val() or key_doc() callback.
  virtual void doc_end() {}
  // LCOV_EXCL_STOP

  /**
    Called to report key value which can be either simple expression,
    array or document. The callback should return a processor to process
    this key value or NULL to ignore it.
  */
  virtual Any_prc* key_val(const string &key) =0;

  virtual ~Doc_processor() {}
};


template <class PRC>
class Doc_base : public Expr_base< Doc_processor<PRC> >
{
public:

  typedef typename Doc_processor<PRC>::string    string;
  typedef typename Any<PRC>::Scalar    Scalar;
  typedef typename Any<PRC>::Document  Document;
  typedef typename Any<PRC>::List      List;
};


/*
  Document path specification is a list of items, each to be processed
  with Doc_path_processor to describe one element of the path.
*/

class Doc_path_element_processor
{
public:

  typedef cdk::api::string  string;
  typedef uint32_t          index_t;

  // Path element is name of document field.

  virtual void member(const string &name) =0;

  // Path element "*".

  virtual void any_member() =0;

  // Path element is at given position within an array.

  virtual void index(index_t) =0;

  // Path element "[*]".

  virtual void any_index() =0;

  // Path element "**".

  virtual void any_path() =0;
};


class Doc_path_processor
  : public List_processor< Doc_path_element_processor >
{
public:

  typedef Element_prc::string  string;
  typedef Element_prc::index_t index_t;

  // The "$" path which denotes the whole document.
  virtual void whole_document() = 0;
};

typedef Expr_base<Doc_path_processor>  Doc_path;


}}  // cdk::api


namespace cdk {

class Doc_path_storage
  : public api::Doc_path
  , public api::Doc_path::Processor
  , api::Doc_path_element_processor
{
public:

  enum Type {
    MEMBER,
    MEMBER_ASTERISK,
    ARRAY_INDEX,
    ARRAY_INDEX_ASTERISK,
    DOUBLE_ASTERISK
  };

  using api::Doc_path_element_processor::string;
  using api::Doc_path_element_processor::index_t;

protected:

  struct Path_el
  {
    Type      m_type;
    string    m_name;
    uint32_t  m_idx;
  };

  std::vector<Path_el> m_path;

public:

  // Access to path data

  size_t length() const
  {
    return m_path.size();
  }

  bool is_empty() const
  {
    return m_whole_document ? false : 0 == length();
  }

  bool is_whole_document() const
  {
    return m_whole_document;
  }

  const Path_el& get_el(size_t pos) const
  {
    return m_path.at(pos);
  }

  void clear()
  {
    m_path.clear();
  }

  // Doc_path

  void process(Processor &prc) const
  {
    if (m_whole_document)
    {
      prc.whole_document();
      return;
    }

    prc.list_begin();

    for (size_t pos = 0; pos < m_path.size(); ++pos)
    {
      api::Doc_path_element_processor *eprc = prc.list_el();
      if (eprc)
      {
        const Path_el &el = m_path[pos];
        switch (el.m_type)
        {
        case MEMBER:                eprc->member(el.m_name);  break;
        case MEMBER_ASTERISK:       eprc->any_member();       break;
        case ARRAY_INDEX:           eprc->index(el.m_idx);    break;
        case ARRAY_INDEX_ASTERISK:  eprc->any_index();        break;
        case DOUBLE_ASTERISK:       eprc->any_path();         break;
        }
      }
    }

    prc.list_end();
  }


  // List_processor

  bool  m_whole_document = false;
  Path_el *m_el = NULL;

  Element_prc* list_el()
  {
    assert(!m_whole_document);
    m_path.push_back(Path_el());
    m_el = &m_path.back();
    return this;
  }

private:

  // Doc_path_processor

  void whole_document()
  {
    m_whole_document = true;
  }

  void member(const string &name)
  {
    assert(m_el);
    m_el->m_type = MEMBER;
    m_el->m_name = name;
  }

  void any_member()
  {
    assert(m_el);
    m_el->m_type = MEMBER_ASTERISK;
  }

  void index(index_t pos)
  {
    assert(m_el);
    m_el->m_type = ARRAY_INDEX;
    m_el->m_idx = pos;
  }

  void any_index()
  {
    assert(m_el);
    m_el->m_type = ARRAY_INDEX_ASTERISK;
  }

  void any_path()
  {
    assert(m_el);
    m_el->m_type = DOUBLE_ASTERISK;
  }
};

} // cdk


namespace cdk {

template <class PRC>
struct Safe_prc< cdk::api::Any_processor<PRC> >
  : Safe_prc_base< cdk::api::Any_processor<PRC> >
{
  typedef Safe_prc_base< cdk::api::Any_processor<PRC> >  Base;
  using typename Base::Processor;

  typedef typename Base::Processor::Scalar_prc  Scalar_prc;
  typedef typename Base::Processor::Doc_prc     Doc_prc;
  typedef typename Base::Processor::List_prc    List_prc;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  Safe_prc<Scalar_prc> scalar()
  { return m_prc ? m_prc->scalar() : NULL; }

  Safe_prc<List_prc> arr()
  { return m_prc ? m_prc->arr() : NULL; }

  Safe_prc<Doc_prc> doc()
  { return m_prc ? m_prc->doc() : NULL; }

};


template <class PRC>
struct Safe_prc< cdk::api::Doc_processor<PRC> >
  : Safe_prc_base< cdk::api::Doc_processor<PRC> >
{
  typedef Safe_prc_base< cdk::api::Doc_processor<PRC> >  Base;
  using typename Base::Processor;

  typedef typename Base::Processor::string  string;
  typedef typename Base::Processor::Any_prc Any_prc;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  void doc_begin()
  { return m_prc ? m_prc->doc_begin() : (void)NULL; }

  void doc_end()
  { return m_prc ? m_prc->doc_end() : (void)NULL; }

  Safe_prc<Any_prc> key_val(const string &key)
  { return m_prc ? m_prc->key_val(key) : NULL; }

};


template<>
struct Safe_prc<api::Doc_path_element_processor>
  : Safe_prc_base<api::Doc_path_element_processor>
{
  typedef Safe_prc_base<api::Doc_path_element_processor> Base;
  using Base::Processor;
  typedef Processor::string  string;
  typedef Processor::index_t index_t;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  using Base::m_prc;

  void member(const string &name)
  { return m_prc ? m_prc->member(name) : (void)NULL; }

  void any_member()
  { return m_prc ? m_prc->any_member() : (void)NULL; }

  void index(index_t ind)
  { return m_prc ? m_prc->index(ind) : (void)NULL; }

  void any_index()
  { return m_prc ? m_prc->any_index() : (void)NULL; }

  void any_path()
  { return m_prc ? m_prc->any_path() : (void)NULL; }
};

template<>
struct Safe_prc<api::Doc_path_processor>
  : Safe_prc_base<api::Doc_path_processor>
{

  typedef Safe_prc_base<api::Doc_path_processor> Base;
  using Base::Processor;
  typedef Processor::string  string;
  typedef Processor::index_t index_t;

  Safe_prc(Processor *prc) : Base(prc)
  {}

  Safe_prc(Processor &prc) : Base(&prc)
  {}

  void list_begin()
  {
    if (m_prc)
      m_prc->list_begin();
  }

  void list_end()
  {
    if (m_prc)
      m_prc->list_end();
  }

  api::Doc_path_processor::Element_prc* list_el()
  { return m_prc ? m_prc->list_el() : NULL; }

  void whole_document()
  { return m_prc ? m_prc->whole_document() : (void)NULL; }

};

}

#endif
