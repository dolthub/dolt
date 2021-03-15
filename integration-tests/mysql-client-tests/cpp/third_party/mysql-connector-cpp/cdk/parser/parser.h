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

#ifndef CDK_PARSER_PARSER_H
#define CDK_PARSER_PARSER_H

#include <mysql/cdk/api/expression.h>
#include "tokenizer.h"


#ifdef _WIN32

 /*
 4061 = enum constant not explicitly handled by switch() case.

 We have a lot of token type constants and we commonly use default:
 clause to catch all otherwise not handled token types. Thus this
 warning must be disabled.

 */

DISABLE_WARNING_CDK(4061)

#endif

/*
  Infrastructure for building list and document parsers from base
  expression parsers.
*/

namespace parser {

typedef Tokenizer::iterator  It;
using std::string;
using cdk::throw_error;


/*
  Class that implements token navigation and usage methods
*/

class Token_base
{
protected:

  It  *m_first;
  It  m_last;
  Token m_last_tok;

  const Token* consume_token()
  {
    const Token *t = peek_token();
    if (!t)
      return NULL;
    m_last_tok = *t;
    assert(m_first);
    ++(*m_first);
    return &m_last_tok;
  }


  // return null if no more tokens available

  const Token* consume_token(Token::Type type)
  {
    if (!tokens_available() || !cur_token_type_is(type))
      return NULL;
    return consume_token();
  }

  const Token& consume_token_throw(Token::Type type, const string &msg)
  {
    const Token *t = consume_token(type);
    if (!t)
      parse_error(msg);
    return *t;
  }


  const Token* peek_token()
  {
    if (!tokens_available())
      return NULL;
    assert(m_first);
    return &(**m_first);
  }

  bool  cur_token_type_is(Token::Type type)
  {
    return tokens_available() && peek_token()->get_type() == type;
  }

  bool  cur_token_type_in(Token::Set types)
  {
    return tokens_available()
           && types.find(peek_token()->get_type()) != types.end();
  }


  It& cur_pos()
  {
    assert(m_first);
    return *m_first;
  }

  const It& cur_pos() const
  {
    return const_cast<Token_base*>(this)->cur_pos();
  }

  const It& end_pos() const
  {
    return m_last;
  }

  bool  tokens_available() const
  {
    return m_first && cur_pos() != end_pos();
  }


  typedef Tokenizer::Error  Error;

  void parse_error(const string&) const;
  void unsupported(const string&) const;

public:

  Token_base()
    : m_first(NULL)
  {}

  Token_base(It &first, const It &last)
  {
    set_tokens(first, last);
  }

  void set_tokens(It &first, const It &last)
  {
    m_first = &first;
    m_last = last;
  }
};


inline
void Token_base::parse_error(const string  &msg) const
{
  throw Error(*m_first, msg);
}

inline
void Token_base::unsupported(const string &what) const
{
  string msg(what);
  msg.append(" not supported yet");
  parse_error(msg);
}


/*
  Base class for parsers which parse tokens and present result as
  an expression over processor PRC.

  Parser objects which derive from Expr_parser<PRC> parse range of tokens
  specified when the object is created, assuming that these tokens represent
  an expression over PRC. The parsed expression is reported to a processor
  in process() method.

  Assuming that P is a class that derives from Expr_parser<PRC>, p is a
  parser object of class P and prc is a processor of type PRC, a call:

    p.process(prc);

  will report to prc the expression represented by tokens specified when p
  was created. If tokens could not be correctly parsed an error is thrown
  either when this method is called or when p is created. It is up to
  the implementation to decide when the parsing happens: it can be during
  parser creation (and then the parser must store results of parsing) or it
  can be done on-the-fly, in the process() method. Method p.parse(prc) has
  the same effect as p.process(prc).

  It is also possible to call:

    p.consume();

  which consumes tokens of the expression without reporting them to any
  parser.

  Since parsing consumes tokens, it is assumed that parse()/process() can
  be called only once for a given parser instance. Another call will throw
  an error.

  Derived classes should implement the functionality by overriding do_parse()
  and do_consume() methods. By default do_consume() is implemented by calling
  do_parse() with NULL processor pointer.
*/

template <class PRC, class Tokens = Token_base>
class Expr_parser
  : public cdk::api::Expr_base<PRC>
  , protected Tokens
{
protected:

  using Tokens::set_tokens;
  using Tokens::cur_pos;
  using Tokens::end_pos;
  using Tokens::tokens_available;
  using Tokens::cur_token_type_is;
  using Tokens::cur_token_type_in;
  using Tokens::consume_token;
  using Tokens::parse_error;
  using Tokens::unsupported;
  using typename Tokens::Error;

public:

  Expr_parser(It &first, const It &last)
    : m_consumed(false)
  {
    set_tokens(first, last);
  }

  void process(PRC &prc) const
  {
    if (!const_cast<Expr_parser*>(this)->parse(prc))
      parse_error("Failed to parse the string");
  }

  /*
    Parse tokens specified when creating this parser instance and
    report parsed expression to the given processor.

    This method can be called only once and it is assumed that it
    consumes the tokens that were parsed. That is, after a successful
    call to parse() the first iterator passed to the constructor
    is moved and points at the first token after the parsed expression.

    Returns false if tokens could not be parsed as an expression and
    and no tokens have been consumed (first iterator is not moved).
    Returns true if complete expression has been parsed. Otherwise
    (could not parse but some tokens were consumed) throws parse
    error.

    Note: this method is implemented in terms of do_parse() which
    should be overridden by derived classes.
  */

  bool parse(PRC &prc)
  {
    if (m_consumed)
      THROW("Expr_praser: second pass");

    if (!do_parse(&prc))
      return false;
    m_consumed = true;
    return true;
  }

  /*
    Consume tokens that form an expression without reporting them
    to any processor.

    Like parse(), this method can be called only once and should
    move the first iterator.

    Note: this method is implemented in terms of do_consume() which
    can be overridden by derived classes to provide more efficient
    implementation.
  */

  void consume()
  {
    if (m_consumed)
      return;
    do_consume();
    m_consumed = true;
  }


  /*
    Helper method which calls consume() if prc is NULL, otherwise
    calls parse() reporting results to the processor pointed by prc.
  */

  bool process_if(PRC *prc)
  {
    if (prc)
      return parse(*prc);
    consume();
    return true;
  }


protected:

  bool m_consumed;

  /*
    Internal method that implements parse() method - to be overridden
    by derived class.

    See documentation of parse() for return value specification and
    information how first iterator should be updated.
  */

  virtual bool do_parse(PRC *prc) =0;

  /*
    Internal method that implements consume() method. By default it
    calls parse() with NULL processor but derived classes can override
    to provide more efficient implementation.
  */

  virtual void do_consume()
  {
    if (!do_parse(NULL))
      throw Error(cur_pos(), "Failed to parse the string");
  }
};


}  // parser


//-------------------------------------------------------------------------


namespace parser {

using cdk::api::List_processor;


/*
  Template which constructs a parser for a list of expressions given a base
  parser for a single expression.

  List_parser<> is implemented using on-the-fly parsing.
*/

template <class Base>
struct List_parser
  : public Expr_parser< List_processor<typename Base::Processor> >
  , cdk::foundation::nocopy
{
  typedef typename Base::Processor  PRC;
  typedef List_processor<PRC>       LPRC;
  typedef Expr_parser<LPRC>         Parser_base;

  using Parser_base::set_tokens;
  using Parser_base::cur_pos;
  using Parser_base::end_pos;
  using Parser_base::tokens_available;
  using Parser_base::cur_token_type_is;
  using Parser_base::cur_token_type_in;
  using Parser_base::consume_token;
  using Parser_base::parse_error;
  using Parser_base::unsupported;


  Token::Type      m_list_sep;


  List_parser(It &first, const It &last, Token::Type sep = Token::COMMA)
    : Expr_parser<LPRC>(first, last), m_list_sep(sep)
  {}


  bool do_parse(LPRC *prc)
  {
    bool first_element = true;

    do {

      Base el_parser(cur_pos(), end_pos());

      if (!el_parser.process_if(prc ? prc->list_el() : NULL))
      {
        if (first_element)
          return false;
        else
          parse_error("Expected next list element");
      }

      if (!consume_token(m_list_sep))
        break;

      first_element = false;
    }
    while (true);

    return true;
  }

};

}  // parser


//-------------------------------------------------------------------------

namespace parser {

using cdk::api::Expr_base;
using cdk::api::Expr_list;
using cdk::api::Any;
using cdk::api::Doc_processor;
using cdk::api::Any_processor;



/*
  Extend base parser with document and array parsing.

  Given type Base of the base parser, Any_parser<Base> is a parser
  which can parse documents, arrays or expressions recognized by the
  base parser. Document and array elements can be again any kind of
  expression recognized by Any_parser. If the first token is '{' or '['
  then Any_parser<> assumes that this is document/array expression.
  Otherwise it must be base expression.

  Any_parser<Base> reports parsed expression to a processor of type
  Any_processor<SPRC>, where SPRC is a processor type for base (scalar)
  values. Normally SPRC is the processor type of the base parser, but
  a different SPRC type can be specified when instantiating Any_parser<>
  template.

  The Base class must define static method for converting processor
  used by Any_parser<> to a processor used by the base parser. The expected
  signature of this method is:

    static Base::Processor* get_base_prc(Any_processor<SPRC>*);

  where SPRC is the scalar processor type specified for Any_parser<>
  template (so, it is Base::Processor by default).

*/

template <class Base,
          class SPRC = Any_processor<typename Base::Processor>
         >
struct Any_parser
  : public Expr_parser< Any_processor<SPRC> >
{
  typedef typename Base::Processor                PRC;
  typedef typename Any<SPRC>::Processor           APRC;
  typedef typename Any<SPRC>::Document::Processor DPRC;
  typedef typename Any<SPRC>::List::Processor     LPRC;

  typedef Expr_parser< Any_processor<SPRC> >  Parser_base;

  using Parser_base::cur_pos;
  using Parser_base::end_pos;
  using Parser_base::tokens_available;
  using Parser_base::cur_token_type_is;
  using Parser_base::cur_token_type_in;
  using Parser_base::consume_token;
  using Parser_base::parse_error;
  using Parser_base::unsupported;


  Any_parser(It &first, const It &last)
    : Expr_parser<APRC>(first, last)
  {}


  bool do_parse(APRC *prc)
  {
    if (cur_token_type_is(Token::LCURLY))
    {
      Doc_parser doc(cur_pos(), end_pos());
      doc.process_if(prc ? prc->doc() : NULL);
    }
    else if (cur_token_type_is(Token::LSQBRACKET))
    {
      Arr_parser arr(cur_pos(), end_pos());
      arr.process_if(prc ? prc->arr() : NULL);
    }
    else
    {
      Base val(cur_pos(), end_pos());
      return val.process_if(prc ? Base::get_base_prc(prc) : NULL);
    }

    return true;
  }

  // Array parser used by Any_parser

  struct Arr_parser : public Expr_parser<LPRC>
  {
    typedef Expr_parser<LPRC>  Parser_base;

    using Parser_base::cur_pos;
    using Parser_base::end_pos;
    using Parser_base::tokens_available;
    using Parser_base::cur_token_type_is;
    using Parser_base::cur_token_type_in;
    using Parser_base::consume_token;
    using Parser_base::parse_error;

    Arr_parser(It &first, const It &last)
      : Expr_parser<LPRC>(first, last)
    {}

    bool do_parse(LPRC *prc)
    {
      if (!consume_token(Token::LSQBRACKET))
        return false;

      if (prc)
        prc->list_begin();

      if (!cur_token_type_is(Token::RSQBRACKET))
      {
        List_parser<Any_parser> list(cur_pos(), end_pos());
        bool ok = list.process_if(prc);
        if (!ok)
          parse_error("Expected array element");
      }

      if (!consume_token(Token::RSQBRACKET))
        parse_error("Expected ']' to close array");

      if (prc)
        prc->list_end();

      return true;
    }

  };

  // Document parser used by Any_parser

  struct Doc_parser
    : public Expr_parser<DPRC>
    , cdk::foundation::nocopy
  {
    typedef Expr_parser<DPRC>  Parser_base;

    using Parser_base::cur_pos;
    using Parser_base::end_pos;
    using Parser_base::tokens_available;
    using Parser_base::cur_token_type_is;
    using Parser_base::cur_token_type_in;
    using Parser_base::consume_token;
    using Parser_base::parse_error;


    Doc_parser(It &first, const It &last)
      : Expr_parser<DPRC>(first, last)
    {}

    /*
      Document parser treats document body as a list of
      key-value pairs. KV_parser parses single key-value
      pair and reports it to a document processor (using
      key_val() callback).
    */

    struct KV_parser;

    /*
      LPrc instance converts a document processor into
      a list processor that can process results of parsing
      a list of key-value pairs. Given document processor
      is returned for each pair in the list. This way a KV_parser
      which parses the key-value pair will report it to the
      document processor.
    */

    struct LPrc : public List_processor<DPRC>
    {
      using typename List_processor<DPRC>::Element_prc;
      DPRC *m_prc;

      LPrc(DPRC *prc) : m_prc(prc)
      {}

      void list_begin() {}
      void list_end()   {}

      Element_prc* list_el()
      {
        return m_prc ? m_prc : NULL;
      }
    };


    bool do_parse(DPRC *prc)
    {
      if (!consume_token(Token::LCURLY))
        return false;

      if (prc)
        prc->doc_begin();

      if (!cur_token_type_is(Token::RCURLY))
      {
        List_parser<KV_parser> kv_list(cur_pos(), end_pos());

        LPrc kv_prc(prc);
        bool ok = kv_list.parse(kv_prc);
        if (!ok)
          parse_error("Expected a key-value pair in a document");
      }

      if (!consume_token(Token::RCURLY))
        parse_error("Expected '}' closing a document");

      if (prc)
        prc->doc_end();

      return true;
    }

    // TODO: efficient skipping of documents

    // Parser for a single key-value pair.

    struct KV_parser
      : public Expr_parser<DPRC>
    {
      typedef Expr_parser<DPRC>  Parser_base;

      using Parser_base::cur_pos;
      using Parser_base::end_pos;
      using Parser_base::tokens_available;
      using Parser_base::cur_token_type_is;
      using Parser_base::cur_token_type_in;
      using Parser_base::consume_token;
      using Parser_base::parse_error;


      cdk::string m_key;

      KV_parser(It &first, const It &last)
        : Expr_parser<DPRC>(first, last)
      {}

      bool do_parse(DPRC *prc)
      {
        // Note: official JSON specs do not allow plain WORD as key name

        if (!cur_token_type_in({ Token::QQSTRING, Token::QSTRING, Token::WORD }))
          return false;

        m_key = consume_token()->get_text();

        if (!consume_token(Token::COLON))
          parse_error("Expected ':' after key name in a document");

        Any_parser val_parser(cur_pos(), end_pos());
        bool ok = val_parser.process_if(prc ? prc->key_val(m_key) : NULL);
        if (!ok)
          parse_error("Expected key value after ':' in a document");

        return true;
      }
    };
  };

};  // Any_parser


/*
  Expose document and array parsers from Any_parser<> in the
  main namespace.
*/

template <class Base, class SPRC = typename Base::Processor>
struct Doc_parser : public Any_parser<Base, SPRC>::Doc_parser
{
  Doc_parser(It &first, const It &last)
    : Any_parser<Base,SPRC>::Doc_parser(first, last)
  {}
};


template <class Base, class SPRC = typename Base::Processor>
struct Arr_parser : public Any_parser<Base, SPRC>::Arr_parser
{
  Arr_parser(It &first, const It &last)
    : Any_parser<Base,SPRC>::Arr_parser(first, last)
  {}
};


}  // parser

#endif
