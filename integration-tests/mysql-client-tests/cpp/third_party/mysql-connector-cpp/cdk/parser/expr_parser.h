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

#ifndef _EXPR_PARSER_H_
#define _EXPR_PARSER_H_

#include <mysql/cdk/common.h>
#include "parser.h"

PUSH_SYS_WARNINGS_CDK
#include <vector>
#include <map>
#include <locale>
#include <algorithm>  // for_each()
POP_SYS_WARNINGS_CDK


/*
  Parsing strings containing expressions as used by X DevAPI.
*/


/*
  List of reserved words used in DevAPI expressions.

  The list is given by KEYWORD_LIST() macro where each X(KKK,SSS) entry
  describes a keyword SSS with name KKK. Below enum value Keyword::KKK is
  defined for each keyword declared here.
*/

#undef IN

#define KEYWORD_LIST(X) \
    X(NOT, "not") \
    X(AND, "and") \
    X(OR, "or") \
    X(XOR, "xor") \
    X(IS, "is") \
    X(BETWEEN, "between") \
    X(L_TRUE, "true") \
    X(L_FALSE, "false") \
    X(L_NULL, "null") \
    X(LIKE, "like") \
    X(RLIKE, "rlike") \
    X(INTERVAL, "interval") \
    X(REGEXP, "regexp") \
    X(OVERLAPS, "overlaps")\
    X(ESCAPE, "escape") \
    X(HEX, "hex") \
    X(BIN, "bin") \
    X(MOD, "mod") \
    X(AS, "as") \
    X(USING, "using") \
    X(ASC, "asc") \
    X(DESC, "desc") \
    X(CAST, "cast") \
    X(CHARACTER, "character") \
    X(SET, "set") \
    X(CHARSET, "charset") \
    X(ASCII, "ascii") \
    X(UNICODE, "unicode") \
    X(BYTE, "byte") \
    X(BINARY, "binary") \
    X(CHAR, "char") \
    X(NCHAR, "nchar") \
    X(DATE, "date") \
    X(DATETIME, "datetime") \
    X(TIME, "time") \
    X(DECIMAL, "decimal") \
    X(SIGNED, "signed") \
    X(UNSIGNED, "unsigned") \
    X(INTEGER, "integer") \
    X(INT, "int") \
    X(JSON, "json") \
    X(IN, "in") \
    X(SOUNDS, "sounds") \
    X(LEADING, "leading") \
    X(TRAILING, "trailing") \
    X(BOTH, "both") \
    X(FROM, "from") \
    UNITS_LIST(X)

#define UNITS_LIST(X) \
    X(MICROSECOND, "microsecond") \
    X(SECOND, "second") \
    X(MINUTE, "minute") \
    X(HOUR, "hour") \
    X(DAY, "day") \
    X(WEEK, "week") \
    X(MONTH, "month") \
    X(QUARTER, "quarter") \
    X(YEAR, "year") \


/*
  List of operators that can appear in X DevAPI expressions.

  Each entry X(OOO,SSS,TTT,KKK) declares operator with name OOO, which is
  reported to CDK as string SSS. TTT is a set of tokens that map to this
  operator (usually just one token). KKK is a set of keywords that map to
  the token. Below an enum constant Op::OOO is defined for each operator
  declared here.

  TODO: Find good reference for operator names recognized by xprotocol
*/

#define OPERATOR_LIST(X) \
  UNARY_OP(X) \
  BINARY_OP(X)

#define UNARY_OP(X) \
  X(STAR, "*", {Token::STAR}, {}) \
  X(PLUS, "+", {Token::PLUS}, {}) \
  X(MINUS, "-", {Token::MINUS}, {}) \
  X(NEG, "!", {Token::BANG}, {})  \
  X(BITNEG, "~", {Token::TILDE}, {})  \
  X(NOT, "not", {}, {Keyword::NOT}) \

#define BINARY_OP(X) \
  X(ADD, "+", {Token::PLUS}, {})  \
  X(SUB, "-", {Token::MINUS}, {})  \
  X(MUL, "*", {Token::STAR}, {})  \
  X(DIV, "/", {Token::SLASH}, {})  \
  X(MOD, "%", {Token::PERCENT}, {Keyword::MOD})  \
  X(OR, "||", {Token::BAR2}, {Keyword::OR})  \
  X(AND, "&&", {Token::AMPERSTAND2}, {Keyword::AND})  \
  X(BITOR, "|", {Token::BAR}, {})  \
  X(BITAND, "&", {Token::AMPERSTAND}, {})  \
  X(BITXOR, "^", {Token::HAT}, {})  \
  X(LSHIFT, "<<", {Token::LSHIFT}, {})  \
  X(RSHIFT, ">>", {Token::RSHIFT}, {})  \
  X(EQ, "==", ({Token::EQ, Token::EQ2}), {})  \
  X(NE, "!=", ({Token::NE, Token::DF}), {})  \
  X(GT, ">", {Token::GT}, {})  \
  X(GE, ">=", {Token::GE}, {})  \
  X(LT, "<", {Token::LT}, {})  \
  X(LE, "<=", {Token::LE}, {}) \
  X(IS, "is", {}, {Keyword::IS}) \
  X(IS_NOT, "is_not", {}, {}) \
  X(IN, "in", {}, {Keyword::IN}) \
  X(NOT_IN, "not_in", {}, {}) \
  X(CONT_IN, "cont_in", {}, {}) \
  X(NOT_CONT_IN, "not_cont_in", {}, {}) \
  X(LIKE, "like", {}, {Keyword::LIKE}) \
  X(NOT_LIKE, "not_like", {}, {}) \
  X(RLIKE, "regexp", {}, {Keyword::RLIKE}) \
  X(NOT_RLIKE, "not_regexp", {}, {}) \
  X(BETWEEN, "between", {}, {Keyword::BETWEEN}) \
  X(NOT_BETWEEN, "not_between", {}, {}) \
  X(REGEXP, "regexp", {}, {Keyword::REGEXP}) \
  X(NOT_REGEXP, "not_regexp", {}, {}) \
  X(CAST, "cast", {}, {Keyword::CAST}) \
  X(SOUNDS_LIKE, "sounds like", {}, {Keyword::SOUNDS})\
  X(OVERLAPS, "overlaps", {}, {Keyword::OVERLAPS}) \
  X(NOT_OVERLAPS, "not_overlaps", {}, {}) \



namespace parser {

using string = std::string;


/*
  Class which manages reserved words.

  For a given token, it can recognize if it is a reserved word and return
  the enum value identifying this keyword.
*/

class Keyword
{
public:

  /*
    For each reserved word with name NNN from RESERVED_LIST(),
    declare enum constant Keyword::NNN.
  */

#define kw_enum(A,B)  A,

  enum Type {
    NONE,
    KEYWORD_LIST(kw_enum)
  };

  typedef std::set<Type> Set;

  /*
    Check if given token is a keyword, and if yes, return enum constant of
    this keyword. If the token is not a keyword it returns NONE.
  */

  static Type get(const Token &tok);

  // Return canonical name of a keyword.

  static const char* name(Type kk)
  {

#define kw_name(A,B)  case Keyword::A: return #A;

    switch (kk)
    {
      KEYWORD_LIST(kw_name)
    default: return NULL;
    }
  }

  /*
    Case insensitive string comparison function which is used to match
    keywords.

    TODO: First argument can be a cdk string - avoid utf8 conversion.
    TODO: Simpler implementation that is not sensitive to locale settings.
  */

  static bool equal(const string &a, const string &b)
  {
    static kw_cmp  cmp;

    return (!cmp(a, b) && !cmp(b, a));
  }

private:

  struct kw_cmp
  {
    struct char_cmp
    {
      bool operator()(char, char) const;
    };

    bool operator()(const std::string &a, const std::string &b) const
    {
      static char_cmp cmp;
      return std::lexicographical_compare(
        a.begin(), a.end(),
        b.begin(), b.end(), cmp
      );
    }
  };

  /*
    Map which maps words to keyword ids. Case insensitive comparison is
    used to locate words in this map.
  */

  typedef std::map<std::string, Type, kw_cmp> map_t;

  static map_t kw_map;

  /*
    Default ctor builds the keyword map based on keyword declarations given
    by KEYWORD_LIST() macro.
  */

  Keyword()
  {

#define kw_add(A,B)  kw_map[B] = A;

    KEYWORD_LIST(kw_add);
  }

  // This initializer instance makes sure that the keyword map is built.

  static Keyword init;
};


inline
Keyword::Type Keyword::get(const Token &t)
{
  // Only WORD tokens can be a keyword.

  if (Token::WORD != t.get_type())
    return NONE;

  // Locate WORD in the keyword map.
  cdk::bytes data = t.get_bytes();

  auto x = kw_map.find(std::string((const char*)data.begin(), data.size()));

  return (x == kw_map.end() ? NONE : x->second);
}


inline
bool operator==(Keyword::Type type, const Token &tok)
{
  return type == Keyword::get(tok);
}


#if 1

inline
bool Keyword::kw_cmp::char_cmp::operator()(char a, char b) const
{
  /*
    Note: Explicitly using "C" locale's ctype facet to not depend on
    default locale settings. This comparison needs to work correctly only
    for ASCII characters. (only these characters are used in keywords).
  */

  static std::locale c_loc("C");
  static const auto &ctf
    = std::use_facet<std::ctype<char>>(c_loc);

  return ctf.tolower(a) < ctf.tolower(b);
}

#endif

// --------------------------------------------------------------------------

/*
  Class managing operators that can appear in X DevAPI expressions.

  This class can recognize if given token names a valid operator. Note that
  the same token can name a unary and a binary operator.
*/

class Op
{
public:

  /*
    Define enum constant for each operator declared by UNARY/BINARY_OP()
    macros.
  */

#define op_enum(A,B,T,K)  A,

  enum Type {
    NONE,
    UNARY_OP(op_enum)
    BINARY_START,
    BINARY_OP(op_enum)
  };

  typedef std::set<Type> Set;

  /*
    Check if given token names a unary operator and if yes return this operator
    id. If the token is not a unary operator returns NONE.
  */

  static Type get_unary(const Token &tok);

  /*
    Check if given token names a binary operator and if yes return this operator
    id. If the token is not a binary operator returns NONE.
  */

  static Type get_binary(const Token &tok);

  static const char* name(Type tt)
  {

#define op_name(A,B,T,K)  case Op::A: return B;

    switch (tt)
    {
      OPERATOR_LIST(op_name)
    case NONE:
    case BINARY_START:
    default:
      return NULL;
    }
  }

private:

  /*
    Maps used to recognize operators.

    Operator can be a keyword or other token. For each kind of operator (unary
    or binary) we have two maps. One map maps keyword ids to operators. The
    other map maps other token types to operators. These maps are filled based
    on the information given by UNARY/BINARY_OP() macros that declare
    operators.
  */

  typedef std::map<Token::Type, Type>   tok_map_t;
  typedef std::map<Keyword::Type, Type> kw_map_t;

  static tok_map_t  unary_tok_map;
  static kw_map_t   unary_kw_map;

  static tok_map_t  binary_tok_map;
  static kw_map_t   binary_kw_map;

  Op()
  {

#define op_add(X, A,B,T,K) \
  for(Token::Type tt : Token::Set T) \
    X##_tok_map[tt] = Op::A; \
  for(Keyword::Type kk : Keyword::Set K) \
    X##_kw_map[kk] = Op::A;

#define op_add_unary(A,B,T,K)   op_add(unary,A,B,T,K)
#define op_add_binary(A,B,T,K)  op_add(binary,A,B,T,K)

    UNARY_OP(op_add_unary)
    BINARY_OP(op_add_binary)
  }

  static Op init;
};


inline
Op::Type Op::get_unary(const Token &tok)
{
  // First check the token map.

  auto find = unary_tok_map.find(tok.get_type());
  if (find != unary_tok_map.end())
    return find->second;

  // If operator not found, try keyword map.

  Keyword::Type kw = Keyword::get(tok);
  if (Keyword::NONE == kw)
    return NONE;
  auto find1 = unary_kw_map.find(kw);
  return find1 == unary_kw_map.end() ? NONE : find1->second;
}


inline
Op::Type Op::get_binary(const Token &tok)
{
  auto find = binary_tok_map.find(tok.get_type());
  if (find != binary_tok_map.end())
    return find->second;
  Keyword::Type kw = Keyword::get(tok);
  if (Keyword::NONE == kw)
    return NONE;
  auto find1 = binary_kw_map.find(kw);
  return find1 == binary_kw_map.end() ? NONE : find1->second;
}


inline
bool operator==(Op::Type tt, const Token &tok)
{
  if (tt > Op::BINARY_START)
    return tt == Op::get_binary(tok);
  else
    return tt == Op::get_unary(tok);
}


// --------------------------------------------------------------------------

/*
  Specialization of Token_base which adds methods that recognize keywords,
  operators and sets of these.
*/

class Expr_token_base
  : public Token_base
{
public:

  const Token* consume_token(Keyword::Type kk)
  {
    if (!cur_token_type_is(kk))
      return NULL;
    return consume_token();
  }

  const Token* consume_token(const Keyword::Set &kws)
  {
    if (!cur_token_type_in(kws))
      return NULL;
    return consume_token();
  }

  const Token* consume_token(Op::Type op)
  {
    if (!cur_token_type_is(op))
      return NULL;
    return consume_token();
  }

  const Token* consume_token(const Op::Set &ops)
  {
    if (!cur_token_type_in(ops))
      return NULL;
    return consume_token();
  }

  using Token_base::consume_token;


  const Token& consume_token_throw(Keyword::Type kk, const string &msg)
  {
    const Token *t = consume_token(kk);
    if (!t)
      parse_error(msg);
    return *t;
  }

  using Token_base::consume_token_throw;


  bool cur_token_type_is(Keyword::Type kk)
  {
    const Token *t = peek_token();
    return (NULL != t) && (kk == *t);
  }

  bool cur_token_type_is(Op::Type op)
  {
    const Token *t = peek_token();
    return (NULL != t) && (op == *t);
  }

  using Token_base::cur_token_type_is;


  bool cur_token_type_in(const Keyword::Set &kws)
  {
    const Token *t = peek_token();
    if (!t)
      return false;
    return kws.find(Keyword::get(*t)) != kws.end();
  }

  bool cur_token_type_in(const Op::Set &ops)
  {
    const Token *t = peek_token();
    if (!t)
      return false;
    Op::Type op = Op::get_binary(*t);
    if (ops.find(op) != ops.end())
      return true;
    return ops.find(Op::get_unary(*t)) != ops.end();
  }

  using Token_base::cur_token_type_in;

  friend class Expression_parser;
};


}  // parser



namespace parser {

using cdk::scoped_ptr;
using cdk::Expression;


// ------------------------------------------------------------------------------

/*
  Helper classes that are used to store column references and document
  paths within the parser.
*/

struct Table_ref : public cdk::api::Table_ref
{
  struct : public cdk::api::Schema_ref
  {
    cdk::string m_name;

    virtual const cdk::string name() const { return m_name; }

  } m_schema_ref;

  cdk::string m_name;

  virtual const cdk::string name() const { return m_name; }

  virtual const cdk::api::Schema_ref* schema() const
  { return m_schema_ref.m_name.empty() ? NULL : &m_schema_ref; }

  void set(const cdk::string &name)
  { m_name = name; }

  void set(const cdk::string &name, const cdk::string &schema)
  {
    m_name = name;
    m_schema_ref.m_name = schema;
  }

  void clear()
  {
    m_name.clear();
    m_schema_ref.m_name.clear();
  }

};


struct Column_ref : public cdk::api::Column_ref
{
  Table_ref m_table_ref;


  cdk::string m_col_name;

  virtual const cdk::string name() const
  { return m_col_name; }

  virtual const cdk::api::Table_ref *table() const
  {
    return m_table_ref.m_name.empty() ? NULL :  &m_table_ref;
  }


  void set_name(const cdk::string &name)
  { m_col_name = name; }

  void set(const cdk::string &name)
  {
    m_table_ref.clear();
    set_name(name);
  }

  void set(const cdk::string &name, const cdk::string &table)
  {
    set(name);
    m_table_ref.set(table);
  }

  void set(const cdk::string &name,
           const cdk::string &table, const cdk::string &schema)
  {
    set(name);
    m_table_ref.set(table, schema);
  }


  Column_ref& operator=(const cdk::api::Column_ref &other)
  {
    m_col_name = other.name();
    if (!other.table())
      return *this;
    if (other.table()->schema())
      m_table_ref.set(other.table()->name(), other.table()->schema()->name());
    else
      m_table_ref.set(other.table()->name());
    return *this;
  }

  void clear()
  {
    m_col_name.clear();
    m_table_ref.clear();
  }
};


/*
  Trivial Format_info class that is used to report opaque blob values.
*/

struct Format_info : public cdk::Format_info
{
  bool for_type(cdk::Type_info ti) const { return cdk::TYPE_BYTES == ti; }
  void get_info(cdk::Format<cdk::TYPE_BYTES>&) const {}

  // bring in the rest of overloads that would be hidden otherwise
  // (avoid compiler warning)
  using cdk::Format_info::get_info;
};


// ------------------------------------------------------------------------------

/*
  Main parser class containing parsing logic. An instance acts
  as Expression, that is, parsed expression can be visited
  by expression processor with process() method.

  There are 2 parsing modes which determine what kind of value references
  are allowed within expression. In TABLE mode expression can refer
  to columns of a table, in DOCUMENT mode it can refer to document elements
  specified by a document path.
*/

struct Parser_mode
{
  enum value { DOCUMENT, TABLE};
};


class Expr_parser_base
    : public Expr_parser<Expression::Processor, Expr_token_base>
{

public:

  typedef Expression::Processor Processor;
  typedef Expression::Scalar::Processor Scalar_prc;
  typedef cdk::api::Doc_path::Processor Path_prc;

  static Expression::Processor *get_base_prc(Processor *prc)
  { return prc; }

protected:

  /*
    TODO: Temporary hack to meet current specs that inside expression
    "x IN ('a', 'b', 'c')" the strings in the list should be reported as
    octets, not as strings. This will probably change but in the meantime,
    to not fail the tests, the m_strings_as_blobs flag tells to report
    strings as octets. This happens only in case of IN expression
    (see parse_iliri() method).
  */

  Parser_mode::value m_parser_mode;
  bool m_strings_as_blobs;

  Expr_parser_base(It &first, const It &last,
                   Parser_mode::value parser_mode,
                   bool strings_as_blobs = false)
    : Expr_parser<Expression::Processor, Expr_token_base>(first, last)
    , m_parser_mode(parser_mode)
    , m_strings_as_blobs(strings_as_blobs)
  {
    return;
  }


  bool do_parse(Processor *prc);


  enum Start { FULL, ATOMIC, MUL, ADD, SHIFT, BIT, COMP, ILRI, AND, OR,
               CAST_TYPE, COLID_DOCPATH, DOC, ARR};

  /*
    Parse tokens using given starting point of the expression grammar.

    If processor is not NULL, the expression is reported directly to the
    processor and this method returns NULL.

    Otherwise, if processor is NULL, the result of parsing is stored
    and returned from the method. Caller of this method takes ownership
    of the returned Expression object.
  */

  Expression* parse(Start, Processor*);

  // methods for parsing different kinds of expressions.

  Expression* parse_atomic(Processor*);
  Expression* parse_mul(Processor*);
  Expression* parse_add(Processor*);
  Expression* parse_shift(Processor*);
  Expression* parse_bit(Processor*);
  Expression* parse_comp(Processor*);
  Expression* parse_ilri(Processor*);
  Expression* parse_and(Processor*);
  Expression* parse_or(Processor*);

  // Additional helper parsing methods.

  Expression* left_assoc_binary_op(const Op::Set&, Start, Start, Processor*);

  bool parse_function_call(const cdk::api::Table_ref&, Scalar_prc*);
  void parse_argslist(Expression::List::Processor*,
                      bool strings_as_blobs = false);
  void parse_special_args(
    const cdk::api::Table_ref&,
    Expression::List::Processor*
  );

  bool parse_schema_ident(Token::Type (*types)[2] = NULL);
  void parse_column_ident(Processor *);
  void parse_column_ident1(Processor*);
  bool get_ident(string&);

  void parse_document_field(Path_prc*, bool prefix = false);
  void parse_document_field(const string&, Path_prc*);
  void parse_document_field(const string&, const string&, Path_prc*);

  bool parse_document_path(Path_prc*, bool require_dot=false);
  bool parse_document_path1(Path_prc*);
  bool parse_docpath_member(Path_prc*);
  bool parse_docpath_member_dot(Path_prc*);
  bool parse_docpath_array(Path_prc*);

  bool parse_cast(Scalar_prc*);
  std::string parse_cast_type();
  std::string cast_data_type_dimension(bool double_dimension = false);

  void parse_doc(Processor::Doc_prc*);
  void parse_arr(Processor::List_prc*);

private:

  Column_ref m_col_ref;

  friend class Expression_parser;
  friend class Order_parser;
  friend class Projection_parser;
  friend class Table_field_parser;
  friend class Doc_field_parser;
};


class Expression_parser
  : public Expression
{
  Tokenizer m_tokenizer;
  Parser_mode::value m_mode;

public:


  Expression_parser(Parser_mode::value parser_mode, bytes expr)
    : m_tokenizer(expr), m_mode(parser_mode)
  {}


  void process(Processor &prc) const
  {

    It first = m_tokenizer.begin();
    It last  = m_tokenizer.end();

    if (m_tokenizer.empty())
      throw Expr_token_base::Error(first, "Expected an expression");

    /*
      note: passing m_toks.end() directly as constructor argument results
      in "incompatible iterators" exception when comparing iterators (at
      least on win, vs2010). problem with passing temporary object?
    */

    Expr_parser_base parser(first, last, m_mode);
    parser.process(prc);

    if (first != last)
      throw Expr_token_base::Error(
              first,
              "Unexpected characters after expression"
            );
  }

};


/**
   @brief The Order_parser class parses "<expr> [ASC|DESC]" using
          Order_expr_processor

          This parser can process api::Order_expr<Expression>.

          Usage:

          When processing api::Order_by::Processor user needs to call first
          Processor::list_begin() and then pass the processor returned by
          Processor::list_el() to  Order_parser for each projection.
          In the end, Processor::list_end() has to be called.
 */

class Order_parser
    : public cdk::api::Order_expr<Expression>
    , Token_base
{
  Tokenizer m_tokenizer;
  Parser_mode::value m_mode;

public:

  Order_parser(Parser_mode::value parser_mode, bytes expr)
    : m_tokenizer(expr), m_mode(parser_mode)
  {}

  void parse(Processor &prc);

  void process(Processor &prc) const
  {
    const_cast<Order_parser*>(this)->parse(prc);
  }

};


/**
   @brief The Projection_parser class parses "<expr> AS <alias>"
          specifications. When used in table mode the "AS <alias>" part
          is optional, otherwise error is thrown if it is not present.

          This parser has can process 2 processor types:
          api::Projection_expr<Expression>::Processor
          Expression::Document::Processor

          Usage:

          When processing api::Projection::Processor user needs to call first
          Processor::list_begin() and then pass the processor returned by
          Processor::list_el() to  Projection_parser for each projection.
          In the end, Processor::list_end() has to be called.

          When processing Expression::Document::Processor user will call
          Processor::doc_begin() and then pass the processor to
          Projection_parser for each projection.
          In the end user will call Processor::doc_end()
 */

class Projection_parser
  : public cdk::api::Projection_expr<Expression>
  , public cdk::Expression::Document
  , Expr_token_base
{
  typedef cdk::api::Projection_expr<Expression>::Processor Projection_processor;
  typedef cdk::Expression::Document::Processor Document_processor;
  Tokenizer m_tokenizer;
  Parser_mode::value m_mode;
  It m_it;

public:

  Projection_parser(Parser_mode::value parser_mode, bytes expr)
    : m_tokenizer(expr), m_mode(parser_mode)
  {
    m_it = m_tokenizer.begin();
    set_tokens(m_it, m_tokenizer.end());
  }

  void parse_tbl_mode(Projection_processor &prc);
  void parse_doc_mode(Document_processor &prc);

  void process(Projection_processor &prc) const
  {
    const_cast<Projection_parser*>(this)->parse_tbl_mode(prc);
  }

  void process(Document_processor &prc) const
  {
    const_cast<Projection_parser*>(this)->parse_doc_mode(prc);
  }

};


/*
  This class acts as cdk::Doc_path object taking path data from a string
  containing document field specification (documentField grammar)
*/

class Doc_field_parser
    : public cdk::Doc_path
{
  Tokenizer m_tokenizer;
  cdk::scoped_ptr<Expr_parser_base> m_parser;
  It m_it;

public:

  Doc_field_parser(bytes doc_path)
    : m_tokenizer(doc_path)
  {
    m_it = m_tokenizer.begin();
    const It end = m_tokenizer.end();
    m_parser.reset(new Expr_parser_base(m_it, end, Parser_mode::DOCUMENT));
  }

  void process(Processor &prc) const
  {
    const_cast<Expr_parser_base*>(m_parser.get())->parse_document_field(&prc);

    if (m_parser->tokens_available())
      m_parser->parse_error("Unexpected characters at the end");
  }
};


// ------------------------------------------------------------------------------

/*
  Infrastructure for storing partial parsing results.

  An infix operator expression of the form:

    LHS OP RHS

  is reported to an expression processor so that first the name of the
  operator is reported, then the list of arguments (LHS,RHS).

  For that reason expression parser can not report LHS expression to the
  processor on-the-fly, while parsing it. Only after seeing and reporting
  OP, it can report LHS expression to the processor. Therefore, parser
  needs to store the result of parsing LHS for later reporting.

  Classes defined here provide infrastructure for storing parsed expression.
  An instance of Stored_expr class acts as expression processor and as an
  expression. When used as expression processor, it stores the expression
  reported to it via processor callbacks. Later, when used as an expression,
  it "re-plays" the stored expression to a given processor.

*/

struct Stored_expr
  : public Expression
  , public Expression::Processor
{
  virtual ~Stored_expr() {}
};


struct Stored_scalar;
struct Stored_list;
struct Stored_doc;


/*
  Storage for any kind of expression: either scalar, array or document.
*/

struct Stored_any
  : public Stored_expr
{
  scoped_ptr<Stored_scalar> m_scalar;
  scoped_ptr<Stored_list>   m_arr;
  scoped_ptr<Stored_doc>    m_doc;

  void process(Processor &prc) const;

  Scalar_prc* scalar();
  List_prc*   arr();
  Doc_prc*    doc();
};


// --------------------------------------------------------------------------


struct Stored_list
  : public Expression::List
  , public Expression::List::Processor
{
  typedef std::vector<Stored_expr*> Ptr_list;
  Ptr_list m_elements;

  ~Stored_list()
  {
    std::for_each(m_elements.begin(), m_elements.end(), delete_el);
  }

  static void delete_el(Stored_expr *ptr)
  {
    delete ptr;
  }

  // List expression (report stored list)

  void process(Processor &prc) const
  {
    for (Ptr_list::const_iterator it = m_elements.begin();
         it != m_elements.end();
         ++it)
      (*it)->process_if(prc.list_el());
  }

  // List processor (store list)

  void list_begin()
  {
    m_elements.clear();
  }

  void list_end() {}

  Element_prc* list_el()
  {
    Stored_expr *e = new Stored_any();
    m_elements.push_back(e);
    return e;
  }
};


struct Stored_doc
  : public Expression::Document
  , public Expression::Document::Processor
{
  typedef std::map<cdk::string, scoped_ptr<Stored_expr> > Ptr_map;
  Ptr_map m_keyval_map;

  // Doc expression (report stored doc)

  void process(Processor &prc) const
  {
    prc.doc_begin();
    for (Ptr_map::const_iterator it = m_keyval_map.begin();
         it != m_keyval_map.end();
         ++it)
    {
      Any_prc *aprc = prc.key_val(it->first);
      it->second->process_if(aprc);
    }
    prc.doc_end();
  }

  // Doc processor (store doc)

  void doc_begin()
  {
    m_keyval_map.clear();
  }

  void doc_end()
  {}

  Any_prc* key_val(const cdk::string &key)
  {
    Stored_expr *s = new Stored_any();
    m_keyval_map[key].reset(s);
    return s;
  }
};


/*
  Storage for base (scalar) expressions.
*/

struct Stored_scalar
  : public Expression::Scalar
  , public Expression::Scalar::Processor
  , public Expression::List::Processor
  , public cdk::Value_processor
{
  // Storage for the values

  parser::Column_ref  m_col_ref;
  cdk::Doc_path_storage m_doc_path;
  std::string m_op_name;
  cdk::string m_str;

  union {
    int64_t     m_int;
    uint64_t    m_uint;
    float       m_float;
    double      m_double;
    bool        m_bool;
  }
  m_num;

  /*
    Storage for arguments of function or operator call.

    As an optimization, the first argument in the list can be taken
    from external storage. This is used in infix operator parsing logic.
    When parsing:

      LHS OP RHS

    the parser first parses and stores LHS. Now, if the whole operator
    expression needs to be stored as well, then the stored LHS can be
    re-used for the first argument of the operator call.

    For that reason here we have m_first member which can point to
    externally created storage for the first argument (Stored_scalar
    takes ownership of all the stored arguments, including the first one).
    If m_first is empty, then only arguments stored in m_args are used.
  */

  scoped_ptr<Expression>  m_first;
  Stored_list  m_args;

  Stored_scalar() {}
  Stored_scalar(Expression *first)
  {
    m_first.reset(first);
  }

  enum { OP, FUNC, COL_REF, PATH, PARAM, VAR,
         V_NULL, V_OCTETS, V_STR, V_INT, V_UINT,
         V_FLOAT, V_DOUBLE, V_BOOL }
  m_type;

  // Scalar expression (report stored value to a processor)

  void process(Processor &prc) const
  {
    switch (m_type)
    {
    case OP:
    case FUNC:
      {
        Args_prc *argsp
          = (m_type == OP ? prc.op(m_op_name.c_str())
                          : prc.call(*m_col_ref.table()) );
        if (!argsp)
          return;

        argsp->list_begin();
        // if we have externally stored first argument, use it here
        if (m_first)
          m_first->process_if(argsp->list_el());
        // the rest of arguments
        m_args.process(*argsp);
        argsp->list_end();
      };
      break;

    case COL_REF:
      prc.ref(m_col_ref, m_doc_path.is_empty() ? NULL : &m_doc_path);
      break;

    case PATH:  prc.ref(m_doc_path); break;
    case PARAM: prc.param(m_str); break;
    case VAR:   prc.var(m_str); break;

    // literal values

    case V_NULL:   safe_prc(prc)->val()->null(); break;
    case V_STR:    safe_prc(prc)->val()->str(m_str); break;
    case V_INT:    safe_prc(prc)->val()->num(m_num.m_int); break;
    case V_UINT:   safe_prc(prc)->val()->num(m_num.m_uint); break;
    case V_FLOAT:  safe_prc(prc)->val()->num(m_num.m_float); break;
    case V_DOUBLE: safe_prc(prc)->val()->num(m_num.m_double); break;
    case V_BOOL:   safe_prc(prc)->val()->yesno(m_num.m_bool); break;

    case V_OCTETS:
      // note: this object acts as Format_info
      safe_prc(prc)->val()->value(cdk::TYPE_BYTES, Format_info(), cdk::bytes(m_op_name));
      break;
    }
  }

  // Processors (store value reported to us)

  // List processor (this is used to process function/op call argument list)

  bool m_first_el;

  void list_begin()
  {
    m_args.list_begin();
    m_first_el = true;
  }

  void list_end()
  {
    m_args.list_end();
  }

  Element_prc* list_el()
  {

    /*
      If we use externally stored first argument (m_first is not empty)
      then there is no need to look at the first argument reported to us,
      because we already have it. In this case we return NULL from here.

      If this is second or later argument, or if m_first is empty,
      we forward to m_args.
    */

    if (m_first_el)
    {
      m_first_el = false;
      if (m_first)
        return NULL;
    }
    return m_args.list_el();
  }


  // Scalar processor

  Value_prc* val()
  {
    return this;
  }

  Args_prc* op(const char *name)
  {
    m_type = OP;
    m_op_name = name;
    return this;
  }

  Args_prc* call(const Object_ref &func)
  {
    m_type = FUNC;

    // Set the table() part of m_col_ref to the name of the function

    if (func.schema())
      m_col_ref.set(string(), func.name(), func.schema()->name());
    else
      m_col_ref.set(string(), func.name());

    return this;
  }

  void ref(const Column_ref &col, const Doc_path *path)
  {
    m_type = COL_REF;
    m_col_ref   = col;
    if (path)
      path->process(m_doc_path);
  }

  void ref(const Doc_path &path)
  {
    m_type = PATH;
    path.process(m_doc_path);
  }

  void param(const string &name)
  {
    m_type = PARAM;
    m_str  = name;
  }

  virtual void param(uint16_t)
  {
    THROW("Positional parameter in expression");
  }

  virtual void var(const string &name)
  {
    m_type = VAR;
    m_str  = name;
  }

  // Value processor

  void null() { m_type = V_NULL; }

  void str(const string &val)
  {
    m_type = V_STR;
    m_str  = val;
  }

  void num(int64_t val)
  {
    m_type = V_INT;
    m_num.m_int = val;
  }

  void num(uint64_t val)
  {
    m_type = V_UINT;
    m_num.m_uint = val;
  }

  void num(float val)
  {
    m_type = V_FLOAT;
    m_num.m_float = val;
  }

  void num(double val)
  {
    m_type = V_DOUBLE;
    m_num.m_double = val;
  }

  void yesno(bool val)
  {
    m_type = V_BOOL;
    m_num.m_bool = val;
  }

  void value(cdk::Type_info, const cdk::Format_info&, cdk::bytes data)
  {
    // TODO: currently we ignore type information and treat everything
    // as opaque byte blobs.

    m_type = V_OCTETS;
    m_op_name.assign(data.begin(), data.end());
  }

};

// --------------------------------------------------------------------------


inline
void Stored_any::process(Processor &prc) const
{
  if (m_scalar)
    return m_scalar->process_if(prc.scalar());

  if (m_arr)
  {
    List_prc *lprc = prc.arr();
    if (!lprc)
      return;
    lprc->list_begin();
    m_arr->process(*lprc);
    lprc->list_end();
    return;
  }

  if (m_doc)
    return m_doc->process_if(prc.doc());
}

inline
Stored_any::Scalar_prc* Stored_any::scalar()
{
  m_scalar.reset(new Stored_scalar());
  return m_scalar.get();
}

inline
Stored_any::List_prc* Stored_any::arr()
{
  m_arr.reset(new Stored_list());
  return m_arr.get();
}

inline
Stored_any::Doc_prc* Stored_any::doc()
{
  m_doc.reset(new Stored_doc());
  return m_doc.get();
}


/*
  Storage of operator call expression that can re-use already
  stored LHS expression.
*/

struct Stored_op
  : public Stored_expr
  , public Stored_scalar
{
  using Stored_expr::process;
  using Stored_scalar::process;

  Stored_op(Expression *lhs)
    : Stored_scalar(lhs)
  {}

  void process(Expression::Processor &prc) const
  {
    Stored_scalar::process_if(prc.scalar());
  }

  // Store reported operator call.

  Scalar_prc* scalar() { return this; }
  List_prc* arr()      { assert(false); return NULL; }
  Doc_prc*  doc()      { assert(false); return NULL; }
};


/*
  Storage for ILRI expressions that can re-use already stored
  first part of the expression.

  When reporting stored expression, it can wrap it in unary
  "not" operator if requested.
*/

struct Stored_ilri
  : public Stored_expr
  , Stored_scalar
{
  using Stored_expr::process;
  using Stored_scalar::process;

  Stored_ilri(Expression *first)
    : Stored_scalar(first)
  {}

  void process(Expression::Processor &prc) const
  {
    Scalar_prc *sprc = prc.scalar();

    if (!sprc)
      return;

    Stored_scalar::process(*sprc);
    return;
  }

  // Store reported ILRI expression.

  Scalar_prc* scalar() { return this; }
  List_prc* arr() { assert(false); return NULL; }
  Doc_prc*  doc() { assert(false); return NULL; }

};


// --------------------------------------------------------------------------


inline
Expression* Expr_parser_base::parse(Start start, Processor *prc)
{
  switch (start)
  {
  case FULL:   return parse_or(prc);
  case ATOMIC: return parse_atomic(prc);
  case MUL:    return parse_mul(prc);
  case ADD:    return parse_add(prc);
  case SHIFT:  return parse_shift(prc);
  case BIT:    return parse_bit(prc);
  case COMP:   return parse_comp(prc);
  case ILRI:   return parse_ilri(prc);
  case AND:    return parse_and(prc);
  case OR:     return parse_or(prc);

  case DOC:
  case ARR:
    {
      scoped_ptr<Stored_expr> stored;

      if (!prc)
      {
        stored.reset(new Stored_any());
        prc = stored.get();
      }

      if (DOC == start)
        parse_doc(prc->doc());
      else
        parse_arr(prc->arr());

      return stored.release();
    }

  default:
    assert(false && "Invalid parser state");
    return NULL;
  }
}


/*
  Class Used to parse Table fields.
  Format: table.column->@.field.arr[]
*/

class Table_field_parser
    : public cdk::api::Column_ref
    , public cdk::Doc_path
{
  parser::Column_ref    m_col;
  Stored_any m_path;

public:

  Table_field_parser(bytes table_field)
  {
    Tokenizer toks(table_field);

    It begin = toks.begin();
    const It end = toks.end();

    Expr_parser_base parser(begin, end, Parser_mode::TABLE);
    parser.parse_column_ident(&m_path);
    m_col = parser.m_col_ref;
  }

  const cdk::string name() const
  {
    return m_col.name();
  }

  const cdk::api::Table_ref *table() const
  {
    return m_col.table();
  }

  bool has_path() const
  {
    return m_path.m_scalar && !m_path.m_scalar->m_doc_path.is_empty();
  }

  void process(Processor &prc) const
  {
    if(m_path.m_scalar)
      m_path.m_scalar->m_doc_path.process(prc);
  }

};


}  // parser

#endif
