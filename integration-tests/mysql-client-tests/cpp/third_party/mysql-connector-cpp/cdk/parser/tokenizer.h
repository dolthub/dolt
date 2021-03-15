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

#ifndef _TOKENIZER_H_
#define _TOKENIZER_H_

#include <mysql/cdk.h>

#include "char_iterator.h"

PUSH_SYS_WARNINGS_CDK
#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <stdexcept>
#include <sstream>
#include <algorithm>
POP_SYS_WARNINGS_CDK

#ifdef _MSC_VER
DISABLE_WARNING_CDK(4061)  // not all enums listed inside switch() statement
#endif

#undef WORD

/*
  Definitions of tokens recognized by tokenizer.

  Each macro TOKEN_LIST(), SYMBOL_LIST1() and SYMBOL_LSIT2() defines list
  of tokens with the following entry for each token:

    X(NNN,SSS)

  where Token::NNN is this token's enum constant and SSS, if not NULL, defines
  characters of the token. For tokens which are not simple 1 or 2 character
  sequences, but are recognized by tokenizer logic, SSS is NULL.
*/

#define TOKEN_LIST(X) \
    X(WORD, NULL) \
    X(QWORD, NULL)    /* word in backtick quotes */ \
    X(QSTRING, NULL)  /* string in single quotes */ \
    X(QQSTRING, NULL) /* string in double quotes */ \
    X(NUMBER, NULL)   /* floating number */ \
    X(INTEGER, NULL)  /* integer number */ \
    X(HEX, NULL)      /* hexadecimal number*/\
    SYMBOL_LIST1(X) \
    SYMBOL_LIST2(X) \

// 2 char symbols

#define SYMBOL_LIST2(X) \
    X(NE, "!=") \
    X(DF, "<>") \
    X(GE, ">=") \
    X(LE, "<=") \
    X(LSHIFT, "<<") \
    X(RSHIFT, ">>") \
    X(DOUBLESTAR, "**") \
    X(ARROW2, "->>") \
    X(ARROW, "->") \
    X(AMPERSTAND2, "&&") \
    X(BAR2, "||") \
    X(EQ2, "==")

// 1 char symbols

#define SYMBOL_LIST1(X) \
    X(LPAREN,"(") \
    X(RPAREN,")") \
    X(LCURLY, "{") \
    X(RCURLY, "}") \
    X(LSQBRACKET,"[") \
    X(RSQBRACKET,"]") \
    X(DOT, ".") \
    X(COMMA, ",") \
    X(EQ, "=") \
    X(GT, ">") \
    X(LT, "<") \
    X(AMPERSTAND, "&") \
    X(BAR, "|") \
    X(HAT, "^") \
    X(PLUS, "+") \
    X(MINUS, "-") \
    X(STAR, "*") \
    X(SLASH, "/") \
    X(PERCENT, "%") \
    X(BANG, "!") \
    X(TILDE, "~") \
    X(QUESTION, "?") \
    X(COLON, ":") \
    X(DOLLAR, "$") \
    /*X(AT,"@")*/



namespace parser {

  using cdk::byte;
  using cdk::bytes;
  using cdk::char_t;
  using cdk::invalid_char;

  class Token;
  class iterator;

  /*
    Base class for all parser and tokenizer errors.

    This class can be used in catch handlers to catch all errors
    related to parsing.
  */

  struct Error : public cdk::Error
  {
    Error() = delete;
    using cdk::Error::Error;
  };


  /*
    Tokenizer and parser error base which shows parsing context in error
    description.

    Instances of Error keep parsing context information which consists of
    the current parser position within the string, and fragments of the string
    before and after parsing position. This information is stored directly in
    the error object and uses statically allocated memory to avoid dynamic memory
    allocation at the time when error is thrown.

    Parser errors use error code cdkerrc::parser_error in the generic cdk
    category. Unless overridden, parser errors produce error descriptions of
    the form:

      "CTX: MSG"

    where MSG is the message passed to the error constructor and CTX describes
    position of the parser in the parsed string. It can be something like
    "After seeing '...', looking at '...'" (see print_ctx() for exact forms of
    the context string).

    Note: This class template is parametrized by the string type, which can
    be either a wide or a standard string, depending on which strings the
    parser is working on (we have both cases). Remaining template parameters
    specify sizes of buffers used to store input string fragments.
  */

  constexpr  size_t  seen_buf_len = 64;
  constexpr  size_t  ahead_buf_len = 12;


  class Error_base
    : public cdk::Error_class< Error_base, parser::Error >
  {
    using Base = cdk::Error_class< Error_base, parser::Error >;

    Error_base() = delete;

  protected:

    using string = std::string;

  public:

    /*
      Parser error with description 'descr' and parsing context specified
      by remaining arguments (see set_ctx() for possibilities).
    */

    template<typename... Ty>
    Error_base(
      const string &descr,
      Ty&&... args
    )
      : Base(nullptr, cdk::cdkerrc::parse_error)
      , m_msg(descr)
    {
      set_ctx(std::forward<Ty>(args)...);
    }

    virtual ~Error_base() throw ()
    {}

  protected:

    // Storage for context data.

    char  m_seen[seen_buf_len];   // Characters seen before current position.
    char  m_ahead[ahead_buf_len]; // Few characters ahead of the current

    void set_ctx(char_iterator &pos);
    void set_ctx(const std::string&, size_t pos);

    string   m_msg;

    //  Print parser context description to the given ostream.

    virtual void print_ctx(std::ostream&) const;

    virtual void do_describe1(std::ostream &out) const
    {
      print_ctx(out);
      if (!m_msg.empty())
        out << ": " << m_msg;
    }

    using Base::code;

    void do_describe(std::ostream &out) const
    {
      do_describe1(out);
      out << " (" << code() << ")";
    }

  };


  // -------------------------------------------------------------------------

  /*
    Class representing a single token.

    It stores token type and its position within the parsed string (begin and
    end position).

    Note: For tokens such as quoted string, the characters of the token do
    not include the quotes.
  */

  class Token
  {
  public:

#define token_enum(T,X) T,

    enum Type
    {
      EMPTY = 0,
      TOKEN_LIST(token_enum)
    };

    typedef std::set<Type>  Set;

    cdk::string get_text() const;
    bytes get_bytes() const;
    std::string get_utf8() const;

    Type get_type() const
    {
      return m_type;
    }

#define token_name(X,T) case X: return #X;

    static const char* get_name(int type)
    {
      switch (type)
      {
        TOKEN_LIST(token_name)
        default: return "<UNKNOWN>";
      }
    }

    const char* get_name() const
    {
      return get_name(m_type);
    }

    Token() = default;
    Token(const Token&) = default;

  protected:

    Type m_type = EMPTY;
    const char *m_begin = nullptr;
    const char *m_end = nullptr;

  };


  inline
  cdk::string Token::get_text() const
  {
    cdk::string ret;
    if (m_begin)
    {
      assert(m_begin <= m_end);

      // Note: only strings and quoted words can contain non-ASCII characters.

      switch (m_type)
      {
      case QSTRING:
      case QQSTRING:
      case QWORD:
        ret.set_utf8({ (byte*)m_begin, (byte*)m_end });
        break;
      default:
        ret.set_ascii(m_begin, (size_t)(m_end - m_begin));
        break;
      }
    }
    return ret;
  }

  inline
  bytes Token::get_bytes() const
  {
    return { (byte*)m_begin, (byte*)m_end };
  }

  inline
  std::string Token::get_utf8() const
  {
    return { (const char*)m_begin, (const char*)m_end };
  }

  // -------------------------------------------------------------------------

  /*
    Class implementing parsing characters into tokens.

    After creating a Tokenizer instance from a given string, one can use
    Tokenizer::iterator returned by method begin() to iterate through the
    sequence of tokens.
  */

  class Tokenizer
  {
  public:

    class Error;
    class  iterator;

    Tokenizer(bytes input);

    /*
      Return true if there are no tokens in the input string.
    */
    bool empty() const;

    iterator begin() const;
    const iterator& end() const;

  public:

    char_iterator _begin;

    friend Error;
  };


  /*
    Iterator for accessing a sequence of tokens of a tokenizer.

       cur_pos()
       |   char_iterator::m_pos
       |   |
       v   v
    ---[--]----
       ^^^^
       m_token

  */

  class Tokenizer::iterator
    : public char_iterator
  {
    using pos_type = const char*;
    pos_type  m_pos;
    bool      m_at_end = true;

    iterator(const char_iterator &input)
      : char_iterator(input)
      , m_at_end(false)
    {
      get_next_token();
    }

    pos_type cur_pos() const noexcept
    {
      return m_pos;
    }

    bool at_end() const noexcept
    {
      return m_at_end;
    }


  public:

    iterator() = default;

    iterator(const iterator &other) = default;

    const Token& operator*() const noexcept
    {
      assert(!(at_end()));
      //if (at_end())
      //  THROW("token iterator: accessing null iterator");
      return m_token;
    }

    const Token* operator->() const noexcept
    {
      assert(!(at_end()));
      //if (at_end())
      //  THROW("token iterator: accessing null iterator");
      return &m_token;
    }

    iterator& operator++() //noexcept
    {
      get_next_token();
      return *this;
    }

    bool operator==(const iterator &other) const noexcept
    {
      if (at_end())
        return other.at_end();

      return m_pos == other.m_pos;
    }

    bool operator!=(const iterator &other) const noexcept
    {
      return !(*this == other);
    }

  private:

    struct : public Token
    {
      friend iterator;
    }
    m_token;

    bool get_next_token();

    // Methods that parse characters into various kinds of tokens.

    bool parse_number();
    bool parse_digits() noexcept; // string *digits = NULL);
    bool parse_hex();
    bool parse_hex_digits() noexcept;
    bool parse_string();
    bool parse_word();
    bool parse_quotted_string(char);

    /*
      Add to the sequence new token of a given type. The token ends at the
      current position within the input string and starts at the position
      marked with set_token_start(). The characters of the token are all the
      characters of the input string between token's start and end position.
    */

    void set_token(Token::Type type, pos_type beg = nullptr, pos_type end = nullptr) noexcept
    {
      set_tok_type(type);
      set_tok_pos(
        beg == nullptr ? m_pos : beg,
        end == nullptr ? char_iterator::cur_pos() : end
      );
    }

    void set_tok_pos(pos_type, pos_type) noexcept;
    void set_tok_type(Token::Type) noexcept;

    // Error reporting

    void throw_error(const std::string&) const;

    friend Tokenizer;
    friend Tokenizer::Error;
  };


  inline
  Tokenizer::Tokenizer(cdk::bytes input)
    : _begin(input)
  {}

  inline
  bool Tokenizer::empty() const
  {
    return _begin.at_end();
  }

  inline
  Tokenizer::iterator Tokenizer::begin() const
  {
    return _begin;
  }

  inline
  const Tokenizer::iterator& Tokenizer::end() const
  {
    static iterator end_iter;
    return end_iter;
  }

  inline
  void Tokenizer::iterator::set_tok_pos(pos_type beg, pos_type end) noexcept
  {
    m_token.m_begin = (const char*)beg;
    m_token.m_end = (const char*)end;
  }

  inline
  void Tokenizer::iterator::set_tok_type(Token::Type type) noexcept
  {
    m_token.m_type = type;
  }


  /*
    Tokenizer error class.

    It is a specialization of the generic Error_base which defines
    convenience constructors.
  */

  class Tokenizer::Error
    : public parser::Error_base
  {
  public:

    Error(char_iterator &it, const string &msg = string())
      : Error_base(msg, it)
    {}
  };


  inline
  void Tokenizer::iterator::throw_error(const std::string &msg) const
  {
    throw Error(*(char_iterator*)this, msg);
  }


  // -------------------------------------------------------------------------
  //  Error class implementation
  // -------------------------------------------------------------------------


  /*
    Construct error instance copying fragments of the parsed string
    to the internal buffers to be used in the error description.

    Note: MSVC generates warning for std::string::copy() method
    used below because it is considered unsafe.
  */

  inline
  void Error_base::set_ctx(
    const std::string &input, size_t pos
  )
  {
    char_iterator it(input, input.data() + pos);
    set_ctx(it);
  }

  inline
  void Error_base::set_ctx(
    char_iterator &it
  )
  {
    memset(m_seen, 0, sizeof(m_seen));
    memset(m_ahead, 0, sizeof(m_ahead));

    /*
      Copy characters seen so far to m_seen[] buffer.
    */

    bool   complete;
    bytes  seen = it.get_seen(seen_buf_len -2, &complete);
    char *dst = m_seen;

    /*
      If seen characters cover only a fragment of the parsed text, set
      first character in m_seen[] to 0 to indicate that trailing '...'
      should be added (see print_ctx()). The characters are then copied
      starting from m_seen[1].
    */

    if (!complete)
    {
      m_seen[0] = '\0';
      dst++;
    }

    std::copy_n(seen.begin(), seen.size(), dst);
    dst[seen.size()] = '\0';

    /*
      Copy some characters ahead of the current position to m_ahead[]
      buffer. If this is just a fragment of the remaining text, indicate
      that a trailing '...' should be added. This is done by setting the
      last element in m_ahead to 1 (see print_ctx()).
    */

    bytes ahead = it.get_ahead(ahead_buf_len - 2, &complete);
    std::copy_n(ahead.begin(), ahead.size(), m_ahead);
    m_ahead[ahead.size()] = '\0';
    if (!complete)
      m_ahead[ahead_buf_len - 1] = 1;

  }


  /*
    Print parser context description used in parser error descriptions.

    It has one of these forms:

    "After seeing '...AAA', looking at 'BBB...'"
    "After seeing '...AAA', with no more characters in the string"
    "While looking at 'BBB...'"
    "While looking at empty string"
  */

  inline
  void parser::Error_base::print_ctx(std::ostream &out) const
  {
    bool seen_part = false;

    // Note: cdk::string() used for utf8 conversion.

    if (m_seen[0] || m_seen[1])
    {
      seen_part = true;
      out << "After seeing '";
      if (!m_seen[0])
        out << "..." << (m_seen + 1);
      else
        out << m_seen;
      out << "'";
    }

    if (m_ahead[0])
    {
      if (seen_part)
        out << ", looking at '";
      else
        out << "While looking at '";

      out << m_ahead;

      if (1 == m_ahead[ahead_buf_len - 1])
        out << "...";

      out << "'";
    }
    else
    {
      if (seen_part)
        out << ", with no more characters in the string";
      else
        out << "While looking at empty string";
    }
  }


  // -------------------------------------------------------------------------
  //  String to number conversions.
  // -------------------------------------------------------------------------
  //
  // TODO: Consider if it should not be implemented as a numeric codec.


  // Numeric conversion error classes.

  class Numeric_conversion_error
    : public cdk::Error_class<Numeric_conversion_error>
  {
    typedef cdk::Error_class<Numeric_conversion_error> Base;

  protected:

    std::string m_inp;

    void do_describe(std::ostream &out) const
    {
      out << msg() << " (" << code() << ")";
    }

  public:

    Numeric_conversion_error(const std::string &inp)
      : Base(NULL, cdk::cdkerrc::parse_error)
      , m_inp(inp)
    {}

    virtual std::string msg() const
    {
      std::string msg("Failed to convert string '");
      msg.append(m_inp);
      msg.append("' to a number");
      return msg;
    }
  };


  class Numeric_conversion_partial
    : public cdk::Error_class<
        Numeric_conversion_partial, Numeric_conversion_error
      >
  {
    typedef cdk::Error_class<
              Numeric_conversion_partial, Numeric_conversion_error
            > Base;

  public:

    Numeric_conversion_partial(const std::string &inp)
      : Base(NULL, inp)
    {}

    std::string msg() const override
    {
      std::string msg("Not all characters consumed when converting string '");
      msg.append(m_inp);
      msg.append("' to a number");
      return msg;
    }
  };


  /*
    Generic string to number conversion function template.

    Returns numeric value after converting given string in a given base,
    which should be either 10, 16 or 8. Throws error if the whole string
    could not be converted to a number.

    Unlike strtod() and friends, this function does not depend on the current
    locale setting but always uses the "C" locale (so that, e.g., decimal point
    character is always '.').
  */

  template<
    typename Num_t
  >
  inline
  Num_t strtonum(const std::string &str, int radix = 10)
  {
    // TODO: Allow white-space at the beginning or end of the string?

    typedef std::istreambuf_iterator<char> iter_t;
    static std::locale c_locale("C");
    static const std::num_get<char> &cvt
      = std::use_facet<std::num_get<char>>(c_locale);

    std::istringstream inp(str);
    Num_t val;

    inp.imbue(c_locale);

    switch (radix) {
    case 10: inp.setf(std::ios_base::dec, std::ios_base::basefield); break;
    case 16: inp.setf(std::ios_base::hex, std::ios_base::basefield); break;
    case  8: inp.setf(std::ios_base::oct, std::ios_base::basefield); break;
    default:
      inp.setf(std::ios_base::fmtflags(0), std::ios_base::basefield);
      break;
    }

    /*
      Note: We could use istream::operator>>() to do conversion, but then
      there are problems with detecting conversion errors on some platforms
      (OSX). For that reason we instead use a number conversion facet directly.
      This gives direct access to the error information.
    */

    iter_t beg(inp), end;
    std::ios::iostate err = std::ios_base::goodbit;

    iter_t last = cvt.get(beg, end, inp, err, val);

    if (std::ios_base::goodbit != err && std::ios_base::eofbit != err)
      throw Numeric_conversion_error(str);

    if (last != end)
      throw Numeric_conversion_partial(str);

    return val;
  }


  inline
  double strtod(const std::string &str)
  {
    return strtonum<double>(str);
  }

  inline
  uint64_t strtoui(const std::string &str, int radix = 10)
  {
    return strtonum<uint64_t>(str, radix);
  }

  inline
  int64_t strtoi(const std::string &str, int radix = 10)
  {
    return strtonum<int64_t>(str, radix);
  }

}  // parser

#endif
