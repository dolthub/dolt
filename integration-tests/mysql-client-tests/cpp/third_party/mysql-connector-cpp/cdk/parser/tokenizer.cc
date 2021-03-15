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

#include <mysql/cdk/common.h>

PUSH_SYS_WARNINGS_CDK
#include <stdexcept>
#include <memory>
#include <cstdlib>
#include <cctype>
#include <cstring>
#include <cstdlib>
POP_SYS_WARNINGS_CDK

#include "tokenizer.h"


using namespace parser;

using std::string;


bool Tokenizer::iterator::get_next_token()
{
  skip_ws();

  m_pos = char_iterator::cur_pos();

  if (m_at_end || char_iterator::at_end())
  {
    m_at_end = true;
    return false;
  }

  if ((unsigned)*m_pos < 127)
  {

    switch (*m_pos)
    {
    case '"': case '\'':
      if (parse_string())
        return true;
      break;
    case 'x': case 'X':
    case '0':
      if (parse_hex())
        return true;
    case '.': case '1': case '2': case '3': case '4':
    case '5': case '6': case '7': case '8': case '9':
      if (parse_number())
        return true;
      break;

    default: break;
    }

    assert(!char_iterator::at_end());

    // check symbol tokens, starting with 2+ char ones

    static struct symb_table_t
    {
      std::map<char, std::vector<std::pair<const char*, Token::Type>>> m_map;
      symb_table_t()
      {
#define  symbol_check(T,X) \
        { \
          auto &entry = m_map[(X)[0]]; \
          entry.push_back({X,Token::T}); \
        }
        SYMBOL_LIST2(symbol_check)
      }
    }
    symb_table;

    auto it = symb_table.m_map.find((char)*m_pos);

    if (it != symb_table.m_map.end())
    {
      for (auto symb : it->second)
      {
        if (consume_chars(symb.first)) {
          set_token(symb.second);
          return true;
        }
      }
    }


    switch (*m_pos)
    {
#define  symbol_check1(T,X) \
      case (X)[0]: consume_char(*m_pos); set_token(Token::T); return true;
      SYMBOL_LIST1(symbol_check1)
      default: break;
    }

  }

  /*
    Note: it is important to parse word last as some words can qualify as
    other tokens.
  */

  if (parse_word())
    return true;

  return false;
}


/*
  Parse number literal starting at position i.

  Returns Token::T_NULL if no number literal can start at position i (and
  leaves i unchanged). Otherwise returns Token::LINTEGER or Token::LNUM
  and sets i to the first position after the literal.

  The grammar used for numeric literals:

    number -> int | float
    int -> digit+
    float -> digit* '.' digit+ expo? | digit+ expo
    expo -> ('E'|'e') ('+'|'-')? digit+

  which is replaced by equivalent:

    number -> digit* ('.' digit+)? expo?

  with extra check that there is at least one digit if fractional part is missing.

  Original grammar for floating numbers:

    FLOAT ::= DIGIT* '.' DIGIT+ ('E' ('+'|'-')? DIGIT+)? | DIGIT+ 'E' ('+'|'-')? DIGIT+
*/

bool Tokenizer::iterator::parse_digits() noexcept
{
  bool has_digits = false;

  while (!char_iterator::at_end() && cur_char_in("0123456789"))
  {
    has_digits = true;
    next_unit();
  }

  return has_digits;
}

bool Tokenizer::iterator::parse_number()
{
  if (at_end())
    return false;

  bool is_float = false;
  bool exponent = false;

  /*
    Note: '.' starts NUMBER token only if followed by a digit.
    Otherwise it is a single DOT token.
  */

  if (cur_char_is(L'.') && !char_iterator::at_end(1) && !next_char_in("0123456789"))
    return false;

  // Parse leading digits, if any

  if (!parse_digits() && !cur_char_is('.'))
  {
    return false;
  }

  // Handle decimal point, if any

  if (!char_iterator::at_end() && consume_char('.'))
  {
    is_float = true;
    if (!parse_digits())
      throw_error("No digits after decimal point");
  }

  // See if we have exponent (but it is not parsed yet)

  if (!char_iterator::at_end() && consume_char("Ee"))
  {
    is_float = true;
    exponent = true;
  }

  /*
    If nothing indicates a floating number, we have already
    parsed the digits of an integer number and we can report
    it now.
  */

  if (!is_float)
  {
    set_token(Token::INTEGER);
    return true;
  }

  // Parse exponent if present.

  if (exponent)
  {
    consume_char("+-");

    if (!parse_digits())
      throw_error("No digits in the exponent");
  }

  // Report floating number.

  set_token(Token::NUMBER);
  return true;
}


/*
  Check if we have a Hexadecimal literal:

  X'12ab'
  x'12ab'
  0x12ab
*/


bool Tokenizer::iterator::parse_hex()
{
  if (char_iterator::at_end())
    return false;

  if (!cur_char_in("Xx0"))
    return false;

  switch (cur_char())
  {

  case 'X': case 'x':
  {
    if (char_iterator::at_end(1) || !next_char_is('\''))
      return false;

    next_unit();
    next_unit();

    pos_type start = char_iterator::cur_pos();

    if (!parse_hex_digits())
      throw_error("Unexpected character inside hex literal");

    set_token(Token::HEX, start);

    if (char_iterator::at_end() || !consume_char('\''))
      throw_error("Unexpected character inside hex literal");

    return true;
  }

  case '0':
  {
    if (char_iterator::at_end(1) || !next_char_in("Xx"))
      return false;

    next_unit();
    next_unit();

    pos_type start = char_iterator::cur_pos();

    if (!parse_hex_digits())
      throw_error("No hex digits found after 0x");

    set_token(Token::HEX, start);

    return true;
  }

  default:
    return false;
  }
}

bool Tokenizer::iterator::parse_hex_digits() noexcept
{
  bool ret = false;
  for (; !char_iterator::at_end() && consume_char("0123456789ABCDEFabcdef"); ret = true);
  return ret;
}


/*
  See if next token is:

  WORD  - plain word
  QWORD - word quotted in back-ticks
*/

bool Tokenizer::iterator::parse_word()
{
  if (char_iterator::at_end())
    return false;

  if (cur_char_is('`'))
  {
    parse_quotted_string('`');
    set_tok_type(Token::QWORD);
    return true;
  }

  bool has_word = false;

  while (!char_iterator::at_end() && cur_char_is_word())
  {
    next_unit();
    has_word = true;
  }

  if (!has_word)
    return false;

  set_token(Token::WORD);
  return true;
}


/*
  See if next token is:

  QSTRING  - a string in single quotes
  QQSTRING - a string in double quotes
*/

bool Tokenizer::iterator::parse_string()
{
  char_t quote = cur_char();

  if (!(U'\"' == quote || U'\'' == quote))
    return false;

  if (!parse_quotted_string((char)quote))
    return false;

  set_tok_type('\"' == quote ? Token::QQSTRING : Token::QSTRING);
  return true;
}


bool Tokenizer::iterator::parse_quotted_string(char qchar)
{
  if (!consume_char(qchar))
    return false;

  pos_type start_pos = char_iterator::cur_pos();

  // Store first few characters for use in error message.

  static const size_t start_len = 8;
  cdk::string error("Unterminated quoted string starting with ");
  error.push_back((char_t)qchar);

  while (!char_iterator::at_end())
  {
    // if we do not have escaped char, look at the end of the string

    if (!consume_char('\\'))
    {
      // if quote char is repeated, then it does not terminate string
      if (
        consume_char(qchar) && (char_iterator::at_end() || !cur_char_is(qchar))
      )
      {
        // end of the string, set token extend
        set_tok_pos(start_pos, char_iterator::cur_pos() - 1);
        return true;
      }
    }

    char_t c = consume_char();

    if (c == invalid_char)
      throw_error("Invalid utf8 string");

    if (char_iterator::cur_pos() < start_pos + start_len)
      error.push_back(c);
  }

  throw_error(error + "...");
  return false;  // quiet compile warnings
}


/*
  Low-level character iterator.
*/


std::locale char_iterator::m_cloc("C");


bytes char_iterator::get_seen(size_t len, bool *complete)
{
  char_iterator_base it(m_ctx_beg, cur_pos());

  while (!it.at_end() && (it.cur_pos() + len <= cur_pos()))
    it++;

  if (complete)
    *complete = (it.cur_pos() == get_beg());

  return { (byte*)it.cur_pos(), (byte*)cur_pos() };
}


bytes char_iterator::get_ahead(size_t len, bool *complete)
{
  char_iterator_base it(cur_pos(), get_end());
  const char *pos = it.cur_pos();

  while (!it.at_end() && (it.cur_pos() < cur_pos() + len))
  {
    pos = it.cur_pos();
    it++;
  }

  if (complete)
    *complete = (pos == get_end());

  return { (byte*)cur_pos(), (byte*)pos };
}

