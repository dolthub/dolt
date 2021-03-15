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

#include "uri_parser.h"
#include <mysql/cdk.h>

PUSH_SYS_WARNINGS_CDK
#include <stdlib.h>
#include <algorithm>
#include <sstream>
#include <bitset>
#include <cstdarg>
#include <locale>
POP_SYS_WARNINGS_CDK


using namespace parser;
using std::string;


void parser::parse_uri(const std::string &uri, URI_processor &up)
{
  URI_parser    parser(uri, true);
  parser.process(up);
}

void parser::parse_conn_str(const std::string &uri, URI_processor &up)
{
  URI_parser    parser(uri);
  parser.process(up);
}

// ---------------------------------------------------------------


/*
  Specialized error message with description:

  "XXX: Expected 'YYY': MSG"

  Where XXX is the base error description, YYY is the expected
  string (or character) passed to the constructor and MSG is
  optional extra message passed to the constructor.
*/

struct Unexpected_error
  : public cdk::Error_class< ::Unexpected_error, URI_parser::Error >
{
  std::string m_expected;
  cdk::string m_msg;

  Unexpected_error(
    const URI_parser *p,
    const std::string &exp,
    const cdk::string &descr = cdk::string()
  )
    : Error_class(NULL, p)
    , m_expected(exp)
    , m_msg(descr)
  {}

  Unexpected_error(
    const URI_parser *p,
    char c,
    const cdk::string &descr = cdk::string()
  )
    : Error_class(NULL, p)
    , m_msg(descr)
  {
    m_expected.append(1, c);
  }

  ~Unexpected_error() throw ()
  {}

  void do_describe1(std::ostream &out) const
  {
    URI_parser::Error::do_describe1(out);
    out << ": Expected '" << m_expected << "'";
    if (!m_msg.empty())
      out << ": " << m_msg;
  }
};


void URI_parser::unexpected(const std::string &what, const std::string &msg) const
{
  throw Unexpected_error(this, what, msg);
}

void URI_parser::unexpected(char what, const std::string &msg) const
{
  throw Unexpected_error(this, what, msg);
}


/*
  Specialized error for reporting invalid characters in a connection string.
*/

struct Error_invalid_char
  : public URI_parser::Error
{
  char m_char;

  Error_invalid_char(const URI_parser *p, char c)
    : Error(p)
  {
    m_char = c;
  }

  void do_describe1(std::ostream &out) const override
  {
    print_ctx(out);
    out << ": Invalid character ";
    out << "'" << m_char << "'";
    out << " (you can embed such character as '";
    out << "%" << std::setfill('0') << std::setw(2) << std::hex
        << (unsigned)m_char;
    out << "')";
  }

  friend URI_parser;
};


inline
void URI_parser::invalid_char(char c) const
{
  throw Error_invalid_char(this, c);
}


// ---------------------------------------------------------------

/*
  Tokens recognized by URI parser.

  unreserved      ::= ALPHA | DIGIT | "-" | "." | "_" | "~" | "\"
                    | "!" | "$" | "&" | "'" | "*" | "+" | ";" | "="
  gen-delims      ::= ":" | "/" | "?" | "@" | "[" | "]" | "#"
  reserved        ::= gen-delims | "(" | ")" | ","
*/

#define URI_TOKEN_LIST(X)  URI_UNRESERVED(X) URI_RESERVED(X)

#define URI_UNRESERVED(X) \
  X (MINUS, '-')          \
  X (DOT, '.')            \
  X (UNDERSCORE, '_')     \
  X (TILD, '~')           \
  X (BSLASH, '\\')        \
  X (EXCLAMATION, '!')    \
  X (DOLLAR, '$')         \
  X (AMP, '&')            \
  X (QUOTE, '\'')         \
  X (ASTERISK, '*')       \
  X (PLUS, '+')           \
  X (SEMICOLON, ';')      \
  X (EQ, '=')             \

#define URI_RESERVED(X)   \
  URI_GEN_DELIMS(X)       \
  X (POPEN, '(')          \
  X (PCLOSE, ')')         \
  X (COMMA, ',')          \

#define URI_GEN_DELIMS(X) \
  X (COLON, ':')          \
  X (SLASH, '/')          \
  X (QUESTION, '?')       \
  X (AT, '@')             \
  X (SQOPEN, '[')         \
  X (SQCLOSE, ']')        \

/*
  We consider '#' an ivnalid character (that must be always pct-encoded). This
  might be important for applications which handle URIs with fragments (in
  a valid URI '#' is used only to separate main part from fragment and can
  never occur in the main part).

  X (HASH, '#')           \
*/


#define URI_TOKEN_ENUM(T,C) T_##T,
enum token_type
{
  T_ZERO,
  T_CHAR,
  T_DIGIT,
  URI_TOKEN_LIST(URI_TOKEN_ENUM)
  T_INVALID,
  T_LAST
};


/*
  Class representing set of token types.
*/

struct URI_parser::TokSet
{
  std::bitset<T_LAST> m_bits;

  template<typename...TYPE>
  TokSet(TYPE...t)
  {
    add(t...);
  }

  void add(token_type tt)
  {
    m_bits.set(tt);
  }

  void add(const TokSet &set)
  {
    for (unsigned tt = 0; tt < T_LAST; ++tt)
      if (set.has_token(token_type(tt)))
        m_bits.set(tt);
  }

  template<typename T, typename...REST>
  void add(T tt, REST...rest)
  {
    add(tt);
    add(rest...);
  }

  bool has_token(token_type tt) const
  {
    return m_bits.test(tt);
  }
};


/*
  Guard object saves parser state and restores it upon destruction unless
  release() method was called.
*/

struct URI_parser::Guard
{
  URI_parser *m_parser;
  bool        m_pop = true;

  Guard(URI_parser *p) : m_parser(p)
  {
    m_parser->push();
  }

  ~Guard() { if (m_pop) m_parser->pop(); }

  void release() { m_pop = false; }
};


/*
  Character classes defined by the grammar.
*/

#define LIST(X,Y)  T_##X,

const URI_parser::TokSet URI_parser::unreserved{ T_CHAR, T_DIGIT, URI_UNRESERVED(LIST) };
const URI_parser::TokSet URI_parser::gen_delims{ URI_GEN_DELIMS(LIST) };

/*
  user-char ::= unreserved | pct-encoded | "(" | ")" | ","
  host-char ::= unreserved | pct-encoded | "(" | ")" | "@"
  db-char  ::= unreserved | pct-encoded | "(" | ")" | ","
             | "[" | "]" | ":" | "@"
*/

const URI_parser::TokSet URI_parser::user_chars{
  unreserved, T_POPEN, T_PCLOSE, T_COMMA
};

const URI_parser::TokSet URI_parser::host_chars{
  unreserved, T_POPEN, T_PCLOSE, T_AT
};

const URI_parser::TokSet URI_parser::db_chars{
  unreserved, T_POPEN, T_PCLOSE, T_COMMA, T_SQOPEN, T_SQCLOSE, T_COLON, T_AT
};


// ---------------------------------------------------------------

/*
  connection-string ::=
    "mysqlx://"? connection-settings ( "/" database )? ( "?" query )?
*/


void URI_parser::parse(Processor &prc)
{
  // Note: check_scheme() resets parser state.

  parse_scheme(m_force_uri, prc);
  parse_connection(prc);
  parse_path(prc);
  parse_query(prc);

  if (has_more_tokens())
    parse_error("Unexpected characters at the end");
}


/*
  database ::= db-char*
  db-char  ::= unreserved | pct-encoded | "(" | ")" | ","
             | "[" | "]" | ":" | "@"
*/

void URI_parser::parse_path(Processor &prc)
{
  if (!consume_token(T_SLASH))
    return;

  std::string db;

  consume_while(db, db_chars);

  prc.schema(db);
}


/*
  connections-settings ::= ( userinfo "@" )? ( host | "[" host-list "]" )
  host-list     ::= list-entry ( "," list-entry )*

  Note: <userinfo> can not be empty. Formally, if connection string starts with
  "@" we could treat it as part of <host>. But we consider it a syntax error
  instead.
*/

void URI_parser::parse_connection(Processor &prc)
{
  if (next_token_is(T_AT))
    parse_error("Expected user credentials before '@'");

  parse_userinfo(prc);

  /*
    If the next character is '[' then we can have either IPv6 address or
    list of hosts. Call to parse_host() will recognize IPv6 address and in
    that case clear ADD_OTHER flag in the returned options. But if
    parse_host() does not clear ADD_OTHER flag, then it means it has not
    recognized IPv6 and it that case we look for a host list (even if
    parse_host() has accepted the string as a host name).
  */

  push();

  bool starts_with_sqopen = next_token_is(T_SQOPEN);

  // First parse as <host>

  std::string host, port;
  Addr_opts opts = parse_host(host, port);

  /*
    If first char was '[' and we did not recognize IPv6 address, re-parse
    as a host list. Otherwise report address parsed by parse_host().
  */

  if (starts_with_sqopen && opts.test(ADDR_OTHER))
  {
    pop();
    consume_token(T_SQOPEN);

    do {
      parse_list_entry(prc);
    }
    while (consume_token(T_COMMA));

    if (!consume_token(T_SQCLOSE))
      parse_error("Expected ']' to close list of hosts");
  }
  else
    report_address(prc, opts, 0, host, port);
}


/*
  host ::= ip-host | non-ip-host | plain-host

  non-ip-host ::= balanced-sequence
  plain-host  ::= host-char*

  port  ::= DIGIT*
*/

URI_parser::Addr_opts URI_parser::parse_host(std::string &address,
                                             std::string &port)
{
  Addr_opts opts;
  opts.set();

  address.clear();
  port.clear();

  if (next_token_is(T_POPEN))
  {
    parse_balanced(address);
    opts.reset(ADDR_IP);
    return opts;
  }

  if (parse_ip_address(address, port))
  {
    opts.reset(ADDR_OTHER);
    return opts;
  }

  consume_while(address, host_chars);
  return opts;
}


bool URI_parser::report_address(Processor &prc,
                                Addr_opts opts,
                                unsigned short priority,
                                const std::string &host,
                                const std::string &port) const
{
  if (opts.test(ADDR_OTHER))
  {
    if (host[0] == '.' || host[0] == '/')
    {
      prc.socket(priority, host);
      return true;
    }

    if (host.substr(0, 4) == "\\\\.\\")
    {
      prc.pipe(priority, host);
      return true;
    }
  }

  if (opts.test(ADDR_IP))
  {
    if (port.empty())
      prc.host(priority, host);
    else
      prc.host(priority, host, convert_val(port));

    return true;
  }

  parse_error("Unrecognized host address");
  return false;
}


/*
  list-entry    ::= host-priority | host

  host-priority ::= "(" ci("address") "=" host ","
                        ci("priority") "=" priority ")"

  priority      ::= DIGIT+
*/

void URI_parser::parse_list_entry(Processor &prc)
{
  std::string host;
  std::string port;
  Addr_opts opts = 0;

  /*
    If we see <"(" ci("address") "="> then we assume it must be a host-priority
    pair, not a single host.
  */

  {
    Guard guard(this);

    if (consume_token(T_POPEN)
        && consume_word_ci("address")
        && consume_token(T_EQ))
    {
      opts = parse_host(host, port);

      if (!(consume_token(T_COMMA)
            && consume_word_ci("priority")
            && consume_token(T_EQ)))
          parse_error("Expected priority specification for a host");

      std::string prio_str;

      consume_while(prio_str, TokSet(T_DIGIT));

      if (prio_str.empty())
        parse_error("Expected priority value");

      if (!consume_token(T_PCLOSE))
        parse_error("Expected ')' to close a host-priority pair");

      report_address(prc, opts, 1U + convert_val(prio_str), host, port);
      guard.release();
      return;
    }
  }

  // Otherwise we expect a single host.

  opts = parse_host(host, port);

  report_address(prc, opts, 0, host, port);
}


/*
  ip-host     ::=  ( IP-literal | IPv4address ) (":" port)?
                |   plain-host ":" port

  plain-host  ::= host-char*
*/

bool URI_parser::parse_ip_address(std::string &host, std::string &port)
{
  std::string addr;

  Guard guard(this);

  if (consume_token(T_SQOPEN))
  {
    /*
      Look for <IP-literal>. For simpicity we ignore <IPvFuture> rule from the
      RFC grammar and only look for IPv6 address. We also assume that any
      non-empty sequence of hex digits and ':' is a valid IPv6 address.
    */

    while (next_token_in({ T_DIGIT, T_CHAR, T_COLON }))
    {
      Token tok = consume_token();

      if (tok.pct_encoded())
        return false;

      if (T_CHAR == tok.get_type())
        switch (tok.get_char())
        {
        case 'A': case 'B' : case 'C' : case 'D' : case 'E' : case 'F':
        case 'a': case 'b' : case 'c' : case 'd' : case 'e' : case 'f':
          break;
        default:
          return false;
        }

      addr.push_back(tok.get_char());
    }

    if (addr.empty())
      return false;

    if (!consume_token(T_SQCLOSE))
      return false;
  }
  else
  {
    /*
      Look for IPv4address or a beginning of plain-host. For simplicity we
      treat as IPv4 address any string matching:

        DIGIT+ "." DIGIT+ "." DIGIT+ "." DIGIT+
    */

    unsigned count = 0;

    do {

      if (!next_token_is(T_DIGIT))
        break;

      if (count > 0)
        addr.push_back('.');

      consume_while(addr, T_DIGIT);

      if (++count < 4)
        continue;
      else
        break;
    }
    while (consume_token(T_DOT));

    /*
      If we have not seen 4 groups of digits then we do not treat it as IPv4
      address. But we can still match <plain-host ":" port>
    */

    if (count < 4)
    {
      consume_while(addr, host_chars);
      if (!next_token_is(T_COLON))
        return false;
    }
  }

  /*
    If we have reached this far, we have recognized single host address which
    is possibly followed by <":" port>
  */

  guard.release();

  host.append(addr);

  if (consume_token(T_COLON))
  {
    // Note that port can be empty according to our grammar.
    consume_while(port, T_DIGIT);
  }

  return true;
}


/*
  balanced-sequence ::= "(" np-char* balanced-sequence? np-char* ")"
  np-char           ::= unreserved | pct-encoded | gen-delims | "#" | ","
*/

void URI_parser::parse_balanced(std::string &chars, bool include_parens)
{
  static TokSet np_char{ unreserved, gen_delims, T_COMMA };

  if (!consume_token(T_POPEN))
    parse_error("Expected opening '('");

  if (include_parens)
    chars.push_back('(');

  consume_while(chars, np_char);

  if (next_token_is(T_POPEN))
    parse_balanced(chars, true);

  consume_while(chars, np_char);

  if (!consume_token(T_PCLOSE))
    parse_error("Expected closing ')'");

  if (include_parens)
    chars.push_back(')');
}


/*
  Accept <( userinfo "@" )?>.

  userinfo ::= user (":" password?)?
  user     ::= user-char+
  password ::= (user-char | ":")*
*/

bool URI_parser::parse_userinfo(Processor &prc)
{
  Guard guard(this);

  std::string user;
  std::string password;
  bool has_pwd = false;

  consume_while(user, user_chars);

  if (user.empty())
    return false;

  if (consume_token(T_COLON))
  {
    has_pwd = true;
    consume_while(password, { user_chars, T_COLON });
  }

  if (!consume_token(T_AT))
    return false;

  guard.release();

  prc.user(user);
  if (has_pwd)
    prc.password(password);

  return true;
}


/*
  Process query part which consists of key-value pairs of the
  form "<key>=<value>" separated by '&'.

  The value part is optional. If it starts with '[' then we
  have a comma separated list of values.

  query           ::= "?" (pair | multiple-pairs)+
  pair            ::= ( key ( "=" (valuelist | value) )?)
  multiple-pairs  ::= pair("&" pair)+
  key             ::= (unreserved | pct-encoded | sub-delims)+
  valuelist       ::= "[" value ("," value)* "]"
  value           ::= (unreserved | pct-encoded | "!" | "$" | "'" | "(" | ")" |  "*" | "+" | ";" | "=")*

*/


void URI_parser::parse_query(Processor &prc)
{
  std::string key;
  std::string val;

  if (!consume_token(T_QUESTION))
    return;

  do {
    key.clear();

    /*
      After key there should be '=' or, if key has no value,
      either '&' that separates next key-value pair or the end of
      the query.
    */

    consume_until(key, { T_EQ, T_AMP });

    if (!consume_token(T_EQ))
    {
      // The case of a key without a value.

      prc.key_val(key);
    }
    else
    {
      // If first value character is '[' then the value is a list.

      if (next_token_is(T_SQOPEN))
      {
        parse_val_list(key, prc);
      }
      else
      {
        /*
          If value is not a list, then it extends until the next
          '&' or the end of the query.
        */
        val.clear();

        consume_until(val, T_AMP);

        prc.key_val(key, val);
      }
    }

  }
  while (consume_token(T_AMP));
}


/*
  Process comma separated list of values enlosed in '[' and ']',
  reporting this list as value of given key.
*/

void URI_parser::parse_val_list(const std::string &key, Processor &prc)
{
  if (!consume_token(T_SQOPEN))
    return;

  std::list<std::string> list;
  std::string val;

  do {
    val.clear();

    consume_until(val, { T_COMMA, T_SQCLOSE });

    list.push_back(val);
  }
  while (consume_token(T_COMMA));

  if (!consume_token(T_SQCLOSE))
  {
    std::ostringstream msg;
    msg << "Missing ']' while parsing list value of query key '"
        << key <<"'" << std::ends;
    parse_error(msg.str());
  }

  prc.key_val(key, list);
}


// -------------------------------------------------
//  Helper methods
// -------------------------------------------------


unsigned short URI_parser::convert_val(const std::string &port) const
{
  const char *beg = port.c_str();
  char *end = NULL;
  long int val = strtol(beg, &end, 10);

  /*
    Note: strtol() returns 0 either if the number is 0
    or conversion was not possible. We distinguish two cases
    by cheking if end pointer was updated.
  */

  if (val == 0 && end == beg)
    throw Error(this, "Expected number");

  if (val > 65535 || val < 0)
    throw Error(this, "Invalid value");

  return static_cast<unsigned short>(val);
}


/*
  Consume tokens and store in the given buffer until the end of
  the current URI part or until a token of type
  from the given set is seen.
*/

void URI_parser::consume_until(std::string &buf, const TokSet &toks)
{
  while (has_more_tokens() && !next_token_in(toks))
    buf.push_back(consume_token().get_char());
}

/*
  Consume tokens and store in the given buffer while current Token type belongs
  to the given set
*/

void URI_parser::consume_while(std::string &buf, const TokSet &toks)
{
  while (has_more_tokens() && next_token_in(toks))
    buf.push_back(consume_token().get_char());
}

/*
  Consume all remaining tokens of the current URI part and store
  them in the given buffer.
*/

void URI_parser::consume_all(std::string &buf)
{
  while (has_more_tokens())
    buf.push_back(consume_token().get_char());
}


// Check type of next token.

bool URI_parser::next_token_is(short tt) const
{
  assert(!m_state.empty());
  return !at_end() && tt == m_state.top().m_tok.get_type();
}

//  Check if type of next token is in the given set.

bool URI_parser::next_token_in(const TokSet &toks) const
{
  assert(!m_state.empty());
  if (!has_more_tokens())
    return false;
  return toks.has_token(token_type(m_state.top().m_tok.get_type()));
}


// -------------------------------------------------


/*
  Check the scheme part of the URI (if present) and set
  the initial parser state.

  If parameter 'force' is true, error is thrown if the
  'mysqlx:://' or 'mysqlx+srv:://' scheme prefix is not present. Otherwise scheme
  is optional, but if present it must be 'mysqlx' or 'mysqlx+srv:://'.
*/

void URI_parser::parse_scheme(bool force, Processor &prc)
{
  std::stack<State> new_state;
  new_state.emplace(Token(), 0, 0);

  m_state.swap(new_state);
  m_has_scheme = false;

  State &state = m_state.top();

  size_t pos = m_uri.find("://");

  if (pos != std::string::npos)
  {
    m_has_scheme = true;
    std::string scheme = m_uri.substr(0, pos);
    if (scheme != "mysqlx" && scheme != "mysqlx+srv")
    {
      std::string error_msg = "Scheme ";
      error_msg+= scheme;
      error_msg+= " is not valid";
      parse_error(error_msg);
    }

    prc.scheme(scheme);

    // move to the first token after '://'
    state.m_pos_next = pos + 3;
  }
  else
  {
    pos = 0;

    if (m_uri.substr(0, 6) == "mysqlx")
    {
      // for correct error reporting
      state.m_pos = 6;
      unexpected("://");
    }

    if (force)
      parse_error("URI scheme expected");
  }

  get_token();
}


/*
  Get next token, store it in m_tok and update parser state.

  If in_part is true then only tokens from the current part
  are considered. Otherwise the whole string is considered.

  Returns false if there are no more tokens (in the current part).
*/

bool URI_parser::get_token()
{
  assert(!m_state.empty());

  State  &state = m_state.top();
  size_t pos = state.m_pos = state.m_pos_next;

  if (at_end())
    return false;

  if ('%' == m_uri[pos])
  {
    long c;

    // TODO: more efficient implementation.

    std::string hex = m_uri.substr(pos + 1, 2);
    hex.push_back('\0');
    char *end = NULL;
    c = strtol(hex.data(), &end, 16);
    if (end != hex.data() + 2 || c < 0 || c > 256)
      parse_error("Invalid pct-encoded character");

    state.m_tok = Token((char)c, true);
    state.m_pos_next = pos + 3;
    return true;
  }

  state.m_tok = Token(m_uri[pos]);
  state.m_pos_next = pos + 1;

  if (T_INVALID == state.m_tok.get_type())
    invalid_char(m_uri[pos]);

  return true;
}

/*
  Return true if all tokens from the URI string have been consumed.
*/

bool URI_parser::at_end() const
{
  assert(!m_state.empty());
  return get_pos() >= m_uri.length();
}

/*
  Return true if there is at least one more token which
  belongs to the current URI part (not counting the delimiter).
*/

bool URI_parser::has_more_tokens() const
{
  return !at_end();
}


/*
  Return the current token and proceed to the next one (if any).
  Throws error if next token is not available.
*/

URI_parser::Token URI_parser::consume_token()
{
  if (at_end())
    parse_error("Expected more characters");
  Token cur_tok(m_state.top().m_tok);
  get_token();
  return cur_tok;
}

/*
  Check if there is next token in the current part of the URI
  and if it is of given type. If yes, consume it, moving to the
  next token.

  Returns false if there are no more tokens in the current part
  or the next token is not of the given type.
*/

bool URI_parser::consume_token(short tt)
{
  if (!has_more_tokens())
    return false;
  if (!next_token_is(tt))
    return false;
  consume_token();
  return true;
}


bool URI_parser::consume_word_base(
  const std::string &word,
  std::function<bool(char,char)> compare
)
{
  Guard guard(this);

  for (auto el : word)
  {
    if (!has_more_tokens())
      return false;

    if (!compare(m_state.top().m_tok.get_char(),el))
      return false;

    consume_token();

  }

  guard.release();

  return true;
}


bool URI_parser::consume_word(const std::string &word)
{
  auto compare= [](char a, char b) -> bool
  {
    return a == b;
  };

  return consume_word_base(word, compare);
}

bool URI_parser::consume_word_ci(const std::string &word)
{
  auto compare_ci= [](char a, char b) -> bool
  {
    return ::tolower(a) == ::tolower(b);
  };

  return consume_word_base(word, compare_ci);
}


// -------------------------------------------------

/*
  Check type of the token. Special URI characters are
  as defined by URI_TOKEN_LIST above. The rest are either DIGIT (0-9) or CHAR,
  the latter being else an arbitrary pct-encoded char or a plain ASCII letter.
  Any other character is invalid inside connection string.
*/

short URI_parser::Token::get_type() const
{
  if (m_pct)
    return T_CHAR;

  if (isalpha(m_char, std::locale("C")))
    return T_CHAR;

#define URI_TOKEN_CASE(T,C)  case C: return T_##T;

  switch(m_char)
  {
    URI_TOKEN_LIST(URI_TOKEN_CASE)
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9': return T_DIGIT;
    default: return T_INVALID;
  }
}
