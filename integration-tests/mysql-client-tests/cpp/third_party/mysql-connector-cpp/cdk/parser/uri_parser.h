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

#ifndef _URI_PARSER_H_
#define _URI_PARSER_H_

#include <mysql/cdk/common.h>
#include "parser.h"

PUSH_SYS_WARNINGS_CDK
#include <list>
#include <stack>
#include <bitset>
#include <iomanip>
#include <functional>
POP_SYS_WARNINGS_CDK


namespace parser {

/*
  Interface used to report contents of mysqlx URI or connection
  string.
*/

class URI_processor
{
public:

  // LCOV_EXCL_START

  /*
    Callbacks for the main components of the URI:

    <scheme>://<user>:<password>@<host>:<port>/<schema>

    If an optional component is not present, the corresponding callback
    is not called.
  */

  virtual void scheme(const std::string&) {}
  virtual void user(const std::string&) {}
  virtual void password(const std::string&) {}
  virtual void schema(const std::string&) {}


  /*
    Callbacks host(), socket() and pipe() can be called several times to
    report multiple targets listed in a connection string. They report
    priority 1+x if user specified priority x, or 0 if no priority was specified.
  */

  virtual void host(unsigned short /*priority*/,
                    const std::string &/*host*/)
  {}
  virtual void host(unsigned short /*priority*/,
                    const std::string &/*host*/,
                    unsigned short /*port*/)
  {}

  // Report Unix socket path.
  virtual void socket(unsigned short /*priority*/,
                      const std::string &/*socket_path*/)
  {}

  // Report Win pipe path, including "\\.\" prefix.
  virtual void pipe(unsigned short /*priority*/, const std::string &/*pipe*/)
  {}


  /*
    Callbacks for reporting the query component, which is a sequence
    of key-value pair. Keys without any value are allowed. Key value
    can be a list: "...&key=[v1,..,vN]&...".
  */

  virtual void key_val(const std::string&) {}
  virtual void key_val(const std::string&, const std::string&) {}
  virtual void key_val(const std::string&, const std::list<std::string>&) {}

  // LCOV_EXCL_STOP
};


/*
  Parse given string as mysqlx URI ("mysqlx:://" scheme required). Pass
  extracted information to the given processor.
*/

void parse_uri(const std::string &uri, URI_processor &prc);

/*
  Parse given string as mysqlx connection string ("mysqlx:://" scheme optional).
  Pass extracted information to the given processor.
*/

void parse_conn_str(const std::string &str, URI_processor &prc);



/*
  Parser for parsing mysqlx URIs. The same parser can be used to parse
  connection string, which is like URI but without the "mysqlx://" scheme
  prefix.
*/

class URI_parser
  : public cdk::api::Expr_base<URI_processor>
{
public:

  class Error;

private:

  /*
    Represents single token in URI string. Tokens are single characters.
    If m_pct is true then this character was read in the %XX form (and then
    it is never treated as a special URI charcater.
  */

  struct Token
  {
    Token() : m_char(0), m_pct(false)
    {}

    Token(char c, bool pct =false)
      : m_char(c), m_pct(pct)
    {}

    short get_type() const;

    char get_char() const
    {
      return m_char;
    }

    bool pct_encoded() const
    {
      return m_pct;
    }

  private:

    char  m_char;
    bool  m_pct;

    friend class Error;
  };

  /*
    Stored constructor parameters.
  */

  std::string m_uri;
  bool        m_force_uri;

  /*
    Set to true if string contains "mysqlx:://" schema prefix (which
    is optional for connection strings).
  */

  bool        m_has_scheme;

  /*
    Parser state.

    m_tok - the current token (not yet consumed),
    m_pos - position of the current token,
    m_pos_next - position of the next token following the current one;
                 if there are no more tokens then m_pos_next = m_pos,
                 otherwise m_pos_next > m_pos.
  */

  struct State
  {
    Token  m_tok;
    size_t m_pos = 0;
    size_t m_pos_next = 0;

    State(const Token &tok, size_t pos, size_t next)
      : m_tok(tok), m_pos(pos), m_pos_next(next)
    {}
  };

  // We use stack to be able to easily save and restore state.

  std::stack<State> m_state;

  size_t get_pos() const
  {
    return m_state.empty() ? 0 : m_state.top().m_pos;
  }

  void push()
  {
    assert(!m_state.empty());
    m_state.push(m_state.top());
  }

  void pop()
  {
    assert(!m_state.empty());
    m_state.pop();
  }

public:

  /*
    Create parser for a given string. If 'force_uri' parameter is true,
    then the string is expected to be a full URI with the schema part
    (errors are reported if schema is missing). Otherwise 'uri' is treated
    as a connection string with optional scheme prefix.
  */

  URI_parser(const std::string &uri, bool force_uri=false)
    : m_uri(uri), m_force_uri(force_uri)
  {
    // make sure state is not empty (safety)
    m_state.emplace(Token(), 0, 0);
  }

  /*
    Method 'process' parses the string passed to constructor and reports
    information extracted from it to the given processor. Throws errors
    if parsing was not possible. These errors are derived from URI_parser::Error.
  */

  void process(Processor &prc) const
  {
    const_cast<URI_parser*>(this)->parse(prc);
  }

  void process_if(Processor *prc) const
  {
    if (!prc)
      return;
    process(*prc);
  }


private:

  struct TokSet;

  // Character classes used in the grammar.

  static const TokSet unreserved;
  static const TokSet gen_delims;
  static const TokSet host_chars;
  static const TokSet user_chars;
  static const TokSet db_chars;

  void parse(Processor &prc);

  // Methods corresponding to grammar rules.

  void parse_connection(Processor &prc);
  bool parse_userinfo(Processor &prc);
  void parse_path(Processor &prc);
  void parse_query(Processor &prc);
  void parse_val_list(const std::string&, Processor &prc);

  typedef std::bitset<2> Addr_opts;
  static const size_t ADDR_IP = 0;
  static const size_t ADDR_OTHER = 1;

  void parse_list_entry(Processor &prc);
  Addr_opts parse_host(std::string &address, std::string &port);
  bool parse_ip_address(std::string &host, std::string &port);
  void parse_balanced(std::string &chars, bool include_parens = false);

  // Helper methods.

  unsigned short convert_val(const std::string &port) const;
  bool report_address(Processor &prc,
                      Addr_opts opts,
                      unsigned short priority,
                      const std::string &host,
                      const std::string &port) const;

  void parse_scheme(bool,Processor&);

  // Methods for processing tokens.

  Token consume_token();
  bool consume_token(short tt);
  bool consume_word_base(const std::string &word,
                         std::function<bool(char,char)> compare);
  bool consume_word(const std::string &word);
  bool consume_word_ci(const std::string& word);

  void consume_until(std::string&, const TokSet&);
  void consume_while(std::string&, const TokSet&);
  void consume_all(std::string&);
  bool has_more_tokens() const;
  bool at_end() const;

  bool next_token_is(short) const;
  bool next_token_in(const TokSet&) const;

  bool get_token();

  // Error reporting methods.

  void parse_error(const std::string&) const;
  void invalid_char(char) const;
  void unexpected(const std::string&, const std::string &msg = std::string()) const;
  void unexpected(char, const std::string &msg = std::string()) const;

  struct Guard;

  friend Error;
};


/*
  Base class for URI parser errors.
*/

class URI_parser::Error
  : public parser::Error_base
{
protected:

  Error(const URI_parser *p, const std::string &descr = std::string())
  : Error_base(descr, p->m_uri, p->get_pos())
  {}

  friend URI_parser;
};


inline
void URI_parser::parse_error(const std::string &msg) const
{
  throw Error(this, msg);
}

}  // parser

#endif
