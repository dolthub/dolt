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

#ifndef _JSON_PARSER_H_
#define _JSON_PARSER_H_

#include <mysql/cdk/common.h>
#include "parser.h"

namespace parser {

using cdk::JSON;

class JSON_parser
  : public JSON
{

  class Error;

  std::string m_json;
public:

  JSON_parser(const std::string &json)
    : m_json(json.begin(), json.end())
  {
    m_json.push_back('\0');
  }

  JSON_parser(std::string &&json)
    : m_json(std::move(json))
  {
    m_json.push_back('\0');
  }

  void process(Processor &prc) const;
};



/*
  Error class for JSON_parse

  It is a specialization of the generic parser::Error_base which defines
  convenience constructors.
*/

class JSON_parser::Error
    : public parser::Error_base
{
public:
  Error(const std::string& parsed_text,
    size_t pos,
    const std::string& desc = string())
    : parser::Error_base(desc, parsed_text, pos)
  {}
};

}  // parser

#endif
