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

#ifndef MYSQLX_COMMON_ERROR_H
#define MYSQLX_COMMON_ERROR_H

/*
  TODO: Error handling infrastructure for XDevAPI and X DevAPI for C still
  needs to be done. Current code is just a temporary hack.
*/

#include "api.h"
#include "util.h"

PUSH_SYS_WARNINGS
#include <string>
#include <stdexcept>
#include <ostream>
#include <memory>
#include <forward_list>
#include <string.h>  // for memcpy
#include <utility>   // std::move etc
POP_SYS_WARNINGS


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

namespace common {

/**
  Base class for connector errors.

  @internal
  TODO: Derive from std::system_error and introduce proper
  error codes.
  @endinternal

  @ingroup devapi
*/

class Error : public std::runtime_error
{
public:

  Error(const char *msg)
    : std::runtime_error(msg)
  {}
};


inline
std::ostream& operator<<(std::ostream &out, const Error &e)
{
  out << e.what();
  return out;
}


inline
void throw_error(const char *msg)
{
  throw Error(msg);
}

}  // common

MYSQLX_ABI_END(2,0)
}  // mysqlx


#endif
