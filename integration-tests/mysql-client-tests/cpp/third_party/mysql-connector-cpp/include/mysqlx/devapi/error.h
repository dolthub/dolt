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

#ifndef MYSQLX_ERROR_H
#define MYSQLX_ERROR_H

/**
  @file
  Classes used to access query and command execution results.
*/


#include "common.h"
#include "detail/error.h"

#include <memory>


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)

/**
  An error, warning or other diagnostic information reported by server
  when executing queries or statements. The information can be printed to
  output stream using `<<` operator.

  @note Normally, errors reported by server are not represented by `Warning`
  instances but instead they are thrown as instances of `mysqlx::Error`.

  @ingroup devapi
*/

class Warning
  : public virtual common::Printable
  , internal::Warning_detail
{
public:

  /// Type of diagnostic information.

  enum Level {
    LEVEL_ERROR,   ///< %Error
    LEVEL_WARNING, ///< %Warning
    LEVEL_INFO     ///< Other information
  };

private:

  Warning(Level level, uint16_t code, const string &msg)
    : Warning_detail(byte(level), code, msg)
  {
  }

  Warning(Warning_detail &&init)
    : Warning_detail(std::move(init))
  {}

  void print(std::ostream &out) const
  {
    try {
      Warning_detail::print(out);
    }
    CATCH_AND_WRAP
  }

public:

  /**
    Return level of the diagnostic info stored in this object.
  */

  Level getLevel() const
  {
    return Level(m_level);
  }

  /**
    Return error/warning code reported by server.
  */

  uint16_t getCode() const
  {
    return m_code;
  }

  /**
    Return diagnostic message reported by server.
  */

  const string& getMessage() const
  {
    return m_msg;
  }


  ///@cond IGNORE
  friend internal::Result_detail;
  ///@endcond

  struct Access;
  friend Access;
};


inline
void internal::Warning_detail::print(std::ostream &out) const
{
  switch (Warning::Level(m_level))
  {
  case Warning::LEVEL_ERROR: out << "Error"; break;
  case Warning::LEVEL_WARNING: out << "Warning"; break;
  case Warning::LEVEL_INFO: out << "Info"; break;
  default: out << "<Unknown>"; break;
  }

  if (m_code)
    out << " " << m_code;

  out << ": " << m_msg;
}


MYSQLX_ABI_END(2,0)
}  // mysqlx

#endif
