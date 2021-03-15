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


/*
  Implementation of error handling infrastructure
*/

#include <mysql/cdk/foundation/error.h>

PUSH_SYS_WARNINGS_CDK
#include <stdexcept>
#include <sstream>
POP_SYS_WARNINGS_CDK


namespace cdk {
namespace foundation {

using std::string;

/*
  Default prefix added to error description in the string returned by
  what().
*/

const string Error::m_default_prefix = "CDK Error: ";


void Error::description_materialize() const
{
  if (m_what)
    return;

  std::ostringstream buf;
  describe(buf);

  Error *self= const_cast<Error*>(this);
  self->m_what= new std::string(m_what_prefix);
  self->m_what->append(buf.str());
}


/*
  Generic error category
  ----------------------
  Error codes in this category map to CDK error conditions with the
  same numeric value. Error code description is the same as description
  of the corresponding error condition.
*/

class error_category_generic : public error_category_base
{
  error_category_generic() {}

public:

  const char* name() const NOEXCEPT { return "cdk"; }
  std::string message(int) const;

  error_condition do_default_error_condition(int code) const NOEXCEPT
  {
    return error_condition(code, generic_error_category());
  }

  bool  do_equivalent(int code, const error_condition &ec) const
  {
    return ec == default_error_condition(code);
  }

  friend const error_category& generic_error_category();
};


const error_category& generic_error_category()
{
  static const error_category_generic instance;
  return instance;
}


// Messages for generic errors

#define ERROR_CONDITION_CASE(C,N,D) \
  case cdkerrc::C: return std::string(D);

std::string error_category_generic::message(int code) const
{
  switch (code)
  {
   case 0: return "No error";
   ERROR_CONDITION_LIST(ERROR_CONDITION_CASE)
   default: return "Unknown error condition";
  }
}


/*
  System error category
  ---------------------
*/

const error_category& system_error_category()
{
  return std::system_category();
}


/*
  Standard/POSIX error category
  -----------------------------
*/


const error_category& std_error_category()
{
  return std::generic_category();
}


const error_category& posix_error_category()
{
  return std::generic_category();
}


bool error_code::operator== (errc::code code) const
{
  return category() == std_error_category() && value() == code;
}

bool error_code::operator== (cdkerrc::code code) const
{
  return category() == generic_error_category() && value() == code;
}


/*
  Wrapping external exceptions as CDK errors
  ------------------------------------------
*/


// Error class that wraps standard exception

class Std_exception : public Error
{

public:

  Std_exception(const std::exception &e)
    : Error(cdkerrc::standard_exception, e.what())
  {}

};


// Error class for unknown exceptions

class Unknown_exception : public Error
{
public:

  Unknown_exception() : Error(cdkerrc::unknown_exception)
  {}
};


void rethrow_error()
{
  try {
    throw;
  }
  catch (const Error&)
  {
    // CDK errors do not need any wrapping
    throw;
  }
  catch (const std::system_error &e)
  {
    throw Error(e);
  }
  catch (const std::exception &e)
  {
    throw Std_exception(e);
  }
  catch (...)
  {
    throw Unknown_exception();
  }
}


// Throwing POSIX and system errors


void throw_posix_error()
{
  if (errno)
    throw_error(errno, posix_error_category());
}


void throw_system_error()
{

#ifdef _WIN32
  int error= static_cast<int>(GetLastError());
#else
  int error= errno;
#endif

  if (error)
    throw_error(error, system_error_category());
}


}} // sdk::foundation
