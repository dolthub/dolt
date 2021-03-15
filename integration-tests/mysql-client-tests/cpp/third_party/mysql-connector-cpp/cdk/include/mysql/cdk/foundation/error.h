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

#ifndef CDK_FOUNDATION_ERROR_H
#define CDK_FOUNDATION_ERROR_H

/*

  Error handling infrastructure
  =============================
  See docs/designs/errors.rst.

*/

#include "common.h"
#include "error_category.h"
#include "std_error_conditions.h"

PUSH_SYS_WARNINGS_CDK
#include <ostream>
#include <stdexcept>
#include <string>
POP_SYS_WARNINGS_CDK

/*
  On Windows, above includes define ERROR macro which breaks
  code that uses this identifier.
*/
#undef ERROR

namespace cdk {
namespace foundation {


/*
  Error conditions
  ----------------
*/


/*
  Macro ERROR_CONDITION_LIST(X) defines a list of error conditions that
  are specific to CDK and will be used in addition to standard error
  conditions defined by C++11. Parameter X is used to process entries
  in the list.
*/

#define ERROR_CONDITION_LIST(X) \
  X (generic_error,      1, "Generic CDK error")  \
  X (standard_exception, 2, "Standard exception") \
  X (unknown_exception,  3, "Unknown exception")  \
  X (boost_error,        4, "Boost error")        \
  X (auth_failure,       5, "Authentication failure")        \
  X (protobuf_error,     6, "Protobuf error")        \
  X (conversion_error,   7, "Value conversion error") \
  X (parse_error,        8, "Parse error") \
  X (in_transaction,     9, "Open transaction") \
  X (no_transaction,    10, "No transaction") \
  X (tls_error,         11, "TLS error") \
  X (bad_savepoint,     12, "Bad savepoint") \
  X (tls_ciphers,       13, "No valid TLS cipher suite configured") \
  X (tls_versions,      14, "No valid TLS protocol versions configured") \

// Define constants for CDK error conditions in cdkerrc structure

#define ERROR_CONDITION_ENUM(C,N,D) C=N,

struct cdkerrc {
  enum code {
    no_error = 0,
    ERROR_CONDITION_LIST(ERROR_CONDITION_ENUM)
  };
};


/*
  Error codes
  -----------
  Class error_code which stores platform-specific numeric error code
  assigned to some error category.
*/

class error_code : public std::error_code
{
public:

  error_code(int code, const error_category &cat)
    : std::error_code(code, cat)
  {}

  explicit error_code(int code)
    : std::error_code(code, generic_error_category())
  {}

  error_code(const std::error_code &other)
    : std::error_code(other)
  {}

  bool operator== (errc::code) const;
  bool operator== (cdkerrc::code) const;
};


template <typename T>
bool operator== (T x, const error_code &code)
{
  return code == x;
}



/*
  Error codes are described as "CCC:NNN" where "CCC" is name of
  error category and "NNN" is its numeric value.
*/

inline
std::ostream& operator<<(std::ostream &out, const error_code &ec)
{
  out <<ec.category().name() <<":" <<ec.value();
  return out;
}


/*
  Like error_code, error_condition is a value-category pair, but
  it is meant to be platform independent.
*/


class error_condition : public std::error_condition
{
protected:


public:

  error_condition(cdkerrc::code code)
    : std::error_condition(code, generic_error_category())
  {}
  error_condition(errc::code code)
    : std::error_condition(code, std_error_category())
  {}
  error_condition(int code)
    : std::error_condition(code, std_error_category())
  {}

  error_condition(int code, const error_category &cat)
    : std::error_condition(code, cat)
  {}

  error_condition(const std::error_condition &ec)
    : std::error_condition(ec)
  {}

  operator error_code()
  {
    return error_code(value(), category());
  }
};


inline
std::error_condition
error_category_base::default_error_condition(int code) const NOEXCEPT
{
  try {
    return do_default_error_condition(code);
  }
  catch (...)
  {
    /*
      If do_default_error_condition() failed for whatever reason, map to
      error condition within this category with code as given.
    */

    return { code, *this };
  }
}

inline
bool
error_category_base::equivalent(int code, const std::error_condition &ec)
const NOEXCEPT
{
  return do_equivalent(code, ec);
}


}}  // cdk::foundation



namespace cdk {
namespace foundation {

/*
  Functions for generating errors
  -------------------------------
  See corresponding section in doc/designs/error.rst
*/

void throw_error(const char *descr);
//template <typename S>
void throw_error(const std::string &descr);
void throw_error(int code, const error_category &ec);
void throw_error(cdkerrc::code code);
void throw_error(cdkerrc::code code, const std::string &prefix);
void throw_error(const error_code &ec);
void throw_error(const error_code &ec, const std::string &prefix);
void rethrow_error();
void rethrow_error(const std::string &prefix);

void throw_posix_error();
void throw_posix_error(const std::string &prefix);

void throw_system_error();
void throw_system_error(const std::string &prefix);


/*
  Base Error class
  ----------------
  This is specialization of std::runtime_error which provides the following
  functionality:

  - Stores error code
  - Defines detailed error description via describe() method
*/


class Error : public std::system_error
{
  static const std::string m_default_prefix;

protected:

  // String used to materialize error description (for what())
  // Note: non-ascii descriptions use UTF-8 encoding
  std::string     *m_what;
  std::string m_what_prefix;

public:

  // If copied error has materialized description, we need to copy it here
  Error(const Error &e)
    : std::system_error(e.code())
    , m_what(e.m_what ? new std::string(*e.m_what) : NULL)
    , m_what_prefix(m_default_prefix)
  {}


  Error(const std::system_error &e)
    : std::system_error(e)
    , m_what(NULL)
    , m_what_prefix(m_default_prefix)
  {
    const char *what = e.what();
    if (what)
      m_what = new std::string(what);
  }

  Error(int _code)
    : std::system_error(_code, generic_error_category())
    , m_what(NULL)
    , m_what_prefix(m_default_prefix)
  {}

  Error(const std::error_code &ec)
    : std::system_error(ec)
    , m_what(NULL)
    , m_what_prefix(m_default_prefix)
  {
  }

  Error(const error_code &ec, const std::string &descr)
    : std::system_error(ec)
    , m_what_prefix(m_default_prefix)
  {
    m_what = new std::string(m_what_prefix);
    m_what->append(descr);
  }

  Error(int _code, const std::string &descr)
    : std::system_error(_code, generic_error_category())
    , m_what_prefix(m_default_prefix)
  {
    m_what = new std::string(m_what_prefix);
    m_what->append(descr);
  }

  virtual ~Error() throw ()
  {
    // delete materialized description if any
    delete m_what;
  }


  error_code code() const
  {
    return std::system_error::code();
  }


  bool operator== (const error_condition &ec) const
  { return code() == ec; }

  bool operator!= (const error_condition &ec) const
  { return !(*this == ec); }


  virtual void describe(std::ostream&) const;

  const std::string description() const
  {
    if (!m_what)
      description_materialize();
    return m_what->substr(m_what_prefix.length());
  }

  const char* what() const throw ()
  {
    if (!m_what)
      description_materialize();
    return m_what->c_str();
  }

  virtual Error* clone() const
  { return new Error(*this); }

  virtual void rethrow() const
  { throw *this; }

private:

  void description_materialize() const;
  virtual void do_describe(std::ostream&) const;
};



inline
void Error::describe(std::ostream &out) const
{
  if (m_what)
  {
    out << m_what->substr(m_what_prefix.length());
    return;
  }

  do_describe(out);
}

inline
void Error::do_describe(std::ostream &out) const
{
  out << code().message();
  out <<" (" << code() <<")";
}


//  Convenience << operator which uses Error::describe().

inline
std::ostream& operator<<(std::ostream &out, const Error &err)
{
  err.describe(out);
  return out;
}


/*
  Convenience template for defining error classes. A new error class
  X, deriving from base error class Y should be declared as follows:

    class X : public Error_class<X, Y>
    {
      ...
    }

  Error_class<> template defines the clone() method required for CDK
  error classes. See "Defining new error classes" in doc/designs/errors.rst
*/

template <class E, class B=Error>
class Error_class : public B
{
protected:

  typedef Error_class<E,B> Error_base;

  Error_class()
  {}

  Error_class(const Error_class &e)
    : B((const B&)e)
  {}

  // Constructors that pass parameters to base class constructor

  template <typename... XX>
  Error_class(void*, XX&&... xx)
    : B(std::forward<XX>(xx)...)
  {}


  virtual ~Error_class() throw ()
  {}

  virtual Error *clone() const
  {
    return new E(*(E*)this);
  }

  virtual void rethrow() const
  {
    throw *(E*)this;
  }
};


/*
  Class of generic errors whose error code is cdkerrc::generic_error
  and description is given during error construction.
*/

class Generic_error : public Error
{
public:

  //template <typename S>
  Generic_error(const std::string &descr)
    : Error(error_code(cdkerrc::generic_error), descr)
  {}

};


/*
  An error which wraps another error and adds prefix to its
  description.
*/

class Extended_error : public Error_class<Extended_error>
{
  const Error *m_base;
  const std::string m_prefix;

public:

  /*
    Copy constructor: note that base error is cloned here.
  */

  Extended_error(const Extended_error &e)
    : Error_base(NULL, e.code())
    , m_base(e.m_base->clone()), m_prefix(e.m_prefix)
  {}

  Extended_error(const Error &base)
    : Error_base(NULL, base.code())
    , m_base(base.clone())
  {}

  Extended_error(const Error &base, const std::string &prefix)
    : Error_base(NULL, base.code())
    , m_base(base.clone()), m_prefix(prefix)
  {}

  ~Extended_error() throw ()
  {
    delete m_base;
  }

protected:

  virtual bool add_prefix(std::ostream&) const;
  void do_describe(std::ostream&) const;

};


inline
bool Extended_error::add_prefix(std::ostream &out) const
{
  if (0 == m_prefix.size())
    return false;
  out <<m_prefix;
  return true;
}

inline
void Extended_error::do_describe(std::ostream &out) const
{
  if (add_prefix(out))
    out <<": ";
  m_base->describe(out);
}


}}  // cdk::foundation


/*
  Inline implementations for error throwing functions
  ---------------------------------------------------
*/

namespace cdk {
namespace foundation {

inline
void throw_error(const char *descr)
{
  throw Generic_error(descr);
}

inline
void throw_error(const std::string &descr)
{
  throw Generic_error(descr);
}

inline
void throw_error(const error_code &ec, const std::string &prefix)
{
  throw Extended_error(Error(ec), prefix);
}

inline
void throw_error(const error_code &ec)
{
  throw Error(ec);
}

inline
void throw_error(int code, const error_category &ec)
{
  throw_error(error_code(code, ec));
}

inline
void throw_error(cdkerrc::code code)
{
  throw_error(error_code(code));
  //, generic_error_category());
}

inline
void throw_error(cdkerrc::code code, const std::string &prefix)
{
  throw_error(error_code(code), prefix);
}

inline
void rethrow_error(const std::string &prefix)
{
  try
  {
    rethrow_error();
  }
  catch (const Error &e)
  {
    throw Extended_error(e, prefix);
  }
}


inline
void throw_posix_error(const std::string &prefix)
{
  try
  {
    throw_posix_error();
  }
  catch (const Error&)
  {
    rethrow_error(prefix);
  }
}

inline
void throw_system_error(const std::string &prefix)
{
  try
  {
    throw_system_error();
  }
  catch (const Error&)
  {
    rethrow_error(prefix);
  }
}


}}


#endif
