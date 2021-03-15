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

#ifndef CDK_FOUNDATION_ERROR_CATEGORY_H
#define CDK_FOUNDATION_ERROR_CATEGORY_H

#include "common.h"

PUSH_SYS_WARNINGS_CDK
#include <system_error>
POP_SYS_WARNINGS_CDK


namespace cdk {
namespace foundation {


class error_condition;
using std::error_category;
const error_category& generic_error_category();


class error_category_base : public std::error_category
{
protected:

  virtual bool do_equivalent(int code, const error_condition &ec) const = 0;
  virtual error_condition do_default_error_condition(int code) const = 0;

public:

  std::error_condition default_error_condition(int code) const NOEXCEPT;

  bool equivalent(int code, const std::error_condition &ec) const NOEXCEPT;

  bool  equivalent(const std::error_code &ec, int code) const NOEXCEPT
  {
    return ec.value() == code &&  ec.category() == generic_error_category();
  }

};


/*
  Pre-defined error categories. See "Error codes and categories" in
  doc/designs/errors.rst
*/

const error_category& generic_error_category();
const error_category& system_error_category();
const error_category& std_error_category();
const error_category& posix_error_category();


}}  // cdk::foundation


/*
  Infrastructure for defining new error categories
  ================================================

  See "Defining error category" section in doc/designs/errors.rst.

*/

// Process error declaration using X
#define CDK_ERROR(X,C,N,M) X (C,N,M)


/*
  Generate definition of a structure holding enumeration with error
  code values.
*/

#define CDK_ERROR_ENUM(C,N,M) C=N,

#define CDK_ERROR_CODES(EC,NS) \
  struct NS                             \
  {                                     \
    enum code {                         \
      no_error = 0,                     \
      EC_##EC##_ERRORS(CDK_ERROR_ENUM)  \
    };                                  \
  }


#define CDK_ERROR_CASE(NS)        case NS:: CDK_ERROR_CASE1
#define CDK_ERROR_CASE1(C,N,M) C: return std::string(M);

/*
  Generate switch() statement which returns error description
  corresponding to given error code.
*/

#define CDK_ERROR_SWITCH(NS,EC,code) \
  switch (code)                           \
  {                                       \
    case 0: return "No error";            \
    EC_##EC##_ERRORS(CDK_ERROR_CASE(NS))  \
    default: return "Unknown error";      \
  }

/*
  Generate definition of error category.

  First a structure with error codes is defined using CDK_ERROR_CODES.
  Then a cdk_NNN_error_category class is defined, where NNN is the name
  of the new category, which is a specialization of error_category class
  that defines category name ("cdk-NNN") and descriptions for error
  codes (method message()).

  Then inline functions cdk_NNN_category() and cdk_NNN_error() are
  defined.
*/

#define CDK_ERROR_CATEGORY(EC,NS) \
  CDK_ERROR_CODES(EC,NS);                                                 \
  struct error_category_##EC : public cdk::foundation::error_category_base  \
  {                                                                       \
    error_category_##EC() {}                                              \
    const char* name() const throw() { return "cdk-" #EC; }               \
    std::string message(int code) const                                   \
    { CDK_ERROR_SWITCH(NS, EC, code); }                                   \
    cdk::foundation::error_condition do_default_error_condition(int) const;  \
    bool  do_equivalent(int, const cdk::foundation::error_condition&) const; \
  };                                                                      \
  inline const cdk::foundation::error_category& EC##_error_category()     \
  { static const error_category_##EC instance;                            \
    return instance; }                                                    \
  inline cdk::foundation::error_code EC##_error(int code)                 \
  { return cdk::foundation::error_code(code, EC##_error_category()); }

#endif
