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

#ifndef CDK_API_SESSION_H
#define CDK_API_SESSION_H

#include "mysql/cdk/foundation.h"


namespace cdk {
namespace api {


class Session
    : public Diagnostics
    , public Async_op<void>
{
public:

  virtual ~Session() {}

  // Check if given session is valid. Function is_valid() performs a lightweight, local check while
  // check_valid() might communicate with the data store to perform this check.
  // Both is_valid() and check_valid() return UNKNOWN if session state could not be determined.

  virtual option_t is_valid() = 0;
  virtual option_t check_valid() = 0;

  // Clear diagnostic information that accumulated for the session.
  // Diagnostics interface methods such as Diagnostics::error_count()
  // and Diagnostics::get_errors() report only new diagnostics entries
  // since last call to clear_errors() (or since session creation if
  // clear_errors() was not called).
  virtual void clear_errors() = 0;

  virtual void close() = 0;

};

}} // cdk::api

#endif // CDK_API_SESSION_H
