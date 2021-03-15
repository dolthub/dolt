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

#ifndef MYSQL_CDK_FOUNDATION_H
#define MYSQL_CDK_FOUNDATION_H

#include "foundation/common.h"
#include "foundation/types.h"
#include "foundation/error.h"
#include "foundation/async.h"
#include "foundation/stream.h"
#include "foundation/connection_tcpip.h"
#ifdef WITH_SSL
#include "foundation/connection_openssl.h"
#endif
#include "foundation/diagnostics.h"
#include "foundation/codec.h"
//#include "foundation/socket.h"

namespace cdk {

  using foundation::char_t;
  using foundation::invalid_char;
  using foundation::byte;
  using foundation::option_t;
  using foundation::string;
  using foundation::scoped_ptr;
  using foundation::shared_ptr;
  using foundation::variant;
  using foundation::opt;

  using foundation::bytes;
  using foundation::buffers;

  using foundation::Error;
  using foundation::Error_class;
  using foundation::error_condition;
  using foundation::error_category;
  using foundation::error_code;
  using foundation::errc;
  using foundation::cdkerrc;
  using foundation::throw_error;
  using foundation::throw_posix_error;
  using foundation::throw_system_error;
  using foundation::rethrow_error;

  using foundation::Diagnostic_arena;
  using foundation::Diagnostic_iterator;

  namespace api {

    using namespace cdk::foundation::api;

  }  // cdk::api

  namespace connection {

    using foundation::connection::TCPIP;
    using foundation::connection::TLS;
    using foundation::connection::Error_eos;
    using foundation::connection::Error_no_connection;
    using foundation::connection::Error_timeout;

  }

} // cdk


#endif
