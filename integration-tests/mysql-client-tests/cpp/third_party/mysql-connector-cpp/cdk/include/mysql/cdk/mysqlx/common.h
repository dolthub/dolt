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

#ifndef CDK_MYSQLX_COMMON_H
#define CDK_MYSQLX_COMMON_H

#include <mysql/cdk/common.h>  // Traits
#include <mysql/cdk/api/processors.h>
#include <mysql/cdk/protocol/mysqlx.h>

namespace cdk {
namespace mysqlx {

/*
  Content type values.

  For fields that contain raw BLOBs, server can send additional 'content_type'
  information in column meta-data. This list contains knonw content type
  values.

  See documentation for protobuf Mysqlx.Resultset.ColumnMetaData message.
*/

#define CONTENT_TYPE_LIST(X) \
  X(GEOMETRY, 0x0001) \
  X(JSON,     0x0002) \
  X(XML,      0x0003)

#define CONTENT_TYPE_ENUM(A,B) A = B,

struct content_type
{
  enum value
  {
    UNKNOWN = 0,
    CONTENT_TYPE_LIST(CONTENT_TYPE_ENUM)
  };
};

using foundation::string;
using foundation::byte;
using foundation::bytes;

using protocol::mysqlx::Protocol;
using protocol::mysqlx::sql_state_t;
using protocol::mysqlx::row_count_t;
using protocol::mysqlx::col_count_t;
using protocol::mysqlx::collation_id_t;
using protocol::mysqlx::insert_id_t;

typedef api::Async_op<void>   Async_op;
typedef api::Async_op<size_t> Proto_op;
typedef api::Severity Severity;

using cdk::api::Table_ref;
using cdk::api::Schema_ref;

typedef cdk::api::Row_processor<cdk::Traits>  Row_processor;

const error_category& server_error_category();
error_code server_error(int code);


class Server_error
    : public Error_class<Server_error>
{
public:

  typedef protocol::mysqlx::sql_state_t sql_state_t;

  Server_error(unsigned num, sql_state_t, const string& desc = string()) throw()
    : Error_base(NULL, server_error(static_cast<int>(num)), desc)
  {
    assert(num < (unsigned)std::numeric_limits<int>::max());
  }

  virtual ~Server_error() throw() {}

};


class Server_prepare_error
    : public Error_class<Server_prepare_error, Server_error>
{
public:

  typedef protocol::mysqlx::sql_state_t sql_state_t;

  Server_prepare_error(
    unsigned num, sql_state_t sql_state, const string& desc = string()
  ) throw()
    : Error_base(NULL, num, sql_state , desc)
  {}
};


}}  // cdk::mysql

#endif
