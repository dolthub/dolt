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

#ifndef MYSQLX_COMMON_COMMON_H
#define MYSQLX_COMMON_COMMON_H

#include <mysqlx/common.h>
#include <version_info.h>


namespace mysqlx {
MYSQLX_ABI_BEGIN(2,0)
namespace common {

class Session_pool;
class Session_impl;
class Result_impl;
class Result_init;
class Column_info;

using Shared_session_impl = std::shared_ptr<Session_impl>;
using Session_pool_shared = std::shared_ptr<Session_pool>;

using cdk::col_count_t;
using cdk::row_count_t;

}
MYSQLX_ABI_END(2,0)
}


namespace mysqlx {
namespace impl {
namespace common {

using namespace mysqlx::common;

/*
  Note: for older gcc versions (4.8, 5.4) above using namespace declaration
  is not sufficient to correctly resolve mysqlx::impl::common::Session_pool
  (but for more recent versions it works).
*/

using mysqlx::common::Session_pool;

using cdk::col_count_t;
using cdk::row_count_t;

}}}


#ifndef THROW_AS_ASSERT
#undef THROW
#define THROW(MSG) do { mysqlx::common::throw_error(MSG); throw (MSG); } while(false)
#endif


#endif
