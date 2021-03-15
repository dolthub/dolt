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

#ifndef SDK_FOUNDATION_STATIC_ASSERT_H
#define SDK_FOUNDATION_STATIC_ASSERT_H

/*
  Emulation of some C++11 features used in CDK code.
*/

#include <mysql/cdk/config.h>


namespace cdk {
namespace foundation {

#ifndef HAVE_STATIC_ASSERT

/*
  Idea: Instantiation of static_assertion_test<false> is not defined and compiler
  will choke on sizeof() in static_assert(). But static_assertion_test<true> is
  defined and compiler should go through static_assert().
*/

template <bool test>
struct static_assert_test;

template <>
struct static_assert_test<true>
{
};

// TODO: Fix this implementation or think how to avoid it altogether
// TODO: Better message when assertion fails

#define static_assert(B,Msg)
/*
  enum { static_assert_test ## __LINE_ \
         = sizeof(cdk::foundation::static_assert_test< (bool)(B) >) }
*/

#endif

}}  // cdk::foundation


#endif
