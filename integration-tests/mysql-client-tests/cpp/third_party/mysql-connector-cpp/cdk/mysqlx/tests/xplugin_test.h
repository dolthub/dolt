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

#ifndef MYSQL_CDK_MYSQLX_TESTS_XPLUGIN_TEST_H
#define MYSQL_CDK_MYSQLX_TESTS_XPLUGIN_TEST_H

// To use getenv() on Windows without a compiler warning.

#undef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS

#include "cdk_test.h"
#include <mysql/cdk/foundation.h>
#include <gtest/gtest.h>



namespace cdk {
namespace test {

using cdk::foundation::connection::TCPIP;

/*
  Fixture for tests that speak to xplugin. The xplugin port should be set
  with XPLUGIN_PORT env variable.

  TODO: move to core/tests as a general purpose header
*/

class Xplugin : public ::testing::Test
{
  typedef foundation::connection::TCPIP  TCPIP;

 protected:
  // Per-test-case set-up.
  // Called before the first test in this test case.
  // Can be omitted if not needed.
  static void SetUpTestCase()
  {

  }

  // Per-test-case tear-down.
  // Called after the last test in this test case.
  // Can be omitted if not needed.
  static void TearDownTestCase()
  {

  }

  const char *m_status;
  TCPIP *m_conn;
  unsigned short m_port;
  const char *m_host;

  // You can define per-test set-up and tear-down logic as usual.
  virtual void SetUp()
  {
    m_status= NULL;
    m_port= 0;
    m_conn= NULL;

    const char *xplugin_port = getenv("XPLUGIN_PORT");
    if (!xplugin_port)
    {
      m_status= "XPLUGIN_PORT not set";
      return;
    }

    m_host= getenv("XPLUGIN_HOST");
    if (!m_host)
    {
      m_host= "localhost";
    }

    m_port= atoi(xplugin_port);
    m_conn= new TCPIP(m_host, m_port);
    try {
      m_conn->connect();
    }
    catch (cdk::Error &e)
    {
      m_conn = NULL;
      m_status = e.what();
      FAIL() <<"Could not connect to xplugin at " <<m_port <<": " <<e;
    }
  }

  virtual void TearDown()
  {
    delete m_conn;
  }

  TCPIP& get_conn()
  {
    if (!m_conn)
      throw_error(m_status);
    return *m_conn;
  }

  bool has_xplugin()
  {
    return NULL == m_status;
  }
};

}} // cdk::test

#define SKIP_IF_NO_XPLUGIN  \
  if (!has_xplugin()) { std::cerr <<"SKIPPED: " <<m_status <<std::endl; return; }

// TODO: remove this when prepare is ok again
#define SKIP_TEST(A) std::cerr << "SKIPPED: " << A << std::endl; return;

#endif
