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

#ifndef MYSQLX_TESTING_TEST_H
#define MYSQLX_TESTING_TEST_H


#include <gtest/gtest.h>
#include <mysqlx/xdevapi.h>
#include <iostream>

namespace mysqlx {
namespace test {

/*
  Fixture for tests that speak to xplugin. The xplugin port should be set
  with XPLUGIN_PORT env variable.
*/

class Xplugin : public ::testing::Test
{
public:

  class Client;
  class Session;

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
  mysqlx::Client *m_client;
  mysqlx::Session *m_sess;
  const char *m_host;
  unsigned short m_port;
  const char *m_user;
  const char *m_password;
  const char *m_socket;
  const char *m_srv;

  // You can define per-test set-up and tear-down logic as usual.
  virtual void SetUp()
  {
    using namespace mysqlx;

    m_status = NULL;
    m_host = NULL;
    m_port = 0;
    m_socket = NULL;
    m_srv = nullptr;
    m_user = NULL;
    m_password = NULL;
    m_client = NULL;
    m_sess = NULL;

    m_host = getenv("XPLUGIN_HOST");
    if (!m_host)
      m_host = "localhost";

    const char *xplugin_port = getenv("XPLUGIN_PORT");
    if (!xplugin_port)
    {
      m_status = "XPLUGIN_PORT not set";
      return;
    }
    m_port = (short)atoi(xplugin_port);

    m_socket = getenv("MYSQLX_SOCKET");

    m_srv = getenv("MYSQLX_SRV");

    // By default use "root" user without any password.
    m_user = getenv("XPLUGIN_USER");
    if (!m_user)
      m_user = "root";

    m_password = getenv("XPLUGIN_PASSWORD");

    create_session();

    // Drop and re-create test schema to clear up after previous tests.

    try {
      get_sess().dropSchema("test");
    }
    catch (const Error&)
    {}

    get_sess().createSchema("test");
  }

  virtual void TearDown()
  {
    delete m_sess;
    delete m_client;
  }

  Schema getSchema(const string &name)
  {
    return get_sess().getSchema(name);
  }

  SqlResult sql(const string &query)
  {
    return get_sess().sql(query).execute();
  }

  mysqlx::Client& get_client() const
  {
    // TODO: better error.
    if (!m_client)
      throw m_status;
    return *m_client;
  }

  mysqlx::Session& get_sess() const
  {
    // TODO: better error.
    if (!m_sess)
      throw m_status;
    return *m_sess;
  }

  void create_session()
  {
    try {
      if(!m_client)
      {
        m_client = new mysqlx::Client(
                     SessionOption::HOST, m_host,
                     SessionOption::PORT, m_port,
                     SessionOption::USER, m_user,
                     SessionOption::PWD, m_password
                     );
      }
      delete m_sess;
      m_sess = nullptr;
      m_sess = new mysqlx::Session(*m_client);
    }
    catch (const Error &e)
    {
      delete m_client;
      delete m_sess;
      m_client = NULL;
      m_sess = NULL;
      m_status = e.what();
      FAIL() << "Could not connect to xplugin at " << m_port
        << " (" << m_host << ")" << ": " << e;
    }
  }

  const char* get_host() const
  {
    return m_host;
  }

  const char* get_socket() const
  {
    return m_socket;
  }

  const char* get_srv() const
  {
    return m_srv;
  }

  unsigned short get_port() const
  {
    return m_port;
  }

  const char* get_user() const
  {
    return m_user;
  }

  const char* get_password() const
  {
    return m_password;
  }

  std::string get_uri() const
  {
    std::stringstream uri;
    uri << "mysqlx://" << get_user();
    if (get_password() && *get_password())
      uri << ":" << get_password();
    uri << "@" << get_host() << ":" << get_port();
    return uri.str();
  }

  bool has_xplugin() const
  {
    return NULL == m_status;
  }

  bool is_server_version_less(int test_upper_version,
                              int test_lower_version,
                              int test_release_version)
  {
    SqlResult res_version = sql("SHOW VARIABLES LIKE 'version'");

    std::stringstream version;
    version << res_version.fetchOne()[1].get<string>();

    int upper_version, minor_version, release_version;
    char sep;
    version >> upper_version;
    version >> sep;
    version >> minor_version;
    version >> sep;
    version >> release_version;

    if ((upper_version < test_upper_version) ||
      (upper_version == test_upper_version &&
        minor_version < test_lower_version) ||
        (upper_version == test_upper_version &&
          minor_version == test_lower_version &&
          release_version < test_release_version))
    {
      return true;
    }
    return false;
  }

  void output_id_list(Result& res)
  {
    std::vector<std::string> ids = res.getGeneratedIds();
    for (auto id : ids)
    {
      std::cout << "- added doc with id: " << id << std::endl;
    }

  }

  friend class Use_native_pwd;
};


class Xplugin::Client : public mysqlx::Client
{
public:

  Client(const Xplugin *test)
    : mysqlx::Client(SessionOption::PORT, test->get_port(),
                     SessionOption::USER, test->get_user(),
                     SessionOption::PWD, test->get_password())
  {}
};

class Xplugin::Session : public mysqlx::Session
{
public:

  Session(const Xplugin *test)
    : mysqlx::Session(test->get_client())
  {}
};


class Use_native_pwd
{
  Xplugin& m_xplugin;
  const char* m_user;
  const char* m_password;

public:
  Use_native_pwd(Xplugin &xplugin)
    : m_xplugin(xplugin)
  {
    m_xplugin.sql("DROP USER If EXISTS unsecure_root ");
    m_xplugin.sql("CREATE USER unsecure_root IDENTIFIED WITH 'mysql_native_password';");
    m_xplugin.sql("grant all on *.* to unsecure_root;");
    m_user = m_xplugin.m_user;
    m_password = m_xplugin.m_password;
    m_xplugin.m_user = "unsecure_root";
    m_password = NULL;
  }

  ~Use_native_pwd()
  {
    m_xplugin.sql("DROP USER unsecure_root");
    m_xplugin.m_user = m_user;
    m_xplugin.m_password = m_password;
  }
};

}} // mysql::test


#define SKIP_IF_NO_XPLUGIN  \
  if (!has_xplugin()) { std::cerr <<"SKIPPED: " <<m_status <<std::endl; return; }

#define SKIP_IF_NO_SOCKET  \
  if (!get_socket()) { std::cerr <<"SKIPPED: No unix socket" <<std::endl; return; }

#define SKIP_IF_NO_SRV_SERVICE  \
  if (!get_srv()) { std::cerr <<"SKIPPED: No MYSQLX_SRV defined." <<std::endl; return; }

#define SKIP_IF_SERVER_VERSION_LESS(x,y,z)\
  if (is_server_version_less(x, y, z)) \
  {\
    std::cerr <<"SKIPPED: " << \
    "Server version not supported (" \
    << x << "." << y <<"." << ")" << z <<std::endl; \
    return; \
  }

// TODO: remove this when prepare is ok again
#define SKIP_TEST(A) std::cerr << "SKIPPED: " << A << std::endl; return;


#define EXPECT_ERR(Code) \
  do { \
    try { Code; FAIL() << "Expected an error"; } \
    catch (const std::exception &e) \
    { cout << "Expected error: " << e.what() << endl; } \
    catch (const char *e) { FAIL() << "Bad exception: " << e; } \
    catch (...) { FAIL() << "Bad exception"; } \
  } while(false)

#endif

#define USE_NATIVE_PWD  \
  mysqlx::test::Use_native_pwd __dummy_user__(*this)
