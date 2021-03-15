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

#ifndef CDK_FOUNDATION_CONNECTION_OPENSSL_H
#define CDK_FOUNDATION_CONNECTION_OPENSSL_H

#include "connection_tcpip.h"
#include "stream.h"
#include "error.h"

#include <vector>
#include <set>
#include <iostream>
#include <functional>

namespace cdk {
namespace foundation {
namespace connection {


class TLS
  : public Socket_base
  , opaque_impl<TLS>
{
public:

  class Options;

  TLS(Socket_base* tcpip,
      const Options& Opts);

  bool is_secure() const
  {
    return true;
  }

  class Read_op;
  class Read_some_op;
  class Write_op;
  class Write_some_op;

private:
  Socket_base::Impl& get_base_impl();
};


class TLS::Options
{
public:

  /*
    Note: Normally m_use_tls should be always true: using TLS options object
    implies an intent to have TLS connection. A TLS::Options object with
    m_use_tls set to false is only used to disable TLS connection inside
    TCPIP::Options object. The TCPIP::Options object holds an instance
    of TLS::Options. Calling TCPIP::Options::set_tls(false) will alter this
    internal TLS::Options instance so that m_use_tls is false and then the
    TCPIP::Options object knows that TLS should not be used for the connection.
  */

  enum class SSL_MODE
  {
    DISABLED,
    PREFERRED,
    REQUIRED,
    VERIFY_CA,
    VERIFY_IDENTITY
  };

  struct TLS_version
  {
    unsigned m_major = 0;
    unsigned m_minor = 0;

    TLS_version(unsigned a, unsigned b)
      : m_major(a), m_minor(b)
    {}

    TLS_version(const std::string &);

    // Note: needed for STL containers
    bool operator<(const TLS_version &other) const
    {
      return 1000 * m_major + m_minor < 1000 * other.m_major + other.m_minor;
    }

    struct Error
      : Error_class<Error>
    {
      Error(const std::string &ver)
        : Error_base(nullptr, cdkerrc::tls_error), m_ver(ver)
      {}

      const std::string& get_ver() const
      {
        return m_ver;
      }

    private:

      std::string m_ver;
    };

  };

  typedef std::set<TLS_version> TLS_versions_list;
  typedef std::vector<std::string> TLS_ciphersuites_list;

  Options(SSL_MODE ssl_mode = SSL_MODE::PREFERRED)
    : m_ssl_mode(ssl_mode)
  {}

  void set_ssl_mode(SSL_MODE ssl_mode) { m_ssl_mode = ssl_mode; }
  SSL_MODE ssl_mode() const { return m_ssl_mode; }

  void set_key(const string &key) { m_key = key; }
  const std::string &get_key() const { return m_key; }

  void set_ca(const string &ca) { m_ca = ca; }
  void set_ca_path(const string &ca_path) { m_ca_path = ca_path; }

  const std::string &get_ca() const { return m_ca; }
  const std::string &get_ca_path() const { return m_ca_path; }
  const std::string &get_host_name() const { return m_host_name; }

  void set_host_name(const std::string &host_name)
  {
    m_host_name = host_name;
  }

  void add_version(const TLS_version& version)
  {
    m_tls_versions.insert(version);
  }

  void add_ciphersuite(const std::string& suite)
  {
    m_tls_ciphersuites.push_back(suite);
  }

  const TLS_versions_list& get_tls_versions()
  {
    return m_tls_versions;
  }

  const TLS_ciphersuites_list& get_ciphersuites()
  {
    return m_tls_ciphersuites;
  }

protected:

  SSL_MODE m_ssl_mode;
  std::string m_key;
  std::string m_ca;
  std::string m_ca_path;
  std::string m_host_name;
  TLS_versions_list m_tls_versions;
  TLS_ciphersuites_list m_tls_ciphersuites;
};


class TLS::Read_op : public Socket_base::IO_op
{
public:
  Read_op(TLS &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  TLS& m_tls;
  unsigned int m_currentBufferIdx;
  size_t m_currentBufferOffset;

  bool common_read();
};


class TLS::Read_some_op : public Socket_base::IO_op
{
public:
  Read_some_op(TLS &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  TLS& m_tls;

  bool common_read();
};


class TLS::Write_op : public Socket_base::IO_op
{
public:
  Write_op(TLS &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  TLS& m_tls;
  unsigned int m_currentBufferIdx;
  size_t m_currentBufferOffset;

  bool common_write();
};


class TLS::Write_some_op : public Socket_base::IO_op
{
public:
  Write_some_op(TLS &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  TLS& m_tls;

  bool common_write();
};


} // namespace connection
} // namespace foundation
} // namespace cdk

#endif // CDK_FOUNDATION_CONNECTION_OPENSSL_H
