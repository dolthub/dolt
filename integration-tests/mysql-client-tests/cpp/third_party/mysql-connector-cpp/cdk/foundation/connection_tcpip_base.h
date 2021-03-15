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

#ifndef IMPL_FOUNDATION_CONNECTION_TCPIP_BASE_H
#define IMPL_FOUNDATION_CONNECTION_TCPIP_BASE_H

PUSH_SYS_WARNINGS_CDK
#include <sys/types.h>
POP_SYS_WARNINGS_CDK

#include "socket_detail.h"

namespace cdk {
namespace foundation {
namespace connection {


void socket_system_initialize();


class Socket_base::Impl
{

public:
  typedef detail::Socket socket;

  socket m_sock;

  Impl()
    : m_sock(detail::NULL_SOCKET)
  {
    // This will initialize socket system (e.g. Winsock) during construction of first CDK connection.
    socket_system_initialize();
  }

  bool is_open() const
  {
    return m_sock != detail::NULL_SOCKET;
  }

  void close()
  {
    if (is_open())
    {
      try
      {
        detail::shutdown(m_sock, detail::SHUTDOWN_MODE_BOTH);
      }
      catch (...)
      {
      }

      detail::close(m_sock);
      m_sock = detail::NULL_SOCKET;
    }
  }

  std::size_t available() const
  {
    if (!is_open())
      return 0;

    try
    {
      return detail::bytes_available(m_sock);
    }
    catch (...)
    {
      // We couldn't establish if there's still data to be read. Assuming there isn't.
      return 0;
    }
  }

  bool has_space() const
  {
    if (!is_open())
      return false;
    return detail::poll_one(m_sock, detail::POLL_MODE_WRITE, false) > 0;
  }

  virtual ~Impl()
  {
    close();
  }

  virtual void do_connect() =0;
};


}}}  // cdk::foundation::connection

#endif
