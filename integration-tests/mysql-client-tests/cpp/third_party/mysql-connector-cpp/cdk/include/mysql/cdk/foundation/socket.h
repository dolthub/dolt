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

#ifndef CDK_FOUNDATION_SOCKET_H
#define CDK_FOUNDATION_SOCKET_H

/*
  TCP/IP socket which can accept connections from other hosts.

  Usage
  =====

  ::

    Socket sock(port);

    Socket::Connection conn(sock);
    conn.wait();

  Socket::Connection instance is an asynchronous object which is completed
  when a connection is accepted. After that, the Socket::Connection instance
  conn can be used like any other connection object. For example, one can
  read some bytes sent by the peer like this::

    Socket::Connection::Read_some_op  read(conn, buf);
    read.wait();
*/


#include "connection_tcpip.h"
#include "opaque_impl.h"


namespace cdk {
namespace foundation {


class Socket
{
  unsigned short m_port;

public:

  class Connection;

  Socket(unsigned short port)
    : m_port(port)
  {}
};


class Socket::Connection
  : public connection::TCPIP
  , public api::Async_op<void>
  , opaque_impl<Socket::Connection>
{
public:

  Connection(const Socket&);

private:

  typedef TCPIP::Impl  Impl;

  Impl& get_base_impl();

  void do_wait();
  bool is_completed() const;

  bool do_cont()
  {
    wait();
    return true;
  }

  void do_cancel()
  { THROW("Not implemented"); }

  api::Event_info* get_event_info() const
  { return NULL; }
};


}}  // cdk::foundation

#endif
