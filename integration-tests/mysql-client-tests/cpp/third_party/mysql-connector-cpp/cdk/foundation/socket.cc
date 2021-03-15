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

#include <mysql/cdk/foundation/socket.h>
#include <mysql/cdk/foundation/opaque_impl.i>
#include "connection_tcpip_base.h"


class Socket_conn_impl
  : public cdk::foundation::connection::TCPIP::Impl
{
public:
  unsigned short m_port;
  Socket_conn_impl(unsigned short port);
  void do_connect();
};

IMPL_TYPE(cdk::foundation::Socket::Connection, Socket_conn_impl);
IMPL_PLAIN(cdk::foundation::Socket::Connection);

Socket_conn_impl::Socket_conn_impl(unsigned short port)
  : m_port(port)
{}


void Socket_conn_impl::do_connect()
{
  m_sock = cdk::foundation::connection::detail::listen_and_accept(m_port);
}


namespace cdk {
namespace foundation {


Socket::Connection::Connection(const Socket &sock)
  : connection::TCPIP("", sock.m_port,
                      connection::Socket_base::Options())
  , opaque_impl<Socket::Connection>(NULL, sock.m_port)
{}

Socket::Connection::Impl& Socket::Connection::get_base_impl()
{
  return opaque_impl<Socket::Connection>::get_impl();
}

void Socket::Connection::do_wait()
{
  connect();
}

bool Socket::Connection::is_completed() const
{
  return Socket_base::get_base_impl().is_open();
}


}} // sdk::foundation

