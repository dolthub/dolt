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

#ifndef SOCKET_H
#define SOCKET_H

#include <mysql/cdk/foundation.h>
PUSH_BOOST_WARNINGS
#include <boost/asio.hpp>
POP_BOOST_WARNINGS


class Socket
{
  typedef ::boost::asio::ip::tcp tcp;

public:

  typedef cdk::buffers buffers;

  Socket(short int port)
    : m_accept(m_io, endpoint(port))
  {}

  class Connection
    : public cdk::api::Connection
    , public cdk::api::Input_stream
    , public cdk::api::Output_stream
    , public cdk::api::Stream_base::Op
  {
  public:

    Connection(Socket &sock)
      : m_sock(sock.m_io)
    {
      sock.m_accept.accept(m_sock);
    }

  private:

    tcp::socket   m_sock;
    size_t        m_howmuch;

    Input_stream&  get_input_stream() { return *this; }
    Output_stream& get_output_stream() { return *this; }
    void close();
    bool is_closed() const { return !m_sock.is_open(); }

    Op& read(const buffers&);
    bool eos() { return !is_closed(); }

    Op& write(const buffers&);
    bool is_full() { return !is_closed(); }
    void flush() {}

    virtual bool is_completed() const { return true; }
    virtual bool do_cont() { return true; }
    virtual void do_wait() {}

    virtual void do_cancel()
    {
      throw "not implemented";
    }

    virtual const cdk::api::Event_info* get_event_info() const
    { return NULL; }
    virtual result_type do_get_result() { return m_howmuch; }
  };


private:

  ::boost::asio::io_service m_io;
  tcp::acceptor m_accept;

  static tcp::endpoint endpoint(short int port)
  {
    using namespace boost::asio;
    static ip::address localhost(ip::address_v4::loopback());
    return tcp::endpoint(localhost, port);
  }
};

inline
void Socket::Connection::close()
{
  if (m_sock.is_open()){
    m_sock.shutdown(tcp::socket::shutdown_both);
    m_sock.close();
  }
}

#endif
