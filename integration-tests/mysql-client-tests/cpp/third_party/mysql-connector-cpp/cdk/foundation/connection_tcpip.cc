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

#include <mysql/cdk/foundation/connection_tcpip.h>
#include <mysql/cdk/foundation/opaque_impl.i>
#include <mysql/cdk/foundation/error.h>

#include "connection_tcpip_base.h"
#include <list>

using namespace ::cdk::foundation;


/*
  Implementation of TCP/IP connection class.
*/


class connection_TCPIP_impl
  : public ::cdk::foundation::connection::Socket_base::Impl
{
  std::string m_host;
  unsigned short m_port;
  typedef ::cdk::foundation::connection::Socket_base::Options Options;
  Options m_opts;

public:

  connection_TCPIP_impl(const std::string &host, unsigned short port)
    : m_host(host), m_port(port)
  {}

  connection_TCPIP_impl(const std::string &host, unsigned short port,
                        const Options &opts)
    : m_host(host), m_port(port), m_opts(opts)
  {}

  void do_connect();
};


void connection_TCPIP_impl::do_connect()
{
  using namespace ::cdk::foundation::connection;

  // do nothing if connection is already established
  if (is_open())
    return;

  m_sock = connection::detail::connect(m_host.c_str(), m_port,
                                       m_opts.get_connection_timeout());
}


IMPL_TYPE(cdk::foundation::connection::TCPIP, connection_TCPIP_impl);
IMPL_PLAIN(cdk::foundation::connection::TCPIP);


#ifndef WIN32
/*
  Implementation of Unix socket connection class.
*/

class connection_Unix_socket_impl
  : public ::cdk::foundation::connection::Socket_base::Impl
{
  std::string m_path;
  typedef ::cdk::foundation::connection::Socket_base::Options Options;
  Options m_opts;

public:

  connection_Unix_socket_impl(const std::string &path)
    : m_path(path)
  {}

  connection_Unix_socket_impl(const std::string &path,
                              const Options& opts)
    : m_path(path), m_opts(opts)
  {}

  void do_connect();
};


void connection_Unix_socket_impl::do_connect()
{
  using namespace ::cdk::foundation::connection;

  // do nothing if connection is already established
  if (is_open())
    return;

  m_sock = connection::detail::connect(m_path.c_str(),
                                       m_opts.get_connection_timeout());
}


IMPL_TYPE(cdk::foundation::connection::Unix_socket, connection_Unix_socket_impl);
IMPL_PLAIN(cdk::foundation::connection::Unix_socket);
#endif //#ifndef WIN32

namespace cdk {
namespace foundation {
namespace connection {

class Socket_system_initializer
{
  Socket_system_initializer()
  {
    detail::initialize_socket_system();
  }

  ~Socket_system_initializer()
  {
    try
    {
      detail::uninitialize_socket_system();
    }
    catch (...)
    {
      // Ignoring errors in destructor.
    }
  }

  friend void socket_system_initialize();
};

void socket_system_initialize()
{
  static Socket_system_initializer initializer;
}


std::string get_local_hostname()
{
  // This will initialize socket system (e.g. Winsock) during construction of first CDK connection.
  socket_system_initialize();

  return detail::get_local_hostname();
}


SRV_host::SRV_host(detail::Srv_host_detail &&detail)
  : prio(detail.prio)
  , weight(detail.weight)
  , port(detail.port)
  , name(std::move(detail.name))
{}


std::forward_list<SRV_host> srv_list(const std::string &host_name)
{
 std::forward_list<SRV_host> list;
 std::forward_list<SRV_host>::const_iterator it = list.before_begin();

 for(auto &el : detail::srv_list(host_name))
 {
   it = list.insert_after(it, std::move(el));
 }

 return list;
}


TCPIP::TCPIP(const std::string& host,
             unsigned short port,
             const Options& opts)
  : opaque_impl<TCPIP>(NULL, host, port, opts)
{}


#ifndef WIN32
Unix_socket::Unix_socket(const std::string& path,
                         const Options& opts)
  : opaque_impl<Unix_socket>(NULL, path, opts)
{}
#endif //#ifndef WIN32


Socket_base::Impl& TCPIP::get_base_impl()
{
  return get_impl();
}

#ifndef _WIN32
Socket_base::Impl& Unix_socket::get_base_impl()
{
  return get_impl();
}
#endif


void Socket_base::IO_op::do_cancel()
{
  // if operation is completed - does nothing
  if (!is_completed())
    set_completed(0);
}


Socket_base::Read_op::Read_op(Socket_base &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
  , m_currentBufferIdx(0)
  , m_currentBufferOffset(0)
{
  Impl &impl = conn.get_base_impl();

  if (!impl.is_open())
    throw Error_eos();
}


bool Socket_base::Read_op::do_cont()
{
  if (is_completed())
    return true;

  Impl& impl = m_conn.get_base_impl();

  const bytes& buffer = m_bufs.get_buffer(m_currentBufferIdx);
  byte* data =buffer.begin() + m_currentBufferOffset;
  size_t buffer_size = buffer.size() - m_currentBufferOffset;

  m_currentBufferOffset += detail::recv_some(impl.m_sock, data, buffer_size, 0);

  if (m_currentBufferOffset == buffer.size())
  {
    ++m_currentBufferIdx;

    if (m_currentBufferIdx == m_bufs.buf_count())
    {
      set_completed(m_bufs.length());
      return true;
    }
  }

  return false;
}


void Socket_base::Read_op::do_wait()
{
  if (is_completed())
    return;

  Impl& impl = m_conn.get_base_impl();

  for (unsigned int end = m_bufs.buf_count(); m_currentBufferIdx != end; ++m_currentBufferIdx)
  {
    const bytes& buffer = m_bufs.get_buffer(m_currentBufferIdx);
    byte* data = buffer.begin() + m_currentBufferOffset;
    size_t buffer_size = buffer.size() - m_currentBufferOffset;

    detail::recv(impl.m_sock, data, buffer_size); // TODO: Implement operation deadline.

    m_currentBufferOffset = 0;
  }

  set_completed(m_bufs.length());
}


Socket_base::Read_some_op::Read_some_op(Socket_base &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
{
  Impl &impl = conn.get_base_impl();

  if (!impl.is_open())
    throw Error_eos();
}


bool Socket_base::Read_some_op::do_cont()
{
  common_read(false);

  return true;
}


void Socket_base::Read_some_op::do_wait()
{
  common_read(true);
}


void Socket_base::Read_some_op::common_read(bool wait)
{
  if (is_completed())
    return;

  Impl& impl = m_conn.get_base_impl();

  const bytes& buffer = m_bufs.get_buffer(0);

  // TODO: Add timeout support.
  set_completed(detail::recv_some(impl.m_sock, buffer.begin(), buffer.size(), wait));
}


Socket_base::Write_op::Write_op(Socket_base &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
  , m_currentBufferIdx(0)
  , m_currentBufferOffset(0)
{
  Impl &impl = conn.get_base_impl();

  if (!impl.is_open())
    throw Error_no_connection();
}


bool Socket_base::Write_op::do_cont()
{
  if (is_completed())
    return true;

  Impl& impl = m_conn.get_base_impl();

  const bytes& buffer = m_bufs.get_buffer(m_currentBufferIdx);
  byte* data = buffer.begin() + m_currentBufferOffset;
  size_t buffer_size = buffer.size() - m_currentBufferOffset;

  m_currentBufferOffset += detail::send_some(impl.m_sock, data, buffer_size, 0);

  if (m_currentBufferOffset == buffer.size())
  {
    ++m_currentBufferIdx;

    if (m_currentBufferIdx == m_bufs.buf_count())
    {
      set_completed(m_bufs.length());
      return true;
    }
  }

  return false;
}


void Socket_base::Write_op::do_wait()
{
  if (is_completed())
    return;

  Impl& impl = m_conn.get_base_impl();

  for (unsigned int end = m_bufs.buf_count(); m_currentBufferIdx != end; ++m_currentBufferIdx)
  {
    const bytes& buffer = m_bufs.get_buffer(m_currentBufferIdx);
    byte* data = buffer.begin() + m_currentBufferOffset;
    size_t buffer_size = buffer.size() - m_currentBufferOffset;

    detail::send(impl.m_sock, data, buffer_size); // TODO: Implement operation deadline.

    m_currentBufferOffset = 0;
  }

  set_completed(m_bufs.length());
}


Socket_base::Write_some_op::Write_some_op(Socket_base &conn, const buffers &bufs, time_t deadline)
  : IO_op(conn, bufs, deadline)
{
  Impl &impl = conn.get_base_impl();

  if (!impl.is_open())
    throw Error_no_connection();
}


bool Socket_base::Write_some_op::do_cont()
{
  common_write(false);

  return true;
}


void Socket_base::Write_some_op::do_wait()
{
  common_write(true);
}


void Socket_base::Write_some_op::common_write(bool wait)
{
  if (is_completed())
    return;

  Impl& impl = m_conn.get_base_impl();

  const bytes& buffer = m_bufs.get_buffer(0);

  // TODO: Add timeout support.
  set_completed(detail::send_some(impl.m_sock, buffer.begin(), buffer.size(), wait));
}


/*
  Implement public interface of connection::TCPIP
  using internal implementation.
*/

void Socket_base::connect()
{
  get_base_impl().do_connect();
}

void Socket_base::close()
{
  get_base_impl().close();
}

bool Socket_base::is_closed() const
{
  return !(get_base_impl().is_open());
}

unsigned int Socket_base::get_fd() const
{
  return static_cast<unsigned int>(get_base_impl().m_sock);
}

bool Socket_base::eos() const
{
  return !get_base_impl().is_open();
}

bool Socket_base::has_bytes() const
{
  return get_base_impl().available() > 0;
}

bool Socket_base::is_ended() const
{
  return is_closed();
}

bool Socket_base::has_space() const
{
  return get_base_impl().has_space();
}

void Socket_base::flush()
{
  if (is_closed())
    throw connection::Error_no_connection();
}


DIAGNOSTIC_PUSH_CDK
#ifdef _MSC_VER
  // 4702 = unreachable code
  DISABLE_WARNING_CDK(4702)
#endif // _MSC_VER

cdk::foundation::error_condition
error_category_io::do_default_error_condition(int errc) const
{
  switch (errc)
  {
  case io_errc::no_error:
    return errc::no_error;
  case io_errc::EOS:
    return errc::operation_not_permitted;
  case io_errc::TIMEOUT:
    return errc::timed_out;
  case io_errc::NO_CONNECTION:
    return errc::not_connected;
  default:
    throw_error("Error code is out of range");
  }
  // use return statement to suppress compiler warning
  return errc::no_error;
}

DIAGNOSTIC_POP_CDK


bool
error_category_io::do_equivalent(int code, const cdk::foundation::error_condition &ec) const
{
  try
  {
    return ec == default_error_condition(code);
  }
  catch (...)
  {
    return false;
  }
}


}  // namespace connection
}  // namespace foundation
}  // namespace cdk
