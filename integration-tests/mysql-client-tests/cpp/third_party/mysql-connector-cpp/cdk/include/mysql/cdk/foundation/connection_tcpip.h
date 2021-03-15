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

#ifndef CDK_FOUNDATION_CONNECTION_TCPIP_H
#define CDK_FOUNDATION_CONNECTION_TCPIP_H

// 10 seconds
#define DEFAULT_CN_TIMEOUT_MS 10000
#define DEFAULT_CN_TIMEOUT_US 10000*1000

#include "async.h"
#include "stream.h"
#include "opaque_impl.h"
#include "error.h"
#include <forward_list>


namespace cdk {
namespace foundation {
namespace connection {


class TCPIP;
class TLS;

namespace detail{
  struct Srv_host_detail;
}

/**
   @brief get_local_hostname returns hostname of the current machine
 */
std::string get_local_hostname();


struct SRV_host
{
  SRV_host(detail::Srv_host_detail&&);

  uint16_t prio;
  uint16_t weight;
  uint16_t port;
  string name;


};


/*
   Return SRV records for name 'host_name' obtained from DNS.
*/

std::forward_list<SRV_host> srv_list(const std::string &host_name);


/*
  Defining cdkio error category
  =======================
  See cdk/foundation/error_category.h for more information.
*/

#define EC_io_ERRORS(X) \
  CDK_ERROR(X, EOS, 1, \
            "Cannot perform this action because input stream is in EOS state") \
  CDK_ERROR(X, TIMEOUT, 2, \
            "Operation could not complete before deadline specified " \
            "when it was created") \
  CDK_ERROR(X, NO_CONNECTION, 3, \
            "Attempt to create i/o operation for a connection object that " \
            "was not yet connected or for which connection attempt has failed")

CDK_ERROR_CATEGORY(io, io_errc)


class IO_error : public Error_class<IO_error>
{
public:

  IO_error(int num)
    : Error_base(NULL, io_error(num))
  {}

  virtual ~IO_error() throw() {}

};


class Error_eos : public IO_error
{
public:
  Error_eos() : IO_error(io_errc::EOS) {}
};


class Error_timeout : public IO_error
{
public:
  Error_timeout() : IO_error(io_errc::TIMEOUT) {}
};


class Error_no_connection : public IO_error
{
public:
  Error_no_connection() : IO_error(io_errc::NO_CONNECTION) {}
};


class Connect_timeout_error :
  public Error_class<Connect_timeout_error, Error_timeout>
{
  uint64_t m_msec = 0;
public:
  Connect_timeout_error(uint64_t msec) :
    m_msec(msec)
  {}

  void do_describe(std::ostream &out) const override
  {
    out << "Connection attempt to the server was aborted. Timeout of " <<
      m_msec << " milliseconds was exceeded";
  }
};

class Socket_base
  : public Connection_class<Socket_base>
{
public:

  class Impl;
  class IO_op;
  class Options;
  class Read_op;
  class Read_some_op;
  class Write_op;
  class Write_some_op;

  // Connection interface

  virtual void connect();
  virtual void close();
  virtual bool is_closed() const;
  virtual unsigned int get_fd() const;

  // Input stream

  bool eos() const;
  bool has_bytes() const;

  // Output stream

  bool is_ended() const;
  bool has_space() const;
  void flush();

protected:

  virtual Impl& get_base_impl() =0;
  const Impl& get_base_impl() const
  {
    return const_cast<Socket_base*>(this)->get_base_impl();
  }

  friend class IO_op;
  friend class TCPIP;
  friend class TLS;
};


class Socket_base::Options
{
  private:

    // By default the timeout is 10 seconds
    uint64_t m_timeout_usec = DEFAULT_CN_TIMEOUT_US;

  public:

    Options()
    {}

    Options(uint64_t timeout_usec) : m_timeout_usec(timeout_usec)
    {}

    uint64_t get_connection_timeout() const
    { return m_timeout_usec; }

    void set_connection_timeout(uint64_t timeout_usec)
    {
      m_timeout_usec = timeout_usec;
    }
};

class TCPIP
  : public Socket_base
  , opaque_impl<TCPIP>
{
public:

  TCPIP(const std::string& host, unsigned short port,
        const Options& opts = Options());

  bool is_secure() const
  {
    return false;
  }

private:

  Socket_base::Impl& get_base_impl();
};


#ifndef _WIN32
class Unix_socket
  : public Socket_base
  , opaque_impl<Unix_socket>
{
public:

  Unix_socket(const std::string& path, const Options& opts);

  bool is_secure() const
  {
    return true;
  }

private:

  Socket_base::Impl& get_base_impl();
};
#endif //_WIN32


// Socket_base

class Socket_base::IO_op : public Base::IO_op
{
protected:

  typedef Socket_base::Impl Impl;

  IO_op(Socket_base &str, const buffers &bufs, time_t deadline =0)
    :  Base::IO_op(str, bufs, deadline)
  {}

  // Async_op interface

  // is_completed() is implemented in Base::IO_op
  // bool is_completed() const;
  virtual bool do_cont() = 0;
  virtual void do_cancel();
  virtual void do_wait() = 0;

  const api::Event_info* get_event_info() const { return  NULL; }
};


class Socket_base::Read_op : public IO_op
{
public:
  Read_op(Socket_base &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  unsigned int m_currentBufferIdx;
  size_t m_currentBufferOffset;
};


class Socket_base::Read_some_op : public IO_op
{
public:
  Read_some_op(Socket_base &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  void common_read(bool wait);
};


class Socket_base::Write_op : public IO_op
{
public:
  Write_op(Socket_base &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  unsigned int m_currentBufferIdx;
  size_t m_currentBufferOffset;
};


class Socket_base::Write_some_op : public IO_op
{
public:
  Write_some_op(Socket_base &conn, const buffers &bufs, time_t deadline = 0);

  virtual bool do_cont();
  virtual void do_wait();

private:
  void common_write(bool wait);
};


}}}  // namespace cdk::foundation::connection

#endif
