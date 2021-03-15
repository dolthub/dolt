/*
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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

#include "socket_detail.h"
#include <mysql/cdk/foundation/error.h>
#include <mysql/cdk/foundation/connection_tcpip.h>
PUSH_SYS_WARNINGS_CDK
#include "openssl/ssl.h"

#include <cstdio>
#include <limits>
#include <chrono>
#include <sstream>
#include <mutex>
#include <thread>
#include <forward_list>
#include <map>
#include <functional>
#include <iostream>

#ifndef _WIN32
#include <arpa/inet.h>
#include <signal.h>
#include <sys/un.h>
#include <poll.h>
#include <resolv.h>
#include <arpa/nameser.h>
#else
#include <windns.h>
#endif
POP_SYS_WARNINGS_CDK

using namespace std::chrono;

namespace cdk {
namespace foundation {
namespace connection {
namespace detail {


#ifdef _WIN32

/*
  error_category_winsock class
  ============================

  Used for handling Winsock errors.
*/

class error_category_winsock : public error_category_base
{
  error_category_winsock() {}

  const char* name() const NOEXCEPT { return "winsock"; }
  std::string message(int) const;

DIAGNOSTIC_PUSH_CDK
#ifdef _MSC_VER
  // 4702 = unreachable code
  DISABLE_WARNING_CDK(4702)
#endif // _MSC_VER

  error_condition do_default_error_condition(int code) const
  {
    switch (code)
    {
      case WSAEACCES: return errc::permission_denied;
      case WSAEADDRINUSE: return errc::address_in_use;
      case WSAEADDRNOTAVAIL: return errc::address_not_available;
      case WSAEAFNOSUPPORT: return errc::address_family_not_supported;
      case WSAEALREADY: return errc::connection_already_in_progress;
      case WSAEBADF: return errc::bad_file_descriptor;
      case WSAECONNABORTED: return errc::connection_aborted;
      case WSAECONNREFUSED: return errc::connection_refused;
      case WSAECONNRESET: return errc::connection_reset;
      case WSAEDESTADDRREQ: return errc::destination_address_required;
      case WSAEFAULT: return errc::bad_address;
      case WSAEHOSTUNREACH: return errc::host_unreachable;
      case WSAEINPROGRESS: return errc::operation_in_progress;
      case WSAEINTR: return errc::interrupted;
      case WSAEINVAL: return errc::invalid_argument;
      case WSAEISCONN: return errc::already_connected;
      case WSAEMFILE: return errc::too_many_files_open;
      case WSAEMSGSIZE: return errc::message_size;
      case WSAENAMETOOLONG: return errc::filename_too_long;
      case WSAENETDOWN: return errc::network_down;
      case WSAENETRESET: return errc::network_reset;
      case WSAENETUNREACH: return errc::network_unreachable;
      case WSAENOBUFS: return errc::no_buffer_space;
      case WSAENOPROTOOPT: return errc::no_protocol_option;
      case WSAENOTCONN: return errc::not_connected;
      case WSAENOTSOCK: return errc::not_a_socket;
      case WSAEOPNOTSUPP: return errc::operation_not_supported;
      case WSAEPROTONOSUPPORT: return errc::protocol_not_supported;
      case WSAEPROTOTYPE: return errc::wrong_protocol_type;
      case WSAETIMEDOUT: return errc::timed_out;
      case WSAEWOULDBLOCK: return errc::operation_would_block;
      default:
        throw_error(code, winsock_error_category());
        return errc::no_error;  // suppress copile warnings
    }
  }

DIAGNOSTIC_POP_CDK

  bool do_equivalent(int code, const error_condition &ec) const
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

  friend const error_category& winsock_error_category();
};


std::string error_category_winsock::message(int code) const
{
  std::string message;
  LPSTR buffer = NULL;

  // Note: on windows error codes are unsigned
  assert(code > 0);

  DWORD result = ::FormatMessageA(
    FORMAT_MESSAGE_ALLOCATE_BUFFER
    | FORMAT_MESSAGE_FROM_SYSTEM
    | FORMAT_MESSAGE_IGNORE_INSERTS,
    NULL, static_cast<DWORD>(code),
    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
    (LPSTR)&buffer, 0, NULL
  );

  if (result == 0)
    message = "Unknown Winsock error";
  else
    message = buffer;

  ::LocalFree(buffer);

  return message;
}


const error_category& winsock_error_category()
{
  static const error_category_winsock instance;

  return instance;
}


#else // _WIN32


const int SOCKET_ERROR = -1;


#endif // _WIN32


/*
  error_category_resolve class
  ============================

  Used for handling errors returned by network name resolution routines
  related to `getaddrinfo` (see
  <http://pubs.opengroup.org/onlinepubs/009695399/functions/getaddrinfo.html>)
*/

class error_category_resolve : public error_category_base
{
  error_category_resolve() {}

  const char* name() const NOEXCEPT { return "resolve"; }
  std::string message(int code) const;

DIAGNOSTIC_PUSH_CDK
#ifdef _MSC_VER
  // 4702 = unreachable code
  DISABLE_WARNING_CDK(4702)
#endif // _MSC_VER

  error_condition do_default_error_condition(int code) const
  {
    switch (code)
    {
      case EAI_AGAIN: return errc::resource_unavailable_try_again;
      case EAI_BADFLAGS: return errc::invalid_argument;
      case EAI_FAIL: return errc::address_not_available;
      case EAI_FAMILY: return errc::address_family_not_supported;
      case EAI_MEMORY: return errc::not_enough_memory;

      // Note: On Windows EAI_NODATA == EAI_NONAME

#if defined(EAI_NODATA) && EAI_NODATA != EAI_NONAME
      case EAI_NODATA:
#endif
      case EAI_NONAME: return errc::address_not_available;

      case EAI_SERVICE:
        //The service passed was not recognized for the specified socket type.
        return errc::invalid_argument;

      case EAI_SOCKTYPE:
        //The intended socket type was not recognized.
        return errc::not_a_socket;

#ifdef EAI_OVERFLOW
      case EAI_OVERFLOW:
        //An argument buffer overflowed.
        return errc::value_too_large;
#endif

#ifdef EAI_SYSTEM
      case EAI_SYSTEM:
        //A system error occurred; the error code can be found in errno.
        return posix_error_category().default_error_condition(errno);
#endif

      default:
        throw_error(code, error_category_resolve());
        return errc::no_error;  // suppress compile warnings
    }
  }

DIAGNOSTIC_POP_CDK

  bool do_equivalent(int code, const error_condition &ec) const
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

  friend const error_category& resolve_error_category();
};


std::string error_category_resolve::message(int code) const
{
  return gai_strerror(code);
}


const error_category& resolve_error_category()
{
  static const error_category_resolve instance;

  return instance;
}


/**
  Throws thread specific socket error.
*/
static void throw_socket_error()
{
#ifdef _WIN32
  int error = WSAGetLastError();
  if (error)
    throw_error(error, winsock_error_category());
#else
  throw_system_error();
#endif
}


/**
  Checks socket's state for errors. If an error is encountered, the appropriate
  exception is thrown.
*/
static void check_socket_error(Socket socket)
{
  int error = 0;
  socklen_t error_length = sizeof(error);

  if (::getsockopt(socket, SOL_SOCKET, SO_ERROR, (char *)&error, &error_length) != 0)
    throw_socket_error();

  if (error)
#ifdef _WIN32
    throw_error(error, winsock_error_category());
#else

    // Note: this is not very clear in POSIX docs, but the error code returned by
    // getsockopt(.. SO_ERROR ..) should be interpreted like errno value.
    // For example IBM docs for SO_ERROR specify:
    // "Return any pending errors in the socket. The value returned corresponds
    //  to the standard error codes defined in <errno.h>"

    throw_error(error, posix_error_category());
#endif
}


void set_nonblocking(Socket socket, bool nonblocking)
{
#ifdef _WIN32
  u_long set_nonblocking = nonblocking ? 1ul : 0ul;

  if (::ioctlsocket(socket, FIONBIO, &set_nonblocking) == SOCKET_ERROR)
    throw_socket_error();
#else
  int flags = ::fcntl(socket, F_GETFL, 0);

  if (flags >= 0)
  {
    if (nonblocking)
      flags |= O_NONBLOCK;
    else
      flags &= ~O_NONBLOCK;

    if (::fcntl(socket, F_SETFL, flags) != 0)
      throw_socket_error();
  }
  else
  {
    throw_socket_error();
  }
#endif
}


#if defined WITH_SSL && OPENSSL_VERSION_NUMBER < 0x10100000L
//Not needed after 1.1

static std::mutex* m_openssl_mutex = nullptr;

void thread_setup()
{
  m_openssl_mutex = new std::mutex[CRYPTO_num_locks()];
}

void thread_cleanup()
{
  delete[] m_openssl_mutex;
}

static void locking_function(
  int mode, int n, const char* /*file*/, int /*line*/
)
{
  if(mode & CRYPTO_LOCK)
  {
    m_openssl_mutex[n].lock();
  }
  else if(mode & CRYPTO_UNLOCK)
  {
    m_openssl_mutex[n].unlock();
  }
}

static void id_function(CRYPTO_THREADID *id)
{
  CRYPTO_THREADID_set_numeric(
        id,
        static_cast<unsigned long>(
          std::hash<std::thread::id>()(std::this_thread::get_id())
          )
        );
}
#endif

void initialize_socket_system()
{
#ifdef _WIN32
  WSADATA wsa_data;
  WORD version_requested = MAKEWORD(2, 2);

  if (::WSAStartup(version_requested, &wsa_data) != 0)
    throw_error("Winsock initialization failed.");
#endif

#ifdef WITH_SSL
  SSL_library_init();
  OpenSSL_add_all_algorithms();
  SSL_load_error_strings();
# if OPENSSL_VERSION_NUMBER < 0x10100000L
  thread_setup();
  CRYPTO_set_locking_callback(locking_function);
  CRYPTO_THREADID_set_callback(id_function);
# endif
#endif

#ifndef WIN32
  //ignore SIGPIPE signal when sending data with connection closed by server
  signal(SIGPIPE, SIG_IGN);
#endif
}


void uninitialize_socket_system()
{
#ifdef _WIN32
  if (::WSACleanup() != 0)
    throw_socket_error();
#endif
#ifdef WITH_SSL
# if OPENSSL_VERSION_NUMBER < 0x10100000L
  thread_cleanup();
# endif
#endif
}


Socket socket(bool nonblocking, addrinfo* hints)
{
  Socket socket = NULL_SOCKET;

  if (hints)
    socket = ::socket(hints->ai_family, hints->ai_socktype, hints->ai_protocol);
  else
    socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

  if (socket != NULL_SOCKET)
  {
    int reuse_addr = 1;
    if (::setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr, sizeof(reuse_addr)) != 0)
      throw_socket_error();

    try
    {
      set_nonblocking(socket, nonblocking);
    }
    catch (...)
    {
      close(socket);
      throw;
    }
  }
  else
  {
    throw_socket_error();
    throw_error("Failed to create socket.");
  }

  return socket;
}

#ifndef _WIN32
Socket unix_socket(bool nonblocking)
{
  Socket socket = NULL_SOCKET;

  socket = ::socket(AF_UNIX, SOCK_STREAM, 0);

  if (socket != NULL_SOCKET)
  {
    int reuse_addr = 1;
    if (::setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr, sizeof(reuse_addr)) != 0)
      throw_socket_error();

    try
    {
      set_nonblocking(socket, nonblocking);
    }
    catch (...)
    {
      close(socket);
      throw;
    }
  }
  else
  {
    throw_socket_error();
  }

  return socket;
}
#endif //_WIN32


void close(Socket socket)
{
  if (socket == NULL_SOCKET)
    return;

#ifdef _WIN32
  if (::closesocket(socket) != 0)
#else
  if (::close(socket) != 0)
#endif
  {
    throw_socket_error();
  }
}


void shutdown(Socket socket, Shutdown_mode mode)
{
#ifdef _WIN32
  const int SHUT_RD = SD_RECEIVE;
  const int SHUT_WR = SD_SEND;
  const int SHUT_RDWR = SD_BOTH;
#endif

  int sys_mode;

  switch(mode)
  {
    case SHUTDOWN_MODE_READ:
      sys_mode = SHUT_RD;
      break;
    case SHUTDOWN_MODE_WRITE:
      sys_mode = SHUT_WR;
      break;
    case SHUTDOWN_MODE_BOTH:
      sys_mode = SHUT_RDWR;
      break;
    default:
      THROW("Invalid socket shutdown mode.");
  }

  if (::shutdown(socket, sys_mode) != 0)
    throw_socket_error();
}


addrinfo* addrinfo_from_string(const char* host_name, unsigned short port)
{
  addrinfo* result = NULL;
  addrinfo hints = {};
  in6_addr addr = {};
  char str_port[6];

  if (std::sprintf(str_port, "%hu", port) < 0)
    throw_error("Invalid port.");

  hints.ai_flags = AI_NUMERICSERV;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  if (inet_pton(AF_INET, host_name, &addr) == 1)
  {
    hints.ai_family = AF_INET;
    hints.ai_flags |= AI_NUMERICHOST;
  }
  else
  {
    if (inet_pton(AF_INET6, host_name, &addr) == 1)
    {
      hints.ai_family = AF_INET6;
      hints.ai_flags |= AI_NUMERICHOST;
    }
  }

  int rc = getaddrinfo(host_name, str_port, &hints, &result);

#ifdef EAI_SYSTEM
  if (EAI_SYSTEM == rc && errno)
    throw_posix_error();
#endif

  if (rc != 0)
    throw_error(rc, resolve_error_category());

  if (!result)
    throw_error(std::string("Invalid host name: ") + host_name);

  return result;
}


DIAGNOSTIC_PUSH_CDK

#ifdef _MSC_VER
  // 4189 = local variable is initialized but not referenced
  DISABLE_WARNING_CDK(4189)
#endif

Socket connect(const char *host_name, unsigned short port,
               uint64_t timeout_usec)
{
  Socket socket = NULL_SOCKET;
  addrinfo* host_list = NULL;
  auto deadline = system_clock::now() + microseconds(timeout_usec);

  // Resolve host name.
  // TODO: Configurable number of attempts
  int attempts = 2;
  while (!host_list)
  {
    attempts--;
    try
    {
      /*
        The DNS async resolution is not supported on all platforms.
        Therefore, we will do the blocking call and measure the time
      */
      host_list = detail::addrinfo_from_string(host_name, port);
      if (timeout_usec > 0 && system_clock::now() >= deadline)
      {
        throw Connect_timeout_error(timeout_usec / 1000);
      }
    }
    catch (Error& e)
    {
      if(e != errc::resource_unavailable_try_again || attempts <= 0)
        throw;
    }
  }

  struct AddrInfoGuard
  {
    addrinfo* list;
    ~AddrInfoGuard() { freeaddrinfo(list); }
  }
  guard = { host_list };

  // Connect to host.
  int connect_result = SOCKET_ERROR;
  addrinfo* host = host_list;

  while (connect_result != 0 && host)
  {
    try
    {
      socket = detail::socket(true, host);
      connect_result = ::connect(socket, host->ai_addr, static_cast<int>(host->ai_addrlen));

      if (connect_result != 0)
      {
      #ifdef _WIN32
        if (connect_result == SOCKET_ERROR && WSAGetLastError() == WSAEWOULDBLOCK)
      #else
        if (connect_result == SOCKET_ERROR && errno == EINPROGRESS)
      #endif
        {

          int select_result = 0;

          do{

            auto timeout = duration_cast<microseconds>(
              deadline - system_clock::now()
            ).count();

            select_result = poll_one(
                              socket, POLL_MODE_CONNECT, true,
                              0 == timeout_usec ? 0 : timeout > 0 ? timeout : 1
                                                                    );
          // Note: if poll_one() returns 0 then, according to POSIX specs:
          // A value of 0 indicates that the call timed out and no file descriptors have been selected
          // Due to a bug on WSApool, it may return 0 even if no timeout occur..
          // So we will check if timeout occurs and try again if not

          } while ((select_result == 0) &&
                   ((timeout_usec == 0) ||
                    (std::chrono::system_clock::now() < deadline)
                    )
                   );

          if ((timeout_usec > 0) &&
              (std::chrono::system_clock::now() >= deadline))
          {
            // Throw the error in milliseconds, which we did not adjust.
            // Otherwise the user will be confused why the timeout
            // in the error message is smaller than defined
            // (original timeout minus DNS resolution time)
            throw Connect_timeout_error(timeout_usec / 1000);
          }

          if (select_result < 0)
            throw_socket_error();
          else
            check_socket_error(socket);

          connect_result = 0;
        }
        else
        {
          throw_socket_error();
        }
      }
    }
    catch (Connect_timeout_error&)
    {
      close(socket);
      throw;
    }
    catch (...)
    {
      close(socket);

      host = host->ai_next;
      if (!host)
        throw;
    }
  }

  return socket;
}

DIAGNOSTIC_POP_CDK

#ifndef _WIN32
Socket connect(const char *path, uint64_t timeout_usec)
{
  Socket socket = NULL_SOCKET;
  auto deadline = system_clock::now() + microseconds(timeout_usec);

  // Connect to host.
  int connect_result = SOCKET_ERROR;
  struct sockaddr_un addr;

  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, path, sizeof(addr.sun_path)-1);

  try
  {
    socket = detail::unix_socket(true);
    connect_result = ::connect(socket,
                               (struct sockaddr*)(&addr),
                               sizeof(addr));

    if (connect_result != 0)
    {
      if (connect_result == SOCKET_ERROR && errno == EINPROGRESS)
      {
        int select_result = poll_one(socket, POLL_MODE_CONNECT, true,
                                       timeout_usec);
        if (select_result == 0 && (timeout_usec > 0) &&
          (system_clock::now() >= deadline))
        {
          // We probably hit the timeout
          throw Connect_timeout_error(timeout_usec / 1000);
        }
        else if (select_result < 0)
          throw_socket_error();
        else
          check_socket_error(socket);

        connect_result = 0;
      }
      else
      {
        throw_socket_error();
      }
    }
  }
  catch (...)
  {
    close(socket);
    rethrow_error();
  }
  return socket;
}
#endif //_WIN32


Socket listen_and_accept(unsigned short port)
{
  Socket client = NULL_SOCKET;
  Socket acceptor = detail::socket(true);

  try
  {
    sockaddr_in serv_addr = {};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (::bind(acceptor, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0 ||
        ::listen(acceptor, 1) == SOCKET_ERROR)
    {
      throw_socket_error();
    }

    int select_result = poll_one(acceptor, POLL_MODE_CONNECT, true);

    if (select_result > 0)
    {
      sockaddr_in cli_addr = {};
      socklen_t cli_addr_length = sizeof(cli_addr);

      client = ::accept(acceptor, (sockaddr *)&cli_addr, &cli_addr_length);

      if (client == NULL_SOCKET)
        throw_socket_error();
    }
    else if (select_result == 0)
    {
      check_socket_error(acceptor);
    }
    else
    {
      throw_socket_error();
    }

    detail::close(acceptor);
  }
  catch (...)
  {
    detail::close(acceptor);
    throw;
  }

  return client;
}


int poll_one(Socket socket, Poll_mode mode, bool wait,
               uint64_t timeout_usec)
{

DIAGNOSTIC_PUSH_CDK

#ifdef _WIN32
  // 4548 = expression has no effect
  // This warning is generated by FD_SET
  DISABLE_WARNING_CDK(4548)
#endif

  struct pollfd fds = {};
  fds.fd = socket;
  switch(mode)
  {
  case POLL_MODE_CONNECT:
    fds.events = POLLIN | POLLOUT;
    break;
  case POLL_MODE_READ:
    fds.events = POLLIN;
    break;
  case POLL_MODE_WRITE:
    fds.events = POLLOUT;
    break;
  }


DIAGNOSTIC_POP_CDK

  //milliseconds
  int timeout =
    !wait ? 0
    : timeout_usec > 0 ? static_cast<int>((1000+timeout_usec) / 1000) : -1;

#ifdef _WIN32
  int result = ::WSAPoll(&fds, 1, timeout);
#else
  int result = ::poll(&fds, 1,  timeout);
 #endif

  if (fds.revents & (POLLERR | POLLHUP | POLLNVAL))
  {
    check_socket_error(socket);
  }

  return result;
}


size_t bytes_available(Socket socket)
{
  unsigned long bytes_available;

#ifdef _WIN32
  if (::ioctlsocket(socket, FIONREAD, &bytes_available) != 0)
#else
  if (::ioctl(socket, FIONREAD, &bytes_available) == -1)
#endif
  {
    throw_socket_error();
  }

  return bytes_available;
}


void recv(Socket socket, byte *buffer, size_t buffer_size)
{
  // TODO: Investigate if more efficient implementation is possible with ::recv() and MSG_WAITALL flag.

  if (buffer_size == 0)
    return;

  size_t bytes_received = 0;

  while (bytes_received != buffer_size)
    bytes_received += recv_some(socket, buffer + bytes_received, buffer_size - bytes_received, true);
}


void send(Socket socket, const byte *buffer, size_t buffer_size)
{
  if (buffer_size == 0)
    return;

  size_t bytes_sent = 0;

  while (bytes_sent != buffer_size)
    bytes_sent += send_some(socket, buffer + bytes_sent, buffer_size - bytes_sent, true);
}


size_t recv_some(Socket socket, byte *buffer, size_t buffer_size, bool wait)
{
  if (buffer_size == 0)
    return 0;

  /*
    TODO: buffer size checks - throw error if passed buffer is bigger than
    some reasonable limit.
  */
  assert(buffer_size > 0);
  assert(buffer_size < (size_t)std::numeric_limits<int>::max());

  size_t bytes_received = 0;

  int select_result = poll_one(socket, POLL_MODE_READ, wait);

  if (select_result > 0)
  {
    int recv_result = ::recv(socket, reinterpret_cast<char *>(buffer),
                             static_cast<int>(buffer_size), 0);

    if (recv_result == 0)
    {
      throw connection::Error_eos();
    }
    else if (recv_result == SOCKET_ERROR)
    {
#ifdef _WIN32
      if (WSAGetLastError() == WSAEWOULDBLOCK)
#else
      if (errno == EAGAIN || errno == EWOULDBLOCK)
#endif
      {
        bytes_received = 0;
      }
      else
      {
        throw_socket_error();
      }
    }
    else
    {
      assert(recv_result > 0);
      bytes_received = static_cast<size_t>(recv_result);
    }
  }
  else if (select_result == 0)
  {
    return 0;
  }
  else
  {
    throw_socket_error();
  }

  return bytes_received;
}


size_t send_some(Socket socket, const byte *buffer, size_t buffer_size, bool wait)
{
  if (buffer_size == 0)
    return 0;

  /*
    TODO: buffer size checks - throw error if passed buffer is bigger than
    some reasonable limit.
  */
  assert(buffer_size > 0);
  assert(buffer_size < (size_t)std::numeric_limits<int>::max());

  size_t bytes_sent = 0;

  int select_result = poll_one(socket, POLL_MODE_WRITE, wait);

  if (select_result > 0)
  {
    int send_result = ::send(socket, reinterpret_cast<const char *>(buffer),
                             static_cast<int>(buffer_size), 0);

    if (send_result == SOCKET_ERROR)
    {
#ifdef _WIN32
      if (WSAGetLastError() == WSAEWOULDBLOCK)
#else
      if (errno == EAGAIN || errno == EWOULDBLOCK)
#endif
      {
        bytes_sent = 0;
      }
      else
      {
        throw_socket_error();
      }
    }
    else
    {
      assert(send_result >= 0);
      bytes_sent = static_cast<size_t>(send_result);
    }
  }
  else if (select_result == 0)
  {
    return 0;
  }
  else
  {
    throw_socket_error();
  }

  return bytes_sent;
}

std::string get_local_hostname()
{
  char buf[1024] = {0};
  if (gethostname(buf, sizeof(buf)) < 0) {
    throw_socket_error();
  }
  return buf;
}

#ifdef _WIN32
std::forward_list<Srv_host_detail> srv_list(const std::string &hostname)
{
  DNS_STATUS status;               //Return value of  DnsQuery_A() function.
  PDNS_RECORD pDnsRecord =nullptr;          //Pointer to DNS_RECORD structure.

  using Srv_list = std::forward_list<Srv_host_detail>;
  Srv_list srv;
  Srv_list::const_iterator srv_it = srv.before_begin();

  status = DnsQuery(hostname.c_str(), DNS_TYPE_SRV, DNS_QUERY_STANDARD, nullptr, &pDnsRecord, nullptr);
  if (!status)
  {
    PDNS_RECORD pRecord = pDnsRecord;
    while (pRecord)
    {
      if (pRecord->wType == DNS_TYPE_SRV)
      {
        srv_it = srv.emplace_after(srv_it,
          Srv_host_detail
        {
          pRecord->Data.Srv.wPriority,
          pRecord->Data.Srv.wWeight,
          pRecord->Data.Srv.wPort,
          pRecord->Data.Srv.pNameTarget
        }
        );
      }
      pRecord = pRecord->pNext;
    }

    DnsRecordListFree(pDnsRecord, DnsFreeRecordListDeep);
  }
  return srv;
}
#else

std::forward_list<Srv_host_detail> srv_list(const std::string &hostname)
{
  struct __res_state state {};
  res_ninit(&state);

  using Srv_list = std::forward_list<Srv_host_detail>;
  Srv_list srv;
  Srv_list::const_iterator srv_it = srv.before_begin();

  unsigned char query_buffer[NS_PACKETSZ];


  //let get
  int res = res_nsearch(&state, hostname.c_str(), ns_c_in, ns_t_srv, query_buffer, sizeof (query_buffer) );

  if (res >= 0)
  {
    ns_msg msg;
    char name_buffer[NS_MAXDNAME];
    Srv_host_detail host_data;
    ns_initparse(query_buffer, res, &msg);


    auto process = [&msg, &name_buffer, &host_data, &srv, &srv_it](const ns_rr &rr) -> void
    {
      const unsigned char* srv_data = ns_rr_rdata(rr);

      //Each NS_GET16 call moves srv_data to next value
      NS_GET16(host_data.prio, srv_data);
      NS_GET16(host_data.weight, srv_data);
      NS_GET16(host_data.port, srv_data);

      dn_expand(ns_msg_base(msg), ns_msg_end(msg),
                srv_data, name_buffer, sizeof(name_buffer));

      host_data.name = name_buffer;

      srv_it = srv.emplace_after(
                 srv_it,
                 std::move(host_data));
    };

    for(int x= 0; x < ns_msg_count(msg, ns_s_an); x++)
    {
          ns_rr rr;
          ns_parserr(&msg, ns_s_an, x, &rr);
          process(rr);
    }
  }
  res_nclose(&state);

  return srv;
}
#endif

}}}} // cdk::foundation::connection::detail
