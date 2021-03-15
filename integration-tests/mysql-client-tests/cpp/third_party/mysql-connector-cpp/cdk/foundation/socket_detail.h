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

#ifndef CDK_FOUNDATION_SOCKET_DETAIL_H
#define CDK_FOUNDATION_SOCKET_DETAIL_H
#include <mysql/cdk/foundation/types.h>
#include <forward_list>
#include <string>

PUSH_SYS_WARNINGS_CDK

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

#else

#include <unistd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <netdb.h>

// Note: this is required on Solaris for FIONREAD ioctl constant

#if defined(__sun) || defined(__SCO_VERSION__)
#include <sys/filio.h>
#endif

#endif

POP_SYS_WARNINGS_CDK


namespace cdk {
namespace foundation {
namespace connection {
namespace detail {


#ifdef _WIN32
typedef SOCKET Socket;
const Socket NULL_SOCKET = INVALID_SOCKET;
#else
typedef int Socket;
const Socket NULL_SOCKET = -1;
#endif


enum Shutdown_mode
{
  SHUTDOWN_MODE_READ,
  SHUTDOWN_MODE_WRITE,
  SHUTDOWN_MODE_BOTH
};


enum Poll_mode
{
  POLL_MODE_CONNECT,
  POLL_MODE_READ,
  POLL_MODE_WRITE
};


/**
  Changes socket's blocking mode.

  Changes blocking mode of an existing TCP/IP socket.

  @param[in] socket
    Socket being modified.
  @param[in] nonblocking
    If `true`, sets socket mode to non-blocking. Otherwise, socket is set to
    block.

  @throw cdk::foundation::Error
    Blocking mode change failed.
*/
void set_nonblocking(Socket socket, bool nonblocking);


/**
  Initialize socket system.

  Initializes socket system (e.g. Winsock), if it is required by a given
  platform. Should be called once, before any socket-related operations are
  performed.

  @throw cdk::foundation::Error
    Socket system initialization failed.
*/

void initialize_socket_system();


/**
  Uninitialize socket system.

  Uninitializes socket system. Should be always called if previous call to
  `initialize_socket_system` succeeded.

  @throw cdk::foundation::Error
    Socket system unintialization failed.
*/

void uninitialize_socket_system();


/**
  Create a TCP/IP socket.

  Creates a new blocking or non-blocking TCP/IP socket.

  @param[in] nonblocking
    If `true`, creates a non-blocking socket. Otherwise, a blocking socket is
    created.
  @param[in] hints
    Optional hints for creating a socket.

  @throw cdk::foundation::Error
    Socket creation failed.
*/

Socket socket(bool nonblocking, addrinfo* hints = 0);

#ifndef _WIN32
/**
  Create a Unix domain socket.

  Creates a new blocking or non-blocking Unix domain socket.

  @param[in] nonblocking
    If `true`, creates a non-blocking socket. Otherwise, a blocking socket is
    created.

  @throw cdk::foundation::Error
    Socket creation failed.
*/

Socket unix_socket(bool nonblocking);
#endif //_WIN32

/**
  Close a socket.

  Closes a socket.

  @param[in] socket
    Socket to be closed.

  @throw cdk::foundation::Error
    Socket closing failed.
*/

void close(Socket socket);


/**
  Shutdown socket.

  Shuts down socket's operations.

  @param[in] socket
    Socket to be shut down.
  @param[in] mode
    Shutdown mode.

  @throw cdk::foundation::Error
    Socket shutdown failed.
*/

void shutdown(Socket socket, Shutdown_mode mode);


/**
  Create a `addrinfo` structure from a string.

  Creates a `addrinfo` structure from a string address representation and port.

  @param[in] host_name
    Host name.
  @param[in] port
    Host port.

  @return
    `addrinfo` structure.

  @throw cdk::foundation::Error
    Failed to create a `addrinfo` structure.

  @note
    This function always blocks.
    Result pointer needs to be released with `freeaddrinfo`.
*/

addrinfo* addrinfo_from_string(const char* host_name, unsigned short port);

/**
  Create and connect socket.

  Creates and connects a socket to a TCP/IP host.

  @param[in] host
    Destination host name.
  @param[in] port
    Destination host port.
  @param[in] timeout_usec
    Timeout in microseconds. 0 means wait indefinitely.

  @return
    Connected socket.

  @throw cdk::foundation::Error
    Connection failed.

  @note
    This function always blocks.
*/

Socket connect(const char *host, unsigned short port,
               uint64_t timeout_usec);

#ifndef _WIN32
/**
  Create and connect socket.

  Creates and connects a socket to a Unix domain socket.

  @param[in] path
    Destination socket path.

  @param[in] timeout_usec
    Timeout in microseconds. 0 means wait indefinitely.

  @return
    Connected socket.

  @throw cdk::foundation::Error
    Connection failed.

  @note
    This function always blocks.
*/
Socket connect(const char *path, uint64_t timeout_usec);
#endif //_WIN32


/**
  Listen for incoming connections and accept them.

  Creates a new socket, binds it to a TCP source port, and listens for
  incoming connections. After connection is successfully accepted, a new socket
  is created. Listening socket is always closed, even if function fails.

  @param[in] port
    Source TCP port.

  @return
    Accepted socket.

  @throw cdk::foundation::Error
    Either creation of listening socket or accepting of incoming socket failed.

  @note
    This function always blocks.
*/

Socket listen_and_accept(unsigned short port);


/**
  Test socket's I/O state.

  Tests if data can be read from or written to a socket without blocking.

  @param[in] socket
    Socket to be tested.
  @param[in] mode
    I/O mode.
  @param[in] wait
    If `true`, function will block. Otherwise, it will return immediately.
  @param[in] timeout_usec
    Timeout in microsedonds
  @return
    Same as POSIX `poll` function.

  @throw cdk::foundation::Error
    If after testing socket is in an erroneous state, function throws.
*/

int poll_one(Socket socket, Poll_mode mode, bool wait,
             uint64_t timeout_usec = 0);

/**
  Get the number of bytes pending read.

  Get the number of bytes available for read in a socket.

  @param[in] socket
    Socket to be tested.

  @return
    Number of bytes available for read.

  @throw cdk::foundation::Error
    Socket testing failed.
*/

size_t bytes_available(Socket socket);


/**
  Receives data from a socket.

  Receives the exact number of bytes from a socket. This function doesn't
  return until all bytes are read or until an error is encountered.

  @param[in] socket
    Socket used for reading.
  @param[out] buffer
    Data buffer.
  @param[in] buffer_size
    Number of bytes that will be read from a socket. May not be larger than
    the size of `buffer`.

  @throw cdk::foundation::connection::Error_eos
    End-of-stream encountered.
  @throw cdk::foundation::Error
    Socket read failed.

  @note
    This function always blocks.
*/

void recv(Socket socket, byte *buffer, size_t buffer_size);


/**
  Sends data to a socket.

  Sends the exact number of bytes to a socket. This function doesn't return
  until all bytes are sent or until an error is encountered.

  @param[in] socket
    Socket used for sending.
  @param[in] buffer
    Data buffer.
  @param[in] buffer_size
    Number of bytes that will be sent to a socket. May not be larger than
    the size of `buffer`.

  @throw cdk::foundation::Error
    Socket write failed.

  @note
    This function always blocks.
*/

void send(Socket socket, const byte *buffer, size_t buffer_size);


/**
  Receives some data from a socket.

  Receives at most `buffer_size` bytes from a socket.

  @param[in] socket
    Socket used for reading.
  @param[out] buffer
    Data buffer.
  @param[in] buffer_size
    Maximum number of bytes that will be read from a socket. May not be larger
    than the size of `buffer`.
  @param[in] wait
    If `true`, operation will block. Otherwise, data is immediately available.

  @return
    The number of bytes read from a socket.

  @throw cdk::foundation::connection::Error_eos
    End-of-stream encountered.
  @throw cdk::foundation::Error
    Socket read failed.
*/

size_t recv_some(Socket socket, byte *buffer, size_t buffer_size, bool wait);


/**
  Sends some data to a socket.

  Sends at most `buffer_size` bytes to a socket.

  @param[in] socket
    Socket used for sending.
  @param[in] buffer
    Data buffer.
  @param[in] buffer_size
    Maximum number of bytes that will be sent to a socket. May not be larger
    than the size of `buffer`.
  @param[in] wait
    If `true`, operation will block. Otherwise, it will return immediately.

  @return
    The number of bytes sent to a socket.

  @throw cdk::foundation::Error
    Socket write failed.
*/

size_t send_some(Socket socket, const byte *buffer, size_t buffer_size, bool wait);


/**
   @brief get_local_hostname returns hostname of the current machine
 */
std::string get_local_hostname();

/*
   Retrieve host SRV record (target:port) list for specified service and protocol
 */
struct Srv_host_detail
{
  uint16_t prio;
  uint16_t weight;
  uint16_t port;
  string name;
};

std::forward_list<Srv_host_detail> srv_list(const std::string &host_name);

}}}} // cdk::foundation::connection::detail


#endif // CDK_FOUNDATION_SOCKET_DETAIL_H
